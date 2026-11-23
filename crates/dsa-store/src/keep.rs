//! The keep rule in SQL. `keep.sql` is the one definition; every form is
//! assembled from its named blocks by concatenation and placeholder values:
//! the complete-history source form (conversion, comparison), the pruned form
//! of a store (verification, horizon advance) and the staged form that
//! conversion evaluates step by step on its private copy.

use std::{collections::BTreeMap, sync::LazyLock};

use dsa_chain::{
    keep::{Bounds, RetainedRows, TABLES},
    profiles::{AddressRole, Profile},
};
use tokio_postgres::{GenericClient, RowStream, types::ToSql};

use crate::Error;

#[derive(Debug, Clone, Copy)]
pub enum SourceSchema {
    Public,
    Serve,
    /// The conversion's private copy, never a source-reader schema.
    Temporary,
}
impl SourceSchema {
    pub fn name(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Serve => "serve",
            Self::Temporary => "pg_temp",
        }
    }
}

/// Which rows the rule reads.
#[derive(Debug, Clone, Copy)]
pub enum Form {
    /// Complete history in the given schema.
    Source(SourceSchema),
    /// A store that is already pruned: outputs recorded in
    /// `dsa.consumed_before_horizon` are spent before H although their spend
    /// is no longer stored.
    Pruned,
}

const MARKER_FILTER: &str =
    "AND NOT EXISTS(SELECT 1 FROM dsa.consumed_before_horizon c WHERE c.tx_out_id=o.id)\n ";

struct Cte {
    name: &'static str,
    materialized: bool,
    body: &'static str,
}
struct Definition {
    ctes: Vec<Cte>,
    fragments: BTreeMap<&'static str, &'static str>,
}

static DEFINITION: LazyLock<Definition> = LazyLock::new(|| parse(include_str!("keep.sql")));

/// Split `keep.sql` at its `-- cte NAME [inline]` and `-- fragment NAME` lines.
fn parse(text: &'static str) -> Definition {
    let mut ctes = Vec::new();
    let mut fragments = BTreeMap::new();
    let mut current: Option<(&str, &str, usize)> = None;
    let mut finish = |current: Option<(&'static str, &'static str, usize)>, end: usize| {
        if let Some((kind, header, start)) = current {
            let body = text[start..end].trim_end_matches('\n');
            let mut words = header.split_whitespace();
            let name = words.next().expect("keep.sql block name");
            if kind == "fragment" {
                fragments.insert(name, body);
            } else {
                ctes.push(Cte {
                    name,
                    materialized: words.next() != Some("inline"),
                    body,
                });
            }
        }
    };
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        let start = offset;
        offset += line.len();
        let header = line.trim_end();
        if let Some(rest) = header.strip_prefix("-- cte ") {
            finish(current.take(), start);
            current = Some(("cte", rest, offset));
        } else if let Some(rest) = header.strip_prefix("-- fragment ") {
            finish(current.take(), start);
            current = Some(("fragment", rest, offset));
        }
    }
    finish(current, text.len());
    Definition { ctes, fragments }
}

/// Conversion progress through its staged evaluation: CTEs already
/// materialized in temporary tables, and which retained facts exist.
#[derive(Default, Clone)]
struct Staged {
    checkpoints: Vec<(&'static str, &'static str)>,
    output_facts: bool,
    asset_facts: bool,
    datum_hashes: bool,
}

const STAGED_DATUM: &str = " SELECT DISTINCT d.id FROM pg_temp.conversion_datum_hashes h CROSS JOIN LATERAL (SELECT d.id FROM pg_temp.datum d WHERE d.hash=h.hash OFFSET 0) d";

fn fill(
    text: &str,
    schema: SourceSchema,
    markers: bool,
    staged: &Staged,
    datum_facts: bool,
) -> String {
    let definition = &*DEFINITION;
    let mut text = text.to_owned();
    for (name, body) in &definition.fragments {
        text = text.replace(&format!("__{}__", name.to_uppercase()), body);
    }
    let facts = |ready: bool, table: &str, source: &str| {
        if ready {
            format!("pg_temp.{table}")
        } else {
            format!("__SOURCE__.{source}")
        }
    };
    text.replace(
        "__OUTPUT_FACTS__",
        &facts(staged.output_facts, "conversion_output_facts", "tx_out"),
    )
    .replace(
        "__ASSET_FACTS__",
        &facts(staged.asset_facts, "conversion_asset_facts", "ma_tx_out"),
    )
    .replace(
        "__DATUM_ASSETS__",
        &facts(datum_facts, "conversion_asset_facts", "ma_tx_out"),
    )
    .replace("__MARKERS__", if markers { MARKER_FILTER } else { "" })
    .replace(
        "__ELIGIBLE_ASSET__",
        &format!("({})", Selection::asset_predicate(schema, "a")),
    )
    .replace("__SOURCE__", schema.name())
}

fn render(schema: SourceSchema, markers: bool, staged: &Staged, bindings: bool) -> String {
    let ctes = DEFINITION
        .ctes
        .iter()
        .map(|cte| {
            let body = match staged
                .checkpoints
                .iter()
                .find(|(name, _)| *name == cte.name)
            {
                Some((_, table)) => format!(" SELECT * FROM pg_temp.{table}"),
                None if cte.name == "keep_datum" && staged.datum_hashes => STAGED_DATUM.to_owned(),
                None => cte.body.to_owned(),
            };
            let kind = if cte.materialized {
                "MATERIALIZED "
            } else {
                ""
            };
            format!("{} AS {kind}(\n{body}\n)", cte.name)
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let bindings = if bindings { BINDINGS } else { "" };
    fill(
        &format!("WITH\n{bindings}{ctes}\n"),
        schema,
        markers,
        staged,
        false,
    )
}

/// One step of the conversion's staged evaluation. Each step is evaluated
/// once into a temporary table, then indexed and analyzed before its
/// consumers are planned.
#[derive(Debug)]
pub struct KeyStage {
    pub table: String,
    pub create_sql: String,
    pub prepare_sql: String,
}

/// Every parameter stays typed when a staged form no longer references it.
pub(crate) const BINDINGS: &str = "conversion_bindings AS (SELECT $1::bigint AS horizon,$2::bigint AS target,\
    $3::bigint AS tip,$4::text[] AS addresses,$5::text[] AS governance_addresses,\
    $6::bytea[] AS governance_policies,$7::bytea AS permissioned_policy,$8::bytea AS permissioned_name,\
    $9::bytea[] AS fixed_policies,$10::bytea[] AS fixed_names),\n";

/// The final selection of one family. The asset family also keeps every
/// eligible window row, so that an orphan in a corrupt source reaches the
/// closure check instead of disappearing.
fn family_keys(table: &str) -> String {
    if table == "ma_tx_out" {
        format!(
            "SELECT 'ma_tx_out'::text AS family,id FROM (SELECT id FROM keep_ma_tx_out UNION SELECT a.id FROM pg_temp.ma_tx_out a WHERE a.id BETWEEN $1::bigint*68719476736 AND $2::bigint*68719476736+68719476735 AND ({})) selected",
            Selection::asset_predicate(SourceSchema::Temporary, "a")
        )
    } else {
        format!("SELECT '{table}'::text AS family,id FROM keep_{table}")
    }
}

fn bytes(value: &str) -> Result<Vec<u8>, Error> {
    hex::decode(value).map_err(|_| Error::Refusal("invalid_keep_profile"))
}

#[derive(Debug)]
pub struct Selection {
    horizon: i64,
    target: i64,
    tip: i64,
    addresses: Vec<String>,
    governance_addresses: Vec<String>,
    governance_policies: Vec<Vec<u8>>,
    permissioned_policy: Vec<u8>,
    permissioned_name: Vec<u8>,
    fixed_policies: Vec<Vec<u8>>,
    fixed_names: Vec<Vec<u8>>,
}

impl Selection {
    /// The conversion's staged evaluation of the source form over its private
    /// copy: five checkpoints, three retained-fact projections, one key table
    /// per family and their union.
    pub fn staging_plan() -> Vec<KeyStage> {
        let mut staged = Staged::default();
        let mut stages = Vec::new();
        // Permissioned candidates and epoch headers keep their multiplicity:
        // empty epochs can select the same historical header twice. The final
        // UNION deduplicates.
        for (cte, table, column, unique) in [
            ("old_unspent", "conversion_keys_unspent", "id", true),
            (
                "permissioned_candidates",
                "conversion_keys_permissioned",
                "tx_id",
                false,
            ),
            ("epoch_headers", "conversion_keys_epochs", "id", false),
            ("keep_tx_out", "conversion_keys_outputs", "id", true),
            ("keep_ma_tx_out", "conversion_keys_assets", "id", true),
        ] {
            let prefix = render(SourceSchema::Temporary, false, &staged, false);
            stages.push(KeyStage {
                table: table.to_owned(),
                create_sql: format!(
                    "CREATE TEMP TABLE {table} ON COMMIT DROP AS {prefix} SELECT * FROM {cte}"
                ),
                prepare_sql: format!(
                    "CREATE {}INDEX {table}_lookup ON pg_temp.{table}({column});ANALYZE pg_temp.{table}",
                    if unique { "UNIQUE " } else { "" }
                ),
            });
            staged.checkpoints.push((cte, table));
        }
        let project = |staged: &Staged, table: String, query: String, prepare: String| {
            let prefix = render(SourceSchema::Temporary, false, staged, true);
            KeyStage {
                create_sql: format!("CREATE TEMP TABLE {table} ON COMMIT DROP AS {prefix} {query}"),
                table,
                prepare_sql: prepare,
            }
        };
        // Only the retained join fields, frozen before eligibility is planned:
        // a small analyzed relation keeps a datum preimage join from driving a
        // scan of the full-width copy.
        let projection = |staged: &Staged, query: &str, datum_facts: bool| {
            fill(query, SourceSchema::Temporary, false, staged, datum_facts)
        };
        stages.push(project(
            &staged,
            "conversion_output_facts".to_owned(),
            projection(&staged, "SELECT o.id,o.tx_id,o.address,o.data_hash __RETAINED_OUTPUT_JOIN__", false),
            "CREATE UNIQUE INDEX conversion_output_facts_lookup ON pg_temp.conversion_output_facts(id);ANALYZE pg_temp.conversion_output_facts".to_owned(),
        ));
        staged.output_facts = true;
        stages.push(project(
            &staged,
            "conversion_asset_facts".to_owned(),
            projection(&staged, "SELECT a.id,a.tx_out_id,a.ident __RETAINED_ASSET_JOIN__", false),
            "CREATE UNIQUE INDEX conversion_asset_facts_lookup ON pg_temp.conversion_asset_facts(id);CREATE INDEX conversion_asset_facts_outpoint ON pg_temp.conversion_asset_facts(tx_out_id,ident);ANALYZE pg_temp.conversion_asset_facts".to_owned(),
        ));
        staged.asset_facts = true;
        // An exact permissioned unit at a retained output is always an eligible
        // asset, so the retained asset facts preserve every datum token probe.
        stages.push(project(
            &staged,
            "conversion_datum_hashes".to_owned(),
            projection(
                &staged,
                "SELECT DISTINCT o.data_hash AS hash FROM pg_temp.conversion_output_facts o\n__DATUM_ELIGIBLE__",
                true,
            ),
            "CREATE UNIQUE INDEX conversion_datum_hashes_lookup ON pg_temp.conversion_datum_hashes(hash);ANALYZE pg_temp.conversion_datum_hashes".to_owned(),
        ));
        staged.datum_hashes = true;
        // Each family is evaluated once, with its own statement budget.
        for family in TABLES {
            let table = format!("conversion_family_{family}");
            stages.push(project(
                &staged,
                table.clone(),
                family_keys(family),
                format!(
                    "ALTER TABLE pg_temp.{table} ADD PRIMARY KEY(family,id);ANALYZE pg_temp.{table}"
                ),
            ));
        }
        let union = TABLES
            .iter()
            .map(|family| format!("SELECT family,id FROM pg_temp.conversion_family_{family}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        stages.push(project(
            &staged,
            "conversion_keep_keys".to_owned(),
            union,
            "ALTER TABLE pg_temp.conversion_keep_keys ADD PRIMARY KEY(family,id);ANALYZE pg_temp.conversion_keep_keys".to_owned(),
        ));
        stages
    }

    pub fn new(profile: &Profile, bounds: Bounds) -> Result<Self, Error> {
        bounds.validate_profile(profile).map_err(|error| {
            Error::Refusal(match error {
                dsa_chain::keep::KeepError::Target => "invalid_keep_target",
                _ => "invalid_keep_bounds",
            })
        })?;
        let mut governance_addresses = Vec::new();
        let mut governance_policies = Vec::new();
        for body in profile.governance() {
            governance_addresses.push(body.address.to_owned());
            governance_policies.push(bytes(&body.asset.policy)?);
        }
        let permissioned = &profile.assets.permissioned_candidates;
        let fixed = dsa_chain::assets::fixed_units(profile);
        let part = |part: Option<&str>| bytes(part.ok_or(Error::Refusal("invalid_keep_profile"))?);
        Ok(Self {
            horizon: i64::from(bounds.horizon),
            target: i64::from(bounds.target),
            tip: i64::from(bounds.tip),
            addresses: [
                AddressRole::Mapping,
                AddressRole::Council,
                AddressRole::TechnicalCommittee,
                AddressRole::Ics,
                AddressRole::Reserve,
            ]
            .into_iter()
            .map(|role| profile.addresses.get(role).to_owned())
            .collect(),
            governance_addresses,
            governance_policies,
            permissioned_policy: bytes(&permissioned.policy)?,
            permissioned_name: bytes(&permissioned.name)?,
            fixed_policies: fixed
                .iter()
                .map(|unit| part(unit.get(..56)))
                .collect::<Result<_, _>>()?,
            fixed_names: fixed
                .iter()
                .map(|unit| part(unit.get(56..)))
                .collect::<Result<_, _>>()?,
        })
    }

    pub fn parameters(&self) -> Vec<&(dyn ToSql + Sync)> {
        vec![
            &self.horizon,
            &self.target,
            &self.tip,
            &self.addresses,
            &self.governance_addresses,
            &self.governance_policies,
            &self.permissioned_policy,
            &self.permissioned_name,
            &self.fixed_policies,
            &self.fixed_names,
        ]
    }

    /// Six fixed units include the governance descriptors; their exact unit
    /// arms are a subset of the two whole-policy arms. Unknown descriptors are
    /// kept so that the closure check refuses them, rather than classified as
    /// foreign by absence.
    pub(crate) fn asset_predicate(schema: SourceSchema, alias: &str) -> String {
        format!(
            "{alias}.ident NOT IN(SELECT catalogue.id FROM {schema}.multi_asset catalogue) \
             OR {alias}.ident IN(SELECT catalogue.id FROM {schema}.multi_asset catalogue \
             WHERE catalogue.policy=ANY($6::bytea[]) OR EXISTS(SELECT 1 FROM unnest($9::bytea[],$10::bytea[]) \
             fixed(policy,name) WHERE fixed.policy=catalogue.policy AND fixed.name=catalogue.name))",
            schema = schema.name()
        )
    }

    /// The `WITH` clause defining `keep_<table>` for every served table.
    pub fn ctes(form: Form) -> String {
        match form {
            Form::Source(schema) => render(schema, false, &Staged::default(), false),
            Form::Pruned => render(SourceSchema::Serve, true, &Staged::default(), false),
        }
    }

    /// One query per table returning the kept rows with their original
    /// payloads, ordered by key. Execute once and consume the stream.
    pub fn rows_sql(&self, form: Form, table: &str) -> Result<String, Error> {
        if !TABLES.contains(&table) {
            return Err(Error::Refusal("invalid_keep_table"));
        }
        let schema = match form {
            Form::Source(schema) => schema.name(),
            Form::Pruned => "serve",
        };
        let key = if table == "epoch_param" {
            "epoch_no"
        } else {
            "id"
        };
        Ok(format!(
            "{} SELECT s.* FROM {schema}.{table} s JOIN keep_{table} k ON k.id=s.{key} ORDER BY s.{key}",
            Self::ctes(form)
        ))
    }

    /// The complete-source conversion stream: original row text, canonical
    /// natural-key text, original key and optional marker text.
    pub fn conversion_rows_sql(&self, table: &str) -> Result<String, Error> {
        crate::canonical::selected_source(table)
    }

    pub async fn stream_rows<C: GenericClient>(
        &self,
        client: &C,
        form: Form,
        table: &str,
    ) -> Result<RowStream, Error> {
        if let Form::Source(schema) = form {
            self.validate_source(client, schema).await?;
        }
        Ok(client
            .query_raw(&self.rows_sql(form, table)?, self.parameters())
            .await?)
    }

    pub async fn validate_source<C: GenericClient>(
        &self,
        client: &C,
        schema: SourceSchema,
    ) -> Result<(), Error> {
        let row = client
            .query_opt(
                &format!(
                    "SELECT epoch_no::integer FROM {}.block WHERE id=$1",
                    schema.name()
                ),
                &[&self.horizon],
            )
            .await?;
        if row.is_none_or(|row| row.get::<_, Option<i32>>(0).is_none()) {
            return Err(Error::Refusal("missing_horizon_epoch"));
        }
        Ok(())
    }

    /// Small sources only: refuse above 100,000 kept keys before building JSON.
    pub async fn collect_ids<C: GenericClient>(
        &self,
        client: &C,
        form: Form,
    ) -> Result<RetainedRows, Error> {
        if let Form::Source(schema) = form {
            self.validate_source(client, schema).await?;
        }
        let entries=TABLES.iter().map(|table|format!("'{table}',(SELECT coalesce(jsonb_agg(id ORDER BY id),'[]'::jsonb) FROM keep_{table})")).collect::<Vec<_>>().join(",");
        let counts = TABLES
            .iter()
            .map(|table| format!("(SELECT count(*) FROM keep_{table})"))
            .collect::<Vec<_>>()
            .join("+");
        let sql = format!(
            "{} SELECT CASE WHEN {counts}>100000 THEN NULL ELSE jsonb_build_object({entries}) END",
            Self::ctes(form)
        );
        let row = client.query_one(&sql, &self.parameters()).await?;
        let value = row
            .get::<_, Option<serde_json::Value>>(0)
            .ok_or(Error::Refusal("keep_validation_too_large"))?;
        Ok(serde_json::from_value(value)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_placeholder_is_filled_in_every_form() {
        let mut texts = vec![
            Selection::ctes(Form::Pruned),
            Selection::ctes(Form::Source(SourceSchema::Public)),
            Selection::ctes(Form::Source(SourceSchema::Serve)),
        ];
        texts.extend(
            Selection::staging_plan()
                .into_iter()
                .map(|stage| stage.create_sql),
        );
        for text in texts {
            assert!(!text.contains("__"), "unfilled placeholder in {text}");
        }
        let names = DEFINITION
            .ctes
            .iter()
            .map(|cte| cte.name)
            .collect::<Vec<_>>();
        for table in TABLES {
            assert!(names.contains(&format!("keep_{table}").as_str()), "{table}");
        }
    }
}
