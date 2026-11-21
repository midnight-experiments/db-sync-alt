//! SQL selection for the pure keep rule. Table queries preserve source payloads
//! and stream once per family. Destination batches never re-execute the source
//! prefix selection. Aggregates consume bounded materialized inputs, so they
//! cannot turn into a backward min/max walk on source indexes.

use dsa_chain::{
    keep::{Bounds, RetainedRows, TABLES},
    profiles::Profile,
};
use tokio_postgres::{GenericClient, RowStream, types::ToSql};

use crate::Error;

#[derive(Debug, Clone, Copy)]
pub enum SourceSchema {
    Public,
    Serve,
    /// Private conversion facts, never a source-reader schema.
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

/// Destination-only checkpoint derived from the same retention CTEs. Source
/// facts are immutable private tables in one writer transaction. Each step is
/// evaluated once, then indexed/analyzed before its consumers are planned.
#[derive(Debug)]
pub struct KeyStage {
    pub table: &'static str,
    pub create_sql: String,
    pub prepare_sql: String,
}

const KEY_UNION_MARKER: &str = " SELECT 'block'::text AS family,id FROM keep_block";
fn prefix(full: &str) -> Result<&str, Error> {
    full.split_once(KEY_UNION_MARKER)
        .and_then(|(prefix, _)| {
            prefix.strip_prefix("CREATE TEMP TABLE conversion_keep_keys ON COMMIT DROP AS ")
        })
        .ok_or(Error::Refusal("invalid_staging_keep_definition"))
}
fn cte_body(full: &str, cte: &str, next: &str) -> Result<String, Error> {
    let begin = full
        .find(&format!("{cte} AS MATERIALIZED ("))
        .map(|n| n + format!("{cte} AS MATERIALIZED (").len())
        .ok_or(Error::Refusal("invalid_staging_keep_definition"))?;
    let end = full
        .find(&format!("{next} AS MATERIALIZED ("))
        .and_then(|n| full[..n].rfind("),"))
        .ok_or(Error::Refusal("invalid_staging_keep_definition"))?;
    full.get(begin..end)
        .map(str::to_owned)
        .ok_or(Error::Refusal("invalid_staging_keep_definition"))
}
fn replace_cte(full: &mut String, cte: &str, next: &str, body: &str) -> Result<(), Error> {
    let old = cte_body(full, cte, next)?;
    let begin = full
        .find(&format!("{cte} AS MATERIALIZED ("))
        .map(|n| n + format!("{cte} AS MATERIALIZED (").len())
        .ok_or(Error::Refusal("invalid_staging_keep_definition"))?;
    full.replace_range(begin..begin + old.len(), body);
    Ok(())
}
fn projection_stage(
    full: &str,
    table: &'static str,
    query: &str,
    prepare: &str,
) -> Result<KeyStage, Error> {
    Ok(KeyStage {
        table,
        create_sql: format!(
            "CREATE TEMP TABLE {table} ON COMMIT DROP AS {} {query}",
            prefix(full)?
        ),
        prepare_sql: prepare.to_owned(),
    })
}

fn bytes(value: &str) -> Result<Vec<u8>, Error> {
    hex::decode(value).map_err(|_| Error::Refusal("invalid_keep_profile"))
}

impl Selection {
    pub fn staging_plan() -> Result<Vec<KeyStage>, Error> {
        let mut full = crate::canonical::staging_keys_sql();
        let mut stages = Vec::new();
        // Preserve multiplicity for permissioned candidates and epoch headers:
        // empty epochs can select the same historical header twice. The final
        // original UNION, rather than a new uniqueness constraint, deduplicates.
        for (cte, next, table, index, unique) in [
            (
                "old_unspent",
                "governance_candidates",
                "conversion_keys_unspent",
                "id",
                true,
            ),
            (
                "permissioned_candidates",
                "permissioned_last_transaction",
                "conversion_keys_permissioned",
                "tx_id",
                false,
            ),
            (
                "epoch_headers",
                "fallback_candidates",
                "conversion_keys_epochs",
                "id",
                false,
            ),
            (
                "keep_tx_out",
                "keep_block",
                "conversion_keys_outputs",
                "id",
                true,
            ),
            (
                "keep_ma_tx_out",
                "keep_multi_asset",
                "conversion_keys_assets",
                "id",
                true,
            ),
        ] {
            let prefix = full
                .split_once(" SELECT 'block'::text AS family,id FROM keep_block")
                .and_then(|(prefix, _)| {
                    prefix.strip_prefix("CREATE TEMP TABLE conversion_keep_keys ON COMMIT DROP AS ")
                })
                .ok_or(Error::Refusal("invalid_staging_keep_definition"))?;
            stages.push(KeyStage {
                table,
                create_sql: format!("CREATE TEMP TABLE {table} ON COMMIT DROP AS {prefix} SELECT * FROM {cte}"),
                prepare_sql: format!("CREATE {}INDEX {table}_lookup ON pg_temp.{table}({index});ANALYZE pg_temp.{table}", if unique {"UNIQUE "} else {""}),
            });
            let start = full
                .find(&format!("{cte} AS MATERIALIZED ("))
                .ok_or(Error::Refusal("invalid_staging_keep_definition"))?;
            let end = full
                .find(&format!("{next} AS MATERIALIZED ("))
                .ok_or(Error::Refusal("invalid_staging_keep_definition"))?;
            if start >= end {
                return Err(Error::Refusal("invalid_staging_keep_definition"));
            }
            full.replace_range(
                start..end,
                &format!("{cte} AS MATERIALIZED (SELECT * FROM pg_temp.{table}),\n"),
            );
        }
        // Freeze only the retained join fields before planning eligibility.
        // They use exactly the source/bounds joins from K3/K4/K9. A small,
        // analyzed relation prevents a datum preimage join from driving a scan
        // of the full-width source copy before output eligibility is known.
        let tx = cte_body(&full, "keep_tx", "keep_tx_in")?;
        let output_join = tx
            .split_once(" UNION SELECT o.tx_id ")
            .map(|(_, tail)| tail)
            .ok_or(Error::Refusal("invalid_staging_keep_definition"))?;
        let outputs = format!("SELECT o.id,o.tx_id,o.address,o.data_hash {output_join}");
        stages.push(projection_stage(&full, "conversion_output_facts", &outputs,
            "CREATE UNIQUE INDEX conversion_output_facts_lookup ON pg_temp.conversion_output_facts(id);ANALYZE pg_temp.conversion_output_facts")?);
        replace_cte(
            &mut full,
            "keep_tx",
            "keep_tx_in",
            &tx.replace("pg_temp.tx_out o", "pg_temp.conversion_output_facts o"),
        )?;

        let catalogue = cte_body(&full, "keep_multi_asset", "keep_datum")?;
        let asset_join = catalogue
            .split_once(" UNION SELECT a.ident ")
            .map(|(_, tail)| tail)
            .ok_or(Error::Refusal("invalid_staging_keep_definition"))?;
        let assets = format!("SELECT a.id,a.tx_out_id,a.ident {asset_join}");
        stages.push(projection_stage(&full, "conversion_asset_facts", &assets,
            "CREATE UNIQUE INDEX conversion_asset_facts_lookup ON pg_temp.conversion_asset_facts(id);CREATE INDEX conversion_asset_facts_outpoint ON pg_temp.conversion_asset_facts(tx_out_id,ident);ANALYZE pg_temp.conversion_asset_facts")?);
        replace_cte(
            &mut full,
            "keep_multi_asset",
            "keep_datum",
            &catalogue.replace("pg_temp.ma_tx_out a", "pg_temp.conversion_asset_facts a"),
        )?;

        let datum = cte_body(&full, "keep_datum", "keep_epoch_param")?;
        let datum_driver = datum
            .replace(
                "SELECT DISTINCT d.id",
                "SELECT DISTINCT o.data_hash AS hash",
            )
            .replace(
                "pg_temp.tx_out o JOIN keep_tx_out k ON k.id=o.id",
                "pg_temp.conversion_output_facts o",
            )
            .replace(" JOIN pg_temp.datum d ON d.hash=o.data_hash", "")
            .replace("pg_temp.ma_tx_out a", "pg_temp.conversion_asset_facts a");
        if datum_driver.contains("d.id") || datum_driver.contains("pg_temp.datum") {
            return Err(Error::Refusal("invalid_staging_keep_definition"));
        }
        // An exact permissioned unit at a retained output is always an eligible
        // v2 asset. Thus its retained asset facts preserve every K4 token probe;
        // unknown descriptors still cannot match the original catalogue join.
        stages.push(projection_stage(&full, "conversion_datum_hashes", &datum_driver,
            "CREATE UNIQUE INDEX conversion_datum_hashes_lookup ON pg_temp.conversion_datum_hashes(hash);ANALYZE pg_temp.conversion_datum_hashes")?);
        replace_cte(
            &mut full,
            "keep_datum",
            "keep_epoch_param",
            " SELECT DISTINCT d.id FROM pg_temp.conversion_datum_hashes h CROSS JOIN LATERAL (SELECT d.id FROM pg_temp.datum d WHERE d.hash=h.hash OFFSET 0) d\n",
        )?;

        // Each original family is evaluated once, with a separate finite
        // command budget. No large joins survive into final key assembly.
        let family_tables = [
            ("block", "conversion_family_block"),
            ("tx", "conversion_family_tx"),
            ("tx_out", "conversion_family_tx_out"),
            ("tx_in", "conversion_family_tx_in"),
            ("ma_tx_out", "conversion_family_ma_tx_out"),
            ("multi_asset", "conversion_family_multi_asset"),
            ("datum", "conversion_family_datum"),
            ("epoch_param", "conversion_family_epoch_param"),
            ("pool_hash", "conversion_family_pool_hash"),
            ("epoch_stake", "conversion_family_epoch_stake"),
            ("tx_metadata", "conversion_family_tx_metadata"),
        ];
        if family_tables
            .iter()
            .map(|(family, _)| *family)
            .ne(crate::NODE_RELATIONS.iter().copied())
        {
            return Err(Error::Refusal("invalid_staging_keep_definition"));
        }
        for (family, table) in family_tables {
            stages.push(projection_stage(
                &full,
                table,
                &crate::canonical::staging_key_family(family),
                &format!(
                    "ALTER TABLE pg_temp.{table} ADD PRIMARY KEY(family,id);ANALYZE pg_temp.{table}"
                ),
            )?);
        }
        let union = family_tables
            .iter()
            .map(|(_, table)| format!("SELECT family,id FROM pg_temp.{table}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        stages.push(projection_stage(&full, "conversion_keep_keys", &union,
            "ALTER TABLE pg_temp.conversion_keep_keys ADD PRIMARY KEY(family,id);ANALYZE pg_temp.conversion_keep_keys")?);
        Ok(stages)
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
        for body in ["council", "technical_committee"] {
            governance_addresses.push(
                profile
                    .addresses
                    .get(body)
                    .ok_or(Error::Refusal("invalid_keep_profile"))?
                    .clone(),
            );
            governance_policies.push(bytes(
                &profile
                    .assets
                    .get(body)
                    .ok_or(Error::Refusal("invalid_keep_profile"))?
                    .policy,
            )?);
        }
        let permissioned = profile
            .assets
            .get("permissioned_candidates")
            .ok_or(Error::Refusal("invalid_keep_profile"))?;
        let fixed = dsa_chain::assets::fixed_units(profile);
        let fixed_policies = fixed
            .iter()
            .map(|unit| {
                bytes(
                    unit.get(..56)
                        .ok_or(Error::Refusal("invalid_keep_profile"))?,
                )
            })
            .collect::<Result<_, _>>()?;
        let fixed_names = fixed
            .iter()
            .map(|unit| {
                bytes(
                    unit.get(56..)
                        .ok_or(Error::Refusal("invalid_keep_profile"))?,
                )
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            horizon: i64::from(bounds.horizon),
            target: i64::from(bounds.target),
            tip: i64::from(bounds.tip),
            addresses: [
                "mapping",
                "council",
                "technical_committee",
                "ics",
                "reserve",
            ]
            .iter()
            .map(|role| profile.addresses[*role].clone())
            .collect(),
            governance_addresses,
            governance_policies,
            permissioned_policy: bytes(&permissioned.policy)?,
            permissioned_name: bytes(&permissioned.name)?,
            fixed_policies,
            fixed_names,
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

    /// Six fixed units include governance descriptors; their exact unit arms
    /// are a subset of the two whole-policy arms. Unknown descriptors are kept
    /// for closure refusal rather than classified as foreign by absence.
    pub(crate) fn asset_predicate(schema: SourceSchema, alias: &str) -> String {
        format!(
            "{alias}.ident NOT IN(SELECT catalogue.id FROM {}.multi_asset catalogue) OR {alias}.ident IN(SELECT catalogue.id FROM {}.multi_asset catalogue WHERE catalogue.policy=ANY($6::bytea[]) OR EXISTS(SELECT 1 FROM unnest($9::bytea[],$10::bytea[]) fixed(policy,name) WHERE fixed.policy=catalogue.policy AND fixed.name=catalogue.name))",
            schema.name(),
            schema.name()
        )
    }

    pub fn ctes_sql(schema: SourceSchema) -> String {
        include_str!("keep.sql")
            .replace(
                "__ELIGIBLE_ASSET__",
                &format!("({})", Self::asset_predicate(schema, "a")),
            )
            .replace("__SOURCE__", schema.name())
    }

    /// One fixed query per schema/table. Execute once and consume its RowStream;
    /// batch destination writes without rerunning the source selection. This
    /// needs no source TEMP privilege, DDL or writable transaction.
    pub fn rows_sql(&self, schema: SourceSchema, table: &str) -> Result<String, Error> {
        if !TABLES.contains(&table) {
            return Err(Error::Refusal("invalid_keep_table"));
        }
        let key = if table == "epoch_param" {
            "epoch_no"
        } else {
            "id"
        };
        Ok(format!(
            "{} SELECT s.* FROM {}.{table} s JOIN keep_{table} k ON k.id=s.{key} ORDER BY s.{key}",
            Self::ctes_sql(schema),
            schema.name()
        ))
    }

    /// Selection over an already pruned owned store. Its sparse before-H spend
    /// bits prevent exceptional carry outputs from being reclassified by K3.
    /// This does not change complete-source public/serve selection or perform
    /// expiry/deletion; a writer must preserve markers before dropping spends.
    pub fn pruned_rows_sql(&self, table: &str) -> Result<String, Error> {
        let sql = self.rows_sql(SourceSchema::Serve, table)?;
        let predicate = "AND NOT EXISTS(SELECT 1 FROM serve.tx_in i\n";
        if sql.matches(predicate).count() != 1 {
            return Err(Error::Refusal("invalid_keep_marker_template"));
        }
        Ok(sql.replace(predicate,"AND NOT EXISTS(SELECT 1 FROM dsa.consumed_before_horizon c WHERE c.tx_out_id=o.id)\n AND NOT EXISTS(SELECT 1 FROM serve.tx_in i\n"))
    }

    /// Complete-source conversion stream: original JSONB row text, lossless
    /// canonical natural-key text, original key and optional sparse marker text.
    /// One execution per family, sorted by its natural key; no source TEMP/DDL.
    pub fn conversion_rows_sql(&self, table: &str) -> Result<String, Error> {
        crate::canonical::selected_source(table)
    }

    pub async fn stream_rows<C: GenericClient>(
        &self,
        client: &C,
        schema: SourceSchema,
        table: &str,
    ) -> Result<RowStream, Error> {
        self.validate_source(client, schema).await?;
        Ok(client
            .query_raw(&self.rows_sql(schema, table)?, self.parameters())
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

    /// Small-source validation only. Refuse above 100,000 total retained keys
    /// before constructing JSON. Production conversion uses `stream_rows`.
    pub async fn collect_ids<C: GenericClient>(
        &self,
        client: &C,
        schema: SourceSchema,
    ) -> Result<RetainedRows, Error> {
        self.validate_source(client, schema).await?;
        let entries=TABLES.iter().map(|table|format!("'{table}',(SELECT coalesce(jsonb_agg(id ORDER BY id),'[]'::jsonb) FROM keep_{table})")).collect::<Vec<_>>().join(",");
        let counts = TABLES
            .iter()
            .map(|table| format!("(SELECT count(*) FROM keep_{table})"))
            .collect::<Vec<_>>()
            .join("+");
        let sql = format!(
            "{} SELECT CASE WHEN {counts}>100000 THEN NULL ELSE jsonb_build_object({entries}) END",
            Self::ctes_sql(schema)
        );
        let row = client.query_one(&sql, &self.parameters()).await?;
        let value = row
            .get::<_, Option<serde_json::Value>>(0)
            .ok_or(Error::Refusal("keep_validation_too_large"))?;
        Ok(serde_json::from_value(value)?)
    }
}
