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

fn bytes(value: &str) -> Result<Vec<u8>, Error> {
    hex::decode(value).map_err(|_| Error::Refusal("invalid_keep_profile"))
}

impl Selection {
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
