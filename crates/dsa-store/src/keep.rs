//! SQL selection for the pure keep rule. Table queries preserve source payloads
//! and accept an inclusive ID interval after the ten shared rule parameters.

use std::collections::BTreeMap;

use dsa_chain::{
    keep::{Bounds, RetainedRows, TABLES},
    profiles::Profile,
};
use tokio_postgres::{GenericClient, types::ToSql};

use crate::Error;

#[derive(Debug, Clone, Copy)]
pub enum SourceSchema {
    Public,
    Serve,
}
impl SourceSchema {
    pub fn name(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Serve => "serve",
        }
    }
}

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
        bounds
            .validate()
            .map_err(|_| Error::Refusal("invalid_keep_bounds"))?;
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
        let fixed: BTreeMap<_, _> = profile
            .assets
            .values()
            .map(|asset| (asset.unit(), asset))
            .collect();
        let fixed_policies = fixed
            .values()
            .map(|asset| bytes(&asset.policy))
            .collect::<Result<_, _>>()?;
        let fixed_names = fixed
            .values()
            .map(|asset| bytes(&asset.name))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            horizon: i64::from(bounds.horizon),
            target: i64::from(bounds.target),
            tip: i64::from(bounds.tip),
            addresses: profile.addresses.values().cloned().collect(),
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

    pub fn row_parameters<'a>(
        &'a self,
        lower: &'a i64,
        upper: &'a i64,
    ) -> Vec<&'a (dyn ToSql + Sync)> {
        let mut params = self.parameters();
        params.extend([lower as &(dyn ToSql + Sync), upper as &(dyn ToSql + Sync)]);
        params
    }

    pub fn ctes_sql(schema: SourceSchema) -> String {
        include_str!("keep.sql").replace("__SOURCE__", schema.name())
    }

    /// Each schema/table pair has one fixed query text. Callers can stream rows
    /// in bounded ID batches through the normal PostgreSQL query/COPY paths.
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
            "{} SELECT s.* FROM {}.{table} s JOIN keep_{table} k ON k.id=s.{key} \
                    WHERE s.{key}>=$11::bigint AND s.{key}<=$12::bigint ORDER BY s.{key}",
            Self::ctes_sql(schema),
            schema.name()
        ))
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

    /// Collect identifiers for validation. Conversion can stream `rows_sql`
    /// instead of materializing all retained identifiers in process memory.
    pub async fn collect_ids<C: GenericClient>(
        &self,
        client: &C,
        schema: SourceSchema,
    ) -> Result<RetainedRows, Error> {
        self.validate_source(client, schema).await?;
        let entries=TABLES.iter().map(|table|format!("'{table}',(SELECT coalesce(jsonb_agg(id ORDER BY id),'[]'::jsonb) FROM keep_{table})")).collect::<Vec<_>>().join(",");
        let sql = format!(
            "{} SELECT jsonb_build_object({entries})",
            Self::ctes_sql(schema)
        );
        let row = client.query_one(&sql, &self.parameters()).await?;
        Ok(serde_json::from_value(row.get(0))?)
    }
}
