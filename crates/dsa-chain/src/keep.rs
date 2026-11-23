//! Keep rule v2, bound to Midnight node 2.1.101.
//!
//! H is the oldest event block, T the last served event block and P the stored
//! header tip. All ranges include their endpoints. The store evaluates the rule
//! in SQL (`dsa-store/src/keep.sql`); the tests check that SQL against an
//! independent model over generated chains.
//!
//! Query paths below are relative to `reference/`. The observation queries are
//! in `primitives/mainchain-follower/src/db/queries/`, candidates in
//! `primitives/mainchain-follower/src/data_source/candidates_data_source/`.
//!
//! * K1 keeps headers H..P. `WHERE block_no >= $1 AND block_no <= $2`
//!   serves header ranges (`partner-chains/toolkit/data-sources/db-sync/src/db_model.rs:253`).
//! * K2 keeps transaction/output/spend/asset/metadata rows in H..T. The event
//!   predicates include `block.block_no >= $2 AND block.block_no <= $4`
//!   (`reference/primitives/mainchain-follower/src/db/queries/cnight_observation.rs:163`).
//! * K3 keeps old outputs without a spend before H, their assets and creating
//!   transactions. `get_asset_spends` joins `tx_out.tx_id = tx_in.tx_out_id`
//!   and the output index (`cnight_observation.rs:213-214`); it does not join the
//!   creating header. Mapping deregistration has the same old-output dependency.
//! * K4 keeps existing datums of retained outputs at the mapping, governance,
//!   ICS or reserve addresses, or holding the exact permissioned-candidates unit.
//!   A governance-policy asset at an ordinary address does not retain its datum.
//!   Registration/deregistration JOIN datum (`cnight_observation.rs:51,111`),
//!   governance JOINs datum and permissioned candidates LEFT JOINs it.
//! * K5 keeps every output in the newest eligible old governance transaction,
//!   with its transaction/header/assets/datum. Eligibility includes a datum row:
//!   `JOIN datum ON tx_out.data_hash = datum.hash`, address, policy and
//!   `block.block_no <= $3` (`federated_authority_observation.rs:44-54`).
//!   All newest transaction ties stay because the upstream LIMIT order omits
//!   output index. NULL datum values are still existing datum rows.
//! * K6 keeps the newest old permissioned transaction at epoch <= epoch(H)-2,
//!   every later eligible old output, and their headers. Match the exact unit:
//!   `WHERE ma_tx_out.ident = $1 AND origin_block.epoch_no <= $2`
//!   (`candidates_data_source/db_model.rs:278-279`). Its ORDER BY
//!   also omits output index, so complete newest transaction ties stay.
//! * K7 keeps the latest non-NULL-slot header at or before each preceding epoch
//!   cutoff, epoch(H)-2 and epoch(H)-1. Empty epochs can use an earlier header:
//!   `block.epoch_no <= $1 AND block.slot_no IS NOT NULL`
//!   (`candidates_data_source/db_model.rs:218`). Compare slot order, as the SQL does.
//! * K8 keeps epoch parameters/stake at epoch >= epoch(H)-2 and referenced pools.
//!   Nonce/stake bind `WHERE epoch_no = $1` and `WHERE es.epoch_no = $1`
//!   (`candidates_data_source/db_model.rs:293,253`). The cursor's maximum size
//!   uses `epoch_param.epoch_no = block.epoch_no` (`cnight_observation.rs:458`).
//! * K9 keeps available fixed descriptors for six explicit profile roles and
//!   every descriptor referenced by a retained row.
//!   The cache resolves `WHERE policy = $1 AND name = $2` once
//!   (`primitives/mainchain-follower/src/db/multi_asset_cache.rs:51`).
//! * K10 keeps the newest old spend, its spending transaction, spent output and
//!   output dependencies when H..T has no spend. The startup `EXISTS(SELECT 1
//!   FROM tx_in)` chooses the node's spend path (`candidates_data_source/db_model.rs:90`).
//!
//! Per-output asset projection applies uniformly at every address, in every
//! clause: only exact `cnight`, auth, permissioned_candidates
//! and bridge units, or any name under the council/technical-committee policies.
//! ICS/reserve have no all-assets exception. Original asset IDs and pre-filter
//! ordinals are preserved, including gaps. Output shells are not discarded by
//! this filter. Other profile-map keys add neither eligibility nor descriptors.
//!
//! A source with no spend anywhere cannot satisfy the serving invariant and
//! remains subject to verification before publication. Selection invents no row.
//! Coarse helper IDs are checked against each database’s bound guarantees;
//! downstream event and state rows are compared exactly.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ids::HEIGHT_LIMIT, profiles::Profile};

pub const TABLES: [&str; 11] = [
    "block",
    "tx",
    "tx_out",
    "tx_in",
    "ma_tx_out",
    "multi_asset",
    "datum",
    "epoch_param",
    "pool_hash",
    "epoch_stake",
    "tx_metadata",
];
pub type RetainedRows = BTreeMap<String, BTreeSet<i64>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bounds {
    pub horizon: u32,
    pub target: u32,
    pub tip: u32,
}

impl Bounds {
    pub fn validate(self) -> Result<(), KeepError> {
        if self.horizon > self.target || self.target > self.tip || self.tip >= HEIGHT_LIMIT {
            return Err(KeepError::Bounds);
        }
        Ok(())
    }

    pub fn validate_profile(self, profile: &Profile) -> Result<(), KeepError> {
        self.validate()?;
        if self.tip - self.target != profile.cardano.security_parameter {
            return Err(KeepError::Target);
        }
        Ok(())
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum KeepError {
    #[error("invalid_keep_bounds")]
    Bounds,
    #[error("invalid_keep_target")]
    Target,
    #[error("missing_horizon_epoch")]
    Horizon,
    #[error("incomplete_chain_model")]
    Reference,
}
