//! Persistent tracker for validators that sign invalid execution proofs.
//!
//! When `ProofStatus::Invalid` is returned for a BLS-valid proof, the signing validator is
//! recorded here. Future proofs from banned validators are IGNORE'd without wasting
//! verification resources.
//!
//! Design decisions (from review):
//! - Ban threshold: 1 (a single signed invalid proof is sufficient)
//! - Ban scope: all proof types from the banned validator
//! - Persistence: DB-backed via `HotColdDB`, survives restarts
//! - Operator escape hatch: CLI subcommand to list/unban/clear (future work)

use ssz::{Decode, Encode};
use ssz_derive::{Decode as DeriveDecode, Encode as DeriveEncode};
use std::collections::HashSet;
use std::sync::Arc;
use store::{DBColumn, Error as StoreError, HotColdDB, ItemStore, StoreItem};
use types::{EthSpec, Hash256};

/// 32-byte key for accessing the persisted tracker. All zero because the column acts as namespace.
pub const INVALID_PROOF_TRACKER_DB_KEY: Hash256 = Hash256::ZERO;

/// Tracks validators that have signed invalid execution proofs.
///
/// The in-memory set is the source of truth during operation. Changes are persisted
/// to `HotColdDB` so bans survive restarts.
#[derive(Debug, Default)]
pub struct InvalidProofTracker {
    /// Set of validator indices that are banned (signed at least one invalid proof).
    banned_validators: HashSet<u64>,
}

/// Information recorded when a validator is banned.
#[derive(Debug, Clone)]
pub struct InvalidProofRecord {
    pub validator_index: u64,
    pub request_root: Hash256,
    pub proof_type: u8,
    pub slot: Option<types::Slot>,
}

/// SSZ-serializable wrapper for persisting the banned validator set.
#[derive(Debug, Clone, DeriveEncode, DeriveDecode)]
struct PersistedInvalidProofTracker {
    /// Sorted list of banned validator indices.
    banned_validators: Vec<u64>,
}

impl StoreItem for PersistedInvalidProofTracker {
    fn db_column() -> DBColumn {
        DBColumn::InvalidProofTracker
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        self.as_ssz_bytes()
    }

    fn from_store_bytes(bytes: &[u8]) -> Result<Self, StoreError> {
        Self::from_ssz_bytes(bytes).map_err(Into::into)
    }
}

impl InvalidProofTracker {
    /// Load a tracker from the database. Returns `Default` if no persisted state exists.
    pub fn load_from_store<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>>(
        store: &Arc<HotColdDB<E, Hot, Cold>>,
    ) -> Self {
        match store.get_item::<PersistedInvalidProofTracker>(&INVALID_PROOF_TRACKER_DB_KEY) {
            Ok(Some(persisted)) => {
                let banned_validators: HashSet<u64> =
                    persisted.banned_validators.into_iter().collect();
                let count = banned_validators.len();
                if count > 0 {
                    tracing::info!(
                        count,
                        "Loaded invalid proof tracker from disk — {} validators banned",
                        count,
                    );
                }
                InvalidProofTracker { banned_validators }
            }
            Ok(None) => {
                tracing::debug!("No persisted invalid proof tracker found, starting fresh");
                InvalidProofTracker::default()
            }
            Err(e) => {
                tracing::warn!(
                    error = ?e,
                    "Failed to load invalid proof tracker from disk, starting fresh"
                );
                InvalidProofTracker::default()
            }
        }
    }

    /// Persist the current state to the database.
    pub fn persist_to_store<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>>(
        &self,
        store: &Arc<HotColdDB<E, Hot, Cold>>,
    ) -> Result<(), StoreError> {
        let mut sorted: Vec<u64> = self.banned_validators.iter().copied().collect();
        sorted.sort_unstable();
        let persisted = PersistedInvalidProofTracker {
            banned_validators: sorted,
        };
        store.put_item(&INVALID_PROOF_TRACKER_DB_KEY, &persisted)
    }

    /// Check whether a validator is banned.
    pub fn is_banned(&self, validator_index: u64) -> bool {
        self.banned_validators.contains(&validator_index)
    }

    /// Record that a validator signed an invalid proof. Returns `true` if this is a new ban.
    ///
    /// Note: The caller is responsible for calling `persist_to_store` after this method
    /// to ensure the ban survives restarts.
    pub fn record_invalid_proof(&mut self, record: InvalidProofRecord) -> bool {
        let is_new = self.banned_validators.insert(record.validator_index);
        if is_new {
            tracing::warn!(
                validator_index = record.validator_index,
                ?record.request_root,
                proof_type = record.proof_type,
                "Banning validator for signing invalid execution proof"
            );
        }
        is_new
    }

    /// Unban a specific validator (operator escape hatch).
    ///
    /// Note: The caller is responsible for calling `persist_to_store` after this method.
    pub fn unban(&mut self, validator_index: u64) -> bool {
        self.banned_validators.remove(&validator_index)
    }

    /// Clear all bans (operator escape hatch).
    ///
    /// Note: The caller is responsible for calling `persist_to_store` after this method.
    pub fn clear(&mut self) {
        self.banned_validators.clear();
    }

    /// Number of banned validators (for metrics / tests).
    pub fn banned_count(&self) -> usize {
        self.banned_validators.len()
    }

    /// List all banned validator indices.
    pub fn banned_validators(&self) -> impl Iterator<Item = u64> + '_ {
        self.banned_validators.iter().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(validator_index: u64) -> InvalidProofRecord {
        InvalidProofRecord {
            validator_index,
            request_root: Hash256::repeat_byte(0x01),
            proof_type: 1,
            slot: None,
        }
    }

    #[test]
    fn ban_on_first_invalid_proof() {
        let mut tracker = InvalidProofTracker::default();
        assert!(!tracker.is_banned(42));

        let is_new = tracker.record_invalid_proof(make_record(42));
        assert!(is_new);
        assert!(tracker.is_banned(42));
    }

    #[test]
    fn duplicate_ban_returns_false() {
        let mut tracker = InvalidProofTracker::default();
        tracker.record_invalid_proof(make_record(42));

        let is_new = tracker.record_invalid_proof(make_record(42));
        assert!(!is_new);
        assert_eq!(tracker.banned_count(), 1);
    }

    #[test]
    fn unban_removes_validator() {
        let mut tracker = InvalidProofTracker::default();
        tracker.record_invalid_proof(make_record(42));

        assert!(tracker.unban(42));
        assert!(!tracker.is_banned(42));
    }

    #[test]
    fn clear_removes_all() {
        let mut tracker = InvalidProofTracker::default();
        tracker.record_invalid_proof(make_record(1));
        tracker.record_invalid_proof(make_record(2));
        tracker.record_invalid_proof(make_record(3));

        tracker.clear();
        assert_eq!(tracker.banned_count(), 0);
    }

    #[test]
    fn ban_scope_is_all_types() {
        let mut tracker = InvalidProofTracker::default();
        // Ban was recorded for proof_type=1, but ban is key-scoped, not type-scoped
        tracker.record_invalid_proof(make_record(42));
        // is_banned doesn't take proof_type — all types are banned
        assert!(tracker.is_banned(42));
    }

    #[test]
    fn ssz_round_trip() {
        let mut tracker = InvalidProofTracker::default();
        tracker.record_invalid_proof(make_record(10));
        tracker.record_invalid_proof(make_record(20));
        tracker.record_invalid_proof(make_record(5));

        // Serialize
        let mut sorted: Vec<u64> = tracker.banned_validators().collect();
        sorted.sort_unstable();
        let persisted = PersistedInvalidProofTracker {
            banned_validators: sorted,
        };
        let bytes = persisted.as_store_bytes();

        // Deserialize
        let restored =
            PersistedInvalidProofTracker::from_store_bytes(&bytes).expect("SSZ decode failed");
        assert_eq!(restored.banned_validators, vec![5, 10, 20]);
    }
}
