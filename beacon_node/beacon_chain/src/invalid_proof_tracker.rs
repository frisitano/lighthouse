//! Persistent tracker for validators that sign invalid execution proofs.
//!
//! When `ProofStatus::Invalid` is returned for a BLS-valid proof, the signing validator is
//! recorded here. Future proofs from banned validators are IGNORE'd without wasting
//! verification resources.
//!
//! Design decisions (from review):
//! - Ban threshold: 1 (a single signed invalid proof is sufficient)
//! - Ban scope: all proof types from the banned validator
//! - Persistence: DB-backed, survives restarts
//! - Operator escape hatch: CLI subcommand to list/unban/clear (future work)

use std::collections::HashSet;

/// Tracks validators that have signed invalid execution proofs.
///
/// Currently in-memory only. DB persistence is a follow-up task.
#[derive(Debug, Default)]
pub struct InvalidProofTracker {
    /// Set of validator indices that are banned (signed at least one invalid proof).
    banned_validators: HashSet<u64>,
}

/// Information recorded when a validator is banned.
#[derive(Debug, Clone)]
pub struct InvalidProofRecord {
    pub validator_index: u64,
    pub request_root: types::Hash256,
    pub proof_type: u8,
    pub slot: Option<types::Slot>,
}

impl InvalidProofTracker {
    /// Check whether a validator is banned.
    pub fn is_banned(&self, validator_index: u64) -> bool {
        self.banned_validators.contains(&validator_index)
    }

    /// Record that a validator signed an invalid proof. Returns `true` if this is a new ban.
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
    pub fn unban(&mut self, validator_index: u64) -> bool {
        self.banned_validators.remove(&validator_index)
    }

    /// Clear all bans (operator escape hatch).
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
    use types::Hash256;

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
}
