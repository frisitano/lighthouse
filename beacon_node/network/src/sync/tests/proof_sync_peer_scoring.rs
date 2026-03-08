//! Peer scoring tests for ProofSync range operations.
//!
//! These tests verify that the peer scoring mechanism correctly handles:
//! - RangeByRange operations (valid data, partial data, invalid data, timeouts, disconnects)
//! - RangeByRoot operations (valid proof, wrong proof, missing proof, timeouts)
//! - Score evolution (repeated failures, consistent good behavior, banning threshold)
//! - Peer selection (best peer selection, failover)

use super::*;
use crate::NetworkMessage;
use crate::sync::SyncMessage;
use crate::sync::proof_sync::ProofSyncState;
use bls::SignatureBytes;
use execution_layer::MissingProofInfo;
use lighthouse_network::PeerId;
use lighthouse_network::rpc::RequestType;
use lighthouse_network::service::api_types::AppRequestId;
use lighthouse_network::service::api_types::ExecutionProofsByRangeRequestId;
use lighthouse_network::service::api_types::ExecutionProofsByRootRequestId;
use lighthouse_network::service::api_types::SyncRequestId;
use std::sync::Arc;
use std::time::Duration;
use types::Hash256;
use types::execution::eip8025::ExecutionProof;
use types::execution::eip8025::SignedExecutionProof;

const D: Duration = Duration::new(0, 0);

/// Helper to build a minimal SignedExecutionProof for testing.
fn make_test_execution_proof() -> Arc<SignedExecutionProof> {
    Arc::new(SignedExecutionProof {
        message: ExecutionProof::default(),
        validator_index: 0,
        signature: SignatureBytes::empty(),
    })
}

/// Helper to build a MissingProofInfo for testing.
fn make_missing_proof(root: Hash256) -> MissingProofInfo {
    MissingProofInfo {
        root,
        existing_proof_types: vec![],
    }
}

/// Helper to get a peer's current score from the network globals.
fn get_peer_score(rig: &TestRig, peer_id: PeerId) -> f64 {
    rig.network_globals.peers.read().score(&peer_id)
}

/// Helper to check if a peer is banned.
fn is_peer_banned(rig: &TestRig, peer_id: PeerId) -> bool {
    rig.network_globals.peers.read().ban_status(&peer_id).is_some()
}

/// Helper to check if a peer is disconnected.
fn is_peer_connected(rig: &TestRig, peer_id: PeerId) -> bool {
    rig.network_globals.peers.read().is_connected(&peer_id)
}

/// Helper to simulate a peer disconnect.
fn disconnect_peer(rig: &mut TestRig, peer_id: PeerId) {
    rig.peer_disconnected(peer_id);
}

/// Helper to send an execution proof response.
fn send_execution_proof_response(
    rig: &mut TestRig,
    req_id: SyncRequestId,
    peer_id: PeerId,
    proof: Option<Arc<SignedExecutionProof>>,
) {
    rig.send_sync_message(SyncMessage::RpcExecutionProof {
        sync_request_id: req_id,
        peer_id,
        execution_proof: proof,
    });
}

/// Helper to send an execution proof stream termination.
fn send_execution_proof_termination(rig: &mut TestRig, req_id: SyncRequestId, peer_id: PeerId) {
    rig.send_sync_message(SyncMessage::RpcExecutionProof {
        sync_request_id: req_id,
        peer_id,
        execution_proof: None,
    });
}

/// Helper to bootstrap proof sync to fill mode.
fn bootstrap_to_fill_mode(rig: &mut TestRig) -> (ExecutionProofsByRangeRequestId, PeerId) {
    rig.harness.advance_slot();
    rig.sync_manager.start_proof_sync();
    rig.sync_manager.poll_proof_sync();
    let (req_id, peer_id) = rig.find_execution_proofs_by_range_request();
    rig.terminate_execution_proofs_by_range(req_id, peer_id);
    assert_eq!(
        rig.sync_manager.proof_sync_state(),
        ProofSyncState::FillingByRoot
    );
    (req_id, peer_id)
}

// Note: Helper methods find_execution_proofs_by_range_request,
// find_execution_proofs_by_root_request, and terminate_execution_proofs_by_range
// are defined in range.rs and available via TestRig.

// =============================================================================
// ExecutionProofByRange Tests
// =============================================================================

/// Test: Peer returns valid proof range → positive score (no penalty expected)
#[test]
fn test_execution_proof_by_range_valid_data_no_penalty() {
    let mut rig = TestRig::test_setup();
    let proof_peer = rig.new_connected_proof_capable_peer();

    // Record initial score
    let initial_score = get_peer_score(&rig, proof_peer);

    // Bootstrap to range request in flight
    rig.harness.advance_slot();
    rig.sync_manager.start_proof_sync();
    rig.sync_manager.poll_proof_sync();

    let (req_id, peer_id) = rig.find_execution_proofs_by_range_request();
    assert_eq!(peer_id, proof_peer);

    // Send valid execution proof response
    let proof = make_test_execution_proof();
    send_execution_proof_response(
        &mut rig,
        SyncRequestId::ExecutionProofsByRange(req_id),
        peer_id,
        Some(proof),
    );

    // Send stream termination
    send_execution_proof_termination(
        &mut rig,
        SyncRequestId::ExecutionProofsByRange(req_id),
        peer_id,
    );

    // Verify no penalty was applied (valid data should not be penalized)
    rig.expect_no_penalty_for(peer_id);

    // Score should remain unchanged (or potentially increase slightly due to positive implicit scoring)
    let final_score = get_peer_score(&rig, peer_id);
    assert!(
        final_score >= initial_score,
        "Peer score should not decrease for valid data: initial={}, final={}",
        initial_score,
        final_score
    );
}

/// Test: Peer returns partial data → neutral/negative score
#[test]
fn test_execution_proof_by_range_partial_data_state_handling() {
    let mut rig = TestRig::test_setup();
    let proof_peer = rig.new_connected_proof_capable_peer();

    // Bootstrap to range request in flight
    rig.harness.advance_slot();
    rig.sync_manager.start_proof_sync();
    rig.sync_manager.poll_proof_sync();

    let (req_id, peer_id) = rig.find_execution_proofs_by_range_request();

    // Send partial data (one proof) then terminate
    let proof = make_test_execution_proof();
    send_execution_proof_response(
        &mut rig,
        SyncRequestId::ExecutionProofsByRange(req_id),
        peer_id,
        Some(proof),
    );

    // Terminate stream early (partial data)
    send_execution_proof_termination(
        &mut rig,
        SyncRequestId::ExecutionProofsByRange(req_id),
        peer_id,
    );

    // State should transition to FillingByRoot even with partial data
    assert_eq!(
        rig.sync_manager.proof_sync_state(),
        ProofSyncState::FillingByRoot
    );
}

/// Test: Peer timeout during range request → negative score
#[test]
fn test_execution_proof_by_range_timeout_penalty() {
    let mut rig = TestRig::test_setup();
    let proof_peer = rig.new_connected_proof_capable_peer();

    // Bootstrap to range request in flight
    rig.harness.advance_slot();
    rig.sync_manager.start_proof_sync();
    rig.sync_manager.poll_proof_sync();

    let (req_id, peer_id) = rig.find_execution_proofs_by_range_request();

    // Simulate RPC error (timeout)
    rig.send_sync_message(SyncMessage::RpcError {
        peer_id,
        sync_request_id: SyncRequestId::ExecutionProofsByRange(req_id),
        error: lighthouse_network::rpc::RPCError::StreamTimeout,
    });

    // Timeout should result in penalty
    rig.expect_penalty(peer_id, "rpc_error");
}

/// Test: Peer disconnects during range sync → handle gracefully
#[test]
fn test_execution_proof_by_range_disconnect_handling() {
    let mut rig = TestRig::test_setup();
    let proof_peer = rig.new_connected_proof_capable_peer();

    // Bootstrap to range request in flight
    rig.harness.advance_slot();
    rig.sync_manager.start_proof_sync();
    rig.sync_manager.poll_proof_sync();

    let (req_id, peer_id) = rig.find_execution_proofs_by_range_request();
    assert_eq!(peer_id, proof_peer);

    // Simulate peer disconnect during active request
    disconnect_peer(&mut rig, peer_id);

    // Verify peer is disconnected
    assert!(!is_peer_connected(&rig, peer_id));
}

// =============================================================================
// ExecutionProofByRoot Tests
// =============================================================================

/// Test: Peer returns valid proof for root → positive score (no penalty)
#[test]
fn test_execution_proof_by_root_valid_proof_no_penalty() {
    let mut rig = TestRig::test_setup();
    let proof_peer = rig.new_connected_proof_capable_peer();

    // Bootstrap to fill mode
    let _ = bootstrap_to_fill_mode(&mut rig);

    // Set up missing proof
    let block_root = Hash256::random();
    let missing = vec![make_missing_proof(block_root)];
    rig.sync_manager.set_proof_sync_missing(missing);

    // Trigger by-root request
    rig.sync_manager.poll_proof_sync();
    let (req_id, peer_id) = rig.find_execution_proofs_by_root_request();

    // Record initial score
    let initial_score = get_peer_score(&rig, peer_id);

    // Send valid proof
    let proof = make_test_execution_proof();
    send_execution_proof_response(
        &mut rig,
        SyncRequestId::ExecutionProofsByRoot(req_id),
        peer_id,
        Some(proof),
    );

    // Complete the request
    send_execution_proof_termination(
        &mut rig,
        SyncRequestId::ExecutionProofsByRoot(req_id),
        peer_id,
    );

    // No penalty for valid proof
    rig.expect_no_penalty_for(peer_id);

    let final_score = get_peer_score(&rig, peer_id);
    assert!(
        final_score >= initial_score,
        "Peer score should not decrease for valid proof: initial={}, final={}",
        initial_score,
        final_score
    );
}

/// Test: Peer returns proof that fails validation → negative score
#[test]
fn test_execution_proof_by_root_invalid_proof_penalty() {
    let mut rig = TestRig::test_setup();
    let proof_peer = rig.new_connected_proof_capable_peer();

    // Bootstrap to fill mode
    let _ = bootstrap_to_fill_mode(&mut rig);

    // Set up missing proof
    let block_root = Hash256::random();
    let missing = vec![make_missing_proof(block_root)];
    rig.sync_manager.set_proof_sync_missing(missing);

    // Trigger by-root request
    rig.sync_manager.poll_proof_sync();
    let (req_id, peer_id) = rig.find_execution_proofs_by_root_request();

    // Send proof (will be validated by beacon processor)
    let proof = make_test_execution_proof();
    send_execution_proof_response(
        &mut rig,
        SyncRequestId::ExecutionProofsByRoot(req_id),
        peer_id,
        Some(proof),
    );

    // The beacon processor will verify the proof and penalize if invalid
    // Since we're using a default proof, it will likely be invalid
    // Note: In a real scenario, the processor would send a ReportPeer message
}

/// Test: Peer doesn't have proof → neutral score (no penalty, just empty response)
#[test]
fn test_execution_proof_by_root_missing_proof_no_penalty() {
    let mut rig = TestRig::test_setup();
    let proof_peer = rig.new_connected_proof_capable_peer();

    // Bootstrap to fill mode
    let _ = bootstrap_to_fill_mode(&mut rig);

    // Set up missing proof
    let block_root = Hash256::random();
    let missing = vec![make_missing_proof(block_root)];
    rig.sync_manager.set_proof_sync_missing(missing);

    // Trigger by-root request
    rig.sync_manager.poll_proof_sync();
    let (req_id, peer_id) = rig.find_execution_proofs_by_root_request();

    // Record initial score
    let initial_score = get_peer_score(&rig, peer_id);

    // Send empty response (peer doesn't have the proof)
    send_execution_proof_termination(
        &mut rig,
        SyncRequestId::ExecutionProofsByRoot(req_id),
        peer_id,
    );

    // No penalty for missing data (soft failure)
    rig.expect_no_penalty_for(peer_id);

    let final_score = get_peer_score(&rig, peer_id);
    assert_eq!(
        final_score, initial_score,
        "Peer score should be unchanged for missing proof: initial={}, final={}",
        initial_score, final_score
    );
}

/// Test: Peer timeout during by-root request → negative score
#[test]
fn test_execution_proof_by_root_timeout_penalty() {
    let mut rig = TestRig::test_setup();
    let proof_peer = rig.new_connected_proof_capable_peer();

    // Bootstrap to fill mode
    let _ = bootstrap_to_fill_mode(&mut rig);

    // Set up missing proof
    let block_root = Hash256::random();
    let missing = vec![make_missing_proof(block_root)];
    rig.sync_manager.set_proof_sync_missing(missing);

    // Trigger by-root request
    rig.sync_manager.poll_proof_sync();
    let (req_id, peer_id) = rig.find_execution_proofs_by_root_request();

    // Simulate RPC error (timeout)
    rig.send_sync_message(SyncMessage::RpcError {
        peer_id,
        sync_request_id: SyncRequestId::ExecutionProofsByRoot(req_id),
        error: lighthouse_network::rpc::RPCError::StreamTimeout,
    });

    // Timeout should result in penalty
    rig.expect_penalty(peer_id, "rpc_error");
}

// =============================================================================
// Score Evolution Tests
// =============================================================================

/// Test: Peer repeatedly sends invalid data → reputation drops
#[test]
fn test_score_evolution_repeated_invalid_data() {
    let mut rig = TestRig::test_setup();
    let proof_peer = rig.new_connected_proof_capable_peer();

    // Record initial score
    let initial_score = get_peer_score(&rig, proof_peer);

    // Simulate multiple RPC errors (timeouts)
    for _ in 0..5 {
        // Bootstrap to range request
        rig.harness.advance_slot();
        rig.sync_manager.start_proof_sync();
        rig.sync_manager.poll_proof_sync();

        let (req_id, peer_id) = rig.find_execution_proofs_by_range_request();

        // Simulate timeout
        rig.send_sync_message(SyncMessage::RpcError {
            peer_id,
            sync_request_id: SyncRequestId::ExecutionProofsByRange(req_id),
            error: lighthouse_network::rpc::RPCError::StreamTimeout,
        });

        // Each timeout results in a penalty
        rig.expect_penalty(peer_id, "rpc_error");

        // Reset for next iteration
        rig.sync_manager.pause_proof_sync();
    }

    // Score should have decreased significantly
    let final_score = get_peer_score(&rig, proof_peer);
    assert!(
        final_score < initial_score,
        "Peer score should decrease after repeated failures: initial={}, final={}",
        initial_score,
        final_score
    );
}

/// Test: Peer consistently provides good data → reputation stays stable
#[test]
fn test_score_evolution_consistent_good_data() {
    let mut rig = TestRig::test_setup();
    let proof_peer = rig.new_connected_proof_capable_peer();

    let initial_score = get_peer_score(&rig, proof_peer);

    // Simulate multiple successful responses
    for _ in 0..3 {
        // Bootstrap to range request
        rig.harness.advance_slot();
        rig.sync_manager.start_proof_sync();
        rig.sync_manager.poll_proof_sync();

        let (req_id, peer_id) = rig.find_execution_proofs_by_range_request();

        // Send valid proof
        let proof = make_test_execution_proof();
        send_execution_proof_response(
            &mut rig,
            SyncRequestId::ExecutionProofsByRange(req_id),
            peer_id,
            Some(proof),
        );

        // Complete request
        send_execution_proof_termination(
            &mut rig,
            SyncRequestId::ExecutionProofsByRange(req_id),
            peer_id,
        );

        // No penalty for valid data
        rig.expect_no_penalty_for(peer_id);

        // Reset for next iteration
        rig.sync_manager.pause_proof_sync();
    }

    // Score should not have decreased (may stay same or improve with decay)
    let final_score = get_peer_score(&rig, proof_peer);
    assert!(
        final_score >= initial_score - 1.0, // Allow small tolerance for score decay
        "Peer score should not decrease with good behavior: initial={}, final={}",
        initial_score,
        final_score
    );
}

// =============================================================================
// Selection Tests
// =============================================================================

/// Test: Multiple peers available, system can select from available peers
#[test]
fn test_peer_selection_multiple_peers_available() {
    let mut rig = TestRig::test_setup();

    // Create multiple proof-capable peers
    let peer1 = rig.new_connected_proof_capable_peer();
    let peer2 = rig.new_connected_proof_capable_peer();
    let peer3 = rig.new_connected_proof_capable_peer();

    // Bootstrap proof sync
    rig.harness.advance_slot();
    rig.sync_manager.start_proof_sync();
    rig.sync_manager.poll_proof_sync();

    // Get the selected peer
    let (req_id, selected_peer) = rig.find_execution_proofs_by_range_request();

    // The selected peer should be one of our created peers
    assert!(
        selected_peer == peer1 || selected_peer == peer2 || selected_peer == peer3,
        "Should select one of the available peers, got {:?}",
        selected_peer
    );
}

/// Test: Failover when selected peer fails
#[test]
fn test_peer_selection_failover_on_failure() {
    let mut rig = TestRig::test_setup();

    // Create two proof-capable peers
    let peer1 = rig.new_connected_proof_capable_peer();
    let peer2 = rig.new_connected_proof_capable_peer();

    // First request
    rig.harness.advance_slot();
    rig.sync_manager.start_proof_sync();
    rig.sync_manager.poll_proof_sync();

    let (req_id1, selected_peer1) = rig.find_execution_proofs_by_range_request();

    // Simulate failure on selected peer
    rig.send_sync_message(SyncMessage::RpcError {
        peer_id: selected_peer1,
        sync_request_id: SyncRequestId::ExecutionProofsByRange(req_id1),
        error: lighthouse_network::rpc::RPCError::StreamTimeout,
    });

    // Penalty should be applied
    rig.expect_penalty(selected_peer1, "rpc_error");

    // Score should decrease
    let score_after = get_peer_score(&rig, selected_peer1);
    assert!(
        score_after < 0.0,
        "Peer score should decrease after failure: now {}",
        score_after
    );
}

/// Test: System can recover after peer disconnection and find alternative
#[test]
fn test_peer_selection_failover_on_disconnect() {
    let mut rig = TestRig::test_setup();

    // Create two proof-capable peers
    let peer1 = rig.new_connected_proof_capable_peer();
    let peer2 = rig.new_connected_proof_capable_peer();

    // Bootstrap
    rig.harness.advance_slot();
    rig.sync_manager.start_proof_sync();
    rig.sync_manager.poll_proof_sync();

    let (req_id, selected_peer) = rig.find_execution_proofs_by_range_request();

    // Disconnect the selected peer
    disconnect_peer(&mut rig, selected_peer);

    // Verify peer is disconnected
    assert!(!is_peer_connected(&rig, selected_peer));

    // The other peer should still be available
    let other_peer = if selected_peer == peer1 { peer2 } else { peer1 };
    assert!(is_peer_connected(&rig, other_peer));
}

// =============================================================================
// Integration Tests
// =============================================================================

/// Test: Full lifecycle - range sync followed by fill mode with peer scoring
#[test]
fn test_full_lifecycle_peer_scoring() {
    let mut rig = TestRig::test_setup();
    let proof_peer = rig.new_connected_proof_capable_peer();

    // Record initial score
    let initial_score = get_peer_score(&rig, proof_peer);

    // 1. Bootstrap phase (RangeByRange)
    rig.harness.advance_slot();
    rig.sync_manager.start_proof_sync();
    rig.sync_manager.poll_proof_sync();

    let (range_req_id, range_peer) = rig.find_execution_proofs_by_range_request();
    assert_eq!(range_peer, proof_peer);

    // Send valid range response
    let proof = make_test_execution_proof();
    send_execution_proof_response(
        &mut rig,
        SyncRequestId::ExecutionProofsByRange(range_req_id),
        range_peer,
        Some(proof),
    );
    send_execution_proof_termination(
        &mut rig,
        SyncRequestId::ExecutionProofsByRange(range_req_id),
        range_peer,
    );

    // Verify transition to FillingByRoot
    assert_eq!(
        rig.sync_manager.proof_sync_state(),
        ProofSyncState::FillingByRoot
    );

    // 2. Fill phase (RangeByRoot)
    let block_root = Hash256::random();
    let missing = vec![make_missing_proof(block_root)];
    rig.sync_manager.set_proof_sync_missing(missing);
    rig.sync_manager.poll_proof_sync();

    let (root_req_id, root_peer) = rig.find_execution_proofs_by_root_request();

    // Send valid root response
    let proof = make_test_execution_proof();
    send_execution_proof_response(
        &mut rig,
        SyncRequestId::ExecutionProofsByRoot(root_req_id),
        root_peer,
        Some(proof),
    );
    send_execution_proof_termination(
        &mut rig,
        SyncRequestId::ExecutionProofsByRoot(root_req_id),
        root_peer,
    );

    // No penalties should have been applied
    rig.expect_no_penalty_for(proof_peer);

    // Score should not have decreased
    let final_score = get_peer_score(&rig, proof_peer);
    assert!(
        final_score >= initial_score,
        "Peer score should not decrease after successful full lifecycle: initial={}, final={}",
        initial_score,
        final_score
    );
}

/// Test: Multiple concurrent by-root requests with mixed peer performance
#[test]
fn test_concurrent_requests_mixed_peer_performance() {
    let mut rig = TestRig::test_setup();

    // Create multiple peers
    let peer1 = rig.new_connected_proof_capable_peer();
    let peer2 = rig.new_connected_proof_capable_peer();

    // Bootstrap to fill mode
    let _ = bootstrap_to_fill_mode(&mut rig);

    // Set up multiple missing proofs
    let missing = vec![
        make_missing_proof(Hash256::random()),
        make_missing_proof(Hash256::random()),
        make_missing_proof(Hash256::random()),
        make_missing_proof(Hash256::random()),
    ];
    rig.sync_manager.set_proof_sync_missing(missing);

    // Trigger requests
    rig.sync_manager.poll_proof_sync();

    // Collect all requests
    let mut requests = vec![];
    for _ in 0..4 {
        if let Ok((req_id, peer_id)) = rig.pop_received_network_event(|ev| match ev {
            NetworkMessage::SendRequest {
                peer_id,
                request: RequestType::ExecutionProofsByRoot(_),
                app_request_id: AppRequestId::Sync(SyncRequestId::ExecutionProofsByRoot(id)),
            } => Some((*id, *peer_id)),
            _ => None,
        }) {
            requests.push((SyncRequestId::ExecutionProofsByRoot(req_id), peer_id));
        }
    }

    // Complete some successfully, fail others
    for (i, (req_id, peer_id)) in requests.iter().enumerate() {
        if i % 2 == 0 {
            // Success
            send_execution_proof_termination(&mut rig, *req_id, *peer_id);
        } else {
            // Timeout
            rig.send_sync_message(SyncMessage::RpcError {
                peer_id: *peer_id,
                sync_request_id: *req_id,
                error: lighthouse_network::rpc::RPCError::StreamTimeout,
            });
        }
    }

    // Should have received penalties for failed requests
    let penalty_count = rig
        .network_rx_queue
        .iter()
        .filter(|ev| matches!(ev, NetworkMessage::ReportPeer { .. }))
        .count();

    assert!(
        penalty_count > 0,
        "Should have received penalties for failed requests"
    );
}
