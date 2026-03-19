//! HTTP proof engine implementation for EIP-8025.
//!
//! Provides an HTTP implementation with an internal proof cache.
//! HTTP transport is delegated to a [`ProofNodeClient`] implementation.

use super::errors::ProofEngineError;
use super::persisted_state::PersistedProofEngineState;
use super::proof_node_client::{HttpProofNodeClient, ProofNodeClient};
use super::types::ProofEvent;
use crate::{
    ForkchoiceState, ForkchoiceUpdatedResponse, MissingProofInfo, NewPayloadRequest,
    PayloadStatusV1, PayloadStatusV1Status,
    eip8025::state::{RequestMetadata, State},
};
use bls::{FixedBytesExtended, SignatureBytes};
use bytes::Bytes;
use futures::stream::Stream;
use parking_lot::RwLock;
use sensitive_url::SensitiveUrl;
use ssz::Encode;
use ssz_types::VariableList;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

use types::execution::eip8025::{
    ExecutionProof, ProofAttributes, ProofStatus, PublicInput, SignedExecutionProof,
};
use types::{EthSpec, Hash256};

// ─── HttpProofEngine ─────────────────────────────────────────────────────────

/// Proof engine with internal proof storage.
///
/// - Stores ALL unfinalized proofs indexed by new_payload_request_root (unbounded)
/// - Delegates transport to a [`ProofNodeClient`] implementation
/// - Prunes proofs when finalization events occur
pub struct HttpProofEngine {
    /// Transport client for proof engine REST+SSZ+SSE API.
    proof_node: Box<dyn ProofNodeClient>,
    /// The internal state storing execution proofs in a tree structure and buffer.
    state: RwLock<State>,
    /// Buffered proofs for request roots not yet seen.
    buffered_proofs: RwLock<HashMap<Hash256, Vec<SignedExecutionProof>>>,
    /// When true, new_payload returns Valid immediately (for mock/testing scenarios).
    mock_mode: bool,
    /// The request root of the latest proof that was promoted from buffer to tree.
    /// Updated on each successful `insert_proof` that returns `Valid`.
    latest_promoted: RwLock<Option<Hash256>>,
}

impl HttpProofEngine {
    /// Create a new proof engine backed by the HTTP proof node client.
    pub fn new(url: SensitiveUrl, timeout: Option<Duration>) -> Self {
        Self::with_proof_node(HttpProofNodeClient::new(url, timeout))
    }

    /// Create a proof engine backed by a custom [`ProofNodeClient`] implementation.
    ///
    /// Useful for injecting a [`MockProofNodeClient`] in tests.
    ///
    /// [`MockProofNodeClient`]: super::super::test_utils::MockProofNodeClient
    pub fn with_proof_node(proof_node: impl ProofNodeClient + 'static) -> Self {
        Self {
            proof_node: Box::new(proof_node),
            state: RwLock::new(State::new()),
            buffered_proofs: RwLock::new(HashMap::new()),
            mock_mode: false,
            latest_promoted: RwLock::new(None),
        }
    }

    /// Create a proof engine backed by a mock [`ProofNodeClient`] with mock mode enabled.
    ///
    /// In mock mode, `new_payload` returns `Valid` immediately, dummy proofs are
    /// injected automatically, and `forkchoice_updated` always returns `Valid`.
    pub fn with_mock_proof_node(proof_node: impl ProofNodeClient + 'static) -> Self {
        let mut engine = Self::with_proof_node(proof_node);
        engine.mock_mode = true;
        engine
    }

    /// Whether this proof engine is running in mock mode.
    ///
    /// When true, BLS signature verification is skipped for received proofs
    /// because mock-generated proofs use empty signatures.
    pub fn is_mock(&self) -> bool {
        self.mock_mode
    }

    /// Returns the request root of the latest proof that was successfully promoted
    /// in the tree. Used by mock mode to update `local_execution_proof_status`
    /// without going through the verify → gossip_methods path.
    pub fn latest_promoted_request_root(&self) -> Option<Hash256> {
        *self.latest_promoted.read()
    }

    /// Subscribe to method-invocation events emitted by a mock proof node client.
    ///
    /// Returns `None` for production (HTTP) clients.
    pub fn subscribe_client_events(
        &self,
    ) -> Option<tokio::sync::broadcast::Receiver<crate::test_utils::MockClientEvent>> {
        self.proof_node.subscribe_client_events()
    }

    /// Subscribe to SSE proof events from the proof engine.
    pub fn subscribe_proof_events(
        &self,
        filter_root: Option<Hash256>,
    ) -> Pin<Box<dyn Stream<Item = Result<ProofEvent, ProofEngineError>> + Send + '_>> {
        self.proof_node.subscribe_proof_events(filter_root)
    }

    /// Download a completed execution proof by proof type.
    pub async fn get_proof(
        &self,
        new_payload_request_root: Hash256,
        proof_type: u8,
    ) -> Result<Bytes, ProofEngineError> {
        self.proof_node
            .get_proof(new_payload_request_root, proof_type)
            .await
    }

    /// Get all proofs for a given new_payload_request_root.
    pub fn get_proofs_by_root(&self, root: &Hash256) -> Vec<SignedExecutionProof> {
        self.state
            .read()
            .get_proofs(root)
            .map(<[SignedExecutionProof]>::to_vec)
            .unwrap_or_default()
    }

    /// Return all buffer entries that do not yet have sufficient proofs for promotion.
    ///
    /// `MissingProofInfo.root` is populated with the new-payload request root.
    /// The beacon chain layer replaces it with the beacon block root before the
    /// sync manager issues `ExecutionProofsByRoot` RPC requests.
    pub fn missing_proofs(&self) -> Vec<MissingProofInfo> {
        self.state.read().missing_proofs()
    }

    /// Verify an individual execution proof via the proof engine.
    pub async fn verify_execution_proof(
        &self,
        proof: &SignedExecutionProof,
    ) -> Result<ProofStatus, ProofEngineError> {
        if !self
            .state
            .read()
            .contains_request_root(&proof.request_root())
        {
            tracing::info!(target: "execution_layer", "Received proof for unknown request root {}, buffering", proof.request_root());
            self.buffered_proofs
                .write()
                .entry(proof.request_root())
                .or_default()
                .push(proof.clone());
            return Ok(ProofStatus::Syncing);
        }

        // In mock mode, accept all proofs without RPC verification.
        if self.mock_mode {
            return Ok(self.state.write().insert_proof(proof.clone())?);
        }

        let status = self
            .proof_node
            .verify_proof(proof.request_root(), proof.proof_type(), proof.proof_data())
            .await?;

        if status.is_valid() {
            return Ok(self.state.write().insert_proof(proof.clone())?);
        }

        Ok(status)
    }

    /// Buffer a new payload request for proof association.
    pub async fn new_payload<E: EthSpec>(
        &self,
        request: &NewPayloadRequest<'_, E>,
    ) -> Result<PayloadStatusV1, ProofEngineError> {
        let block_hash = request.block_hash();
        let request: RequestMetadata = request.into();
        let request_root = request.request_root;
        let buffered_proofs = self
            .buffered_proofs
            .write()
            .remove(&request_root)
            .unwrap_or_default();
        self.state.write().buffer_request(request);

        // In mock mode, return Valid immediately without waiting for proofs,
        // but still process any buffered proofs so they are stored in state.
        if self.mock_mode {
            for proof in buffered_proofs {
                let _ = self.verify_execution_proof(&proof).await;
            }

            // Generate and inject dummy proofs directly so they can be served
            // to peers via ExecutionProofsByRoot and gossiped. In production the
            // VC proof service drives proof generation, but in mock/Kurtosis mode
            // the VC may not have a proof engine endpoint configured.
            let dummy = Self::generate_mock_proof(request_root);
            if let Ok(status) = self.state.write().insert_proof(dummy)
                && status.is_valid()
            {
                *self.latest_promoted.write() = Some(request_root);
            }
            tracing::debug!(target: "execution_layer", ?block_hash, ?request_root, "Mock proof engine: injected dummy proof");

            tracing::debug!(target: "execution_layer", ?block_hash, "Mock proof engine: returning VALID for new_payload");
            return Ok(PayloadStatusV1 {
                status: PayloadStatusV1Status::Valid,
                latest_valid_hash: Some(block_hash),
                validation_error: None,
            });
        }

        let mut status = PayloadStatusV1Status::Syncing;
        for proof in buffered_proofs {
            let proof_status = self.verify_execution_proof(&proof).await?;
            if proof_status.is_valid() {
                status = PayloadStatusV1Status::Valid;
            }
        }

        // Set latest_valid_hash when status is Valid, as required by process_payload_status.
        let latest_valid_hash = if status == PayloadStatusV1Status::Valid {
            Some(block_hash)
        } else {
            None
        };

        Ok(PayloadStatusV1 {
            status,
            latest_valid_hash,
            validation_error: None,
        })
    }

    /// Notify the proof engine of a forkchoice update.
    pub async fn forkchoice_updated(
        &self,
        forkchoice_state: ForkchoiceState,
    ) -> Result<ForkchoiceUpdatedResponse, ProofEngineError> {
        tracing::info!(target: "execution_layer", "Received forkchoice update: head {}, safe {}, finalized {}", forkchoice_state.head_block_hash, forkchoice_state.safe_block_hash, forkchoice_state.finalized_block_hash);

        // In mock mode, still update the internal state so the tree bootstraps
        // and proofs can promote from buffer → tree (needed for Valid status).
        // But always return Valid to avoid blocking fork choice during sync.
        if self.mock_mode {
            let _ = self.state.write().forkchoice_updated(forkchoice_state);
            tracing::debug!(target: "execution_layer", "Mock proof engine: returning VALID for forkchoice_updated");
            return Ok(ForkchoiceUpdatedResponse {
                payload_status: PayloadStatusV1 {
                    status: PayloadStatusV1Status::Valid,
                    latest_valid_hash: Some(forkchoice_state.head_block_hash),
                    validation_error: None,
                },
                payload_id: None,
            });
        }

        Ok(self.state.write().forkchoice_updated(forkchoice_state)?)
    }

    /// Request proof generation from the proof engine.
    ///
    /// SSZ-encodes the payload then sends it to `POST /v1/execution_proof_requests`.
    /// Returns the `new_payload_request_root` identifying this request.
    pub async fn request_proofs<E: EthSpec>(
        &self,
        new_payload_request: NewPayloadRequest<'_, E>,
        proof_attributes: ProofAttributes,
    ) -> Result<Hash256, ProofEngineError> {
        // In mock mode, proofs are injected directly in new_payload() on the BN side.
        // Return a dummy Hash256 so the VC's ProofService doesn't error.
        if self.mock_mode {
            tracing::debug!(target: "execution_layer", "Mock proof engine: returning dummy Hash256 for request_proofs");
            return Ok(Hash256::zero());
        }

        match new_payload_request {
            NewPayloadRequest::Bellatrix(_) => {
                Err(ProofEngineError::ForkNotSupported("Bellatrix".to_string()))
            }
            NewPayloadRequest::Capella(_) => {
                Err(ProofEngineError::ForkNotSupported("Capella".to_string()))
            }
            NewPayloadRequest::Deneb(_) => {
                Err(ProofEngineError::ForkNotSupported("Deneb".to_string()))
            }
            NewPayloadRequest::Electra(_) => {
                Err(ProofEngineError::ForkNotSupported("Electra".to_string()))
            }
            NewPayloadRequest::Fulu(fulu) => {
                self.proof_node
                    .request_proofs(fulu.as_ssz_bytes(), proof_attributes)
                    .await
            }
            NewPayloadRequest::Gloas(_) => {
                Err(ProofEngineError::ForkNotSupported("Gloas".to_string()))
            }
        }
    }

    /// Generate a dummy signed execution proof for mock/testing scenarios.
    ///
    /// Creates a minimal valid `SignedExecutionProof` with dummy proof data,
    /// proof_type 0, validator_index 0, and an empty signature. This is
    /// sufficient for the proof engine state to track and serve the proof.
    fn generate_mock_proof(request_root: Hash256) -> SignedExecutionProof {
        let mut proof_bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
        proof_bytes.extend_from_slice(&request_root.0[0..16]);

        SignedExecutionProof {
            message: ExecutionProof {
                proof_data: VariableList::new(proof_bytes)
                    .expect("mock proof data within max size"),
                proof_type: 0,
                public_input: PublicInput {
                    new_payload_request_root: request_root,
                },
            },
            validator_index: 0,
            signature: SignatureBytes::empty(),
        }
    }

    /// Snapshot the current state into a persisted form for serialization.
    pub fn to_persisted(&self) -> PersistedProofEngineState {
        let state = self.state.read();
        PersistedProofEngineState::from_state(&state)
    }

    /// Restore in-memory state from a previously persisted snapshot.
    pub fn restore_from_persisted(&self, persisted: PersistedProofEngineState) {
        let restored = persisted.to_state();
        *self.state.write() = restored;
    }
}
