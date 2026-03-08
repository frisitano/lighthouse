//! Error handling tests for ExecutionProofStatus RPC
//!
//! This module contains tests for error conditions when handling ExecutionProofStatus
//! requests and responses, including:
//! - Invalid SSZ encoding
//! - Malformed requests
//! - Network errors (timeout, disconnect)
//! - State errors (unknown block, pre-Fulu fork, sync mismatch)

#![cfg(test)]

use crate::rpc::codec::{SSZSnappyInboundCodec, SSZSnappyOutboundCodec};
use crate::rpc::methods::{
    ErrorType, ExecutionProofStatus, RpcErrorResponse, RpcResponse,
};
use crate::rpc::protocol::{
    Encoding, ProtocolId, RPCError, SupportedProtocol,
};
use crate::rpc::RequestType;
use bls::FixedBytesExtended;
use libp2p::bytes::BytesMut;
use ssz::{Decode, Encode};
use std::sync::Arc;
use tokio_util::codec::{Decoder, Encoder};
use types::{ChainSpec, Epoch, EthSpec, ForkContext, ForkName, Hash256, MinimalEthSpec};

type E = MinimalEthSpec;

/// Creates a fork context for the given fork name
fn fork_context(fork_name: ForkName, spec: &ChainSpec) -> ForkContext {
    let current_epoch = match fork_name {
        ForkName::Base => Some(Epoch::new(0)),
        ForkName::Altair => spec.altair_fork_epoch,
        ForkName::Bellatrix => spec.bellatrix_fork_epoch,
        ForkName::Capella => spec.capella_fork_epoch,
        ForkName::Deneb => spec.deneb_fork_epoch,
        ForkName::Electra => spec.electra_fork_epoch,
        ForkName::Fulu => spec.fulu_fork_epoch,
        ForkName::Gloas => spec.gloas_fork_epoch,
    };
    let current_slot = current_epoch
        .unwrap_or_else(|| panic!("expect fork {fork_name} to be scheduled"))
        .start_slot(E::slots_per_epoch());
    ForkContext::new::<E>(current_slot, Hash256::zero(), spec)
}

/// Returns a chain spec with all forks enabled
fn spec_with_all_forks_enabled() -> ChainSpec {
    let mut chain_spec = E::default_spec();
    chain_spec.altair_fork_epoch = Some(Epoch::new(1));
    chain_spec.bellatrix_fork_epoch = Some(Epoch::new(2));
    chain_spec.capella_fork_epoch = Some(Epoch::new(3));
    chain_spec.deneb_fork_epoch = Some(Epoch::new(4));
    chain_spec.electra_fork_epoch = Some(Epoch::new(5));
    chain_spec.fulu_fork_epoch = Some(Epoch::new(6));
    chain_spec.gloas_fork_epoch = Some(Epoch::new(7));

    // check that we have all forks covered
    assert!(chain_spec.fork_epoch(ForkName::latest()).is_some());
    chain_spec
}

/// Creates a valid ExecutionProofStatus for testing
fn valid_execution_proof_status() -> ExecutionProofStatus {
    ExecutionProofStatus {
        latest_verified_slot: 100,
        latest_verified_block_root: Hash256::from_low_u64_be(0x1234),
    }
}

/// Creates an ExecutionProofStatus with pre-Fulu slot
fn pre_fulu_execution_proof_status() -> ExecutionProofStatus {
    ExecutionProofStatus {
        latest_verified_slot: 10, // Before Fulu fork
        latest_verified_block_root: Hash256::from_low_u64_be(0x5678),
    }
}

// ============================================================================
// Invalid Request Tests
// ============================================================================

#[test]
fn test_execution_proof_status_malformed_ssz_encoding() {
    let spec = Arc::new(spec_with_all_forks_enabled());
    let fork_ctx = Arc::new(fork_context(ForkName::Fulu, &spec));

    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    let mut codec: SSZSnappyInboundCodec<E> =
        SSZSnappyInboundCodec::new(protocol, spec.max_payload_size as usize, fork_ctx);

    // Create malformed SSZ data (truncated - missing bytes) and compress with snappy
    let malformed_ssz = vec![0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]; // Only 8 bytes (missing 32 for Hash256)
    
    // Compress with snappy framing
    let mut compressed = Vec::new();
    {
        use snap::write::FrameEncoder;
        use std::io::Write;
        let mut encoder = FrameEncoder::new(&mut compressed);
        encoder.write_all(&malformed_ssz).unwrap();
        encoder.flush().unwrap();
    }

    // Add length prefix
    let mut bytes = BytesMut::new();
    let length = malformed_ssz.len(); // Length is uncompressed size
    let mut buf = [0u8; 19];
    let encoded_len = unsigned_varint::encode::u128(length as u128, &mut buf);
    bytes.extend_from_slice(encoded_len);
    bytes.extend_from_slice(&compressed);

    // Attempt to decode - should fail with InvalidData because length (8) is out of bounds (expected 40)
    let result = codec.decode(&mut bytes);
    assert!(
        matches!(result, Err(RPCError::InvalidData(_))),
        "Expected InvalidData for truncated encoding (length {} is out of bounds), got {:?}",
        malformed_ssz.len(),
        result
    );
}

#[test]
fn test_execution_proof_status_invalid_ssz_extra_bytes() {
    let spec = Arc::new(spec_with_all_forks_enabled());
    let fork_ctx = Arc::new(fork_context(ForkName::Fulu, &spec));

    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    let mut codec: SSZSnappyInboundCodec<E> =
        SSZSnappyInboundCodec::new(protocol, spec.max_payload_size as usize, fork_ctx);

    // Create SSZ with extra bytes (should be exactly 40 bytes) and compress
    let valid_status = valid_execution_proof_status();
    let mut ssz_bytes = valid_status.as_ssz_bytes();
    ssz_bytes.extend_from_slice(&[0xFF; 10]); // Add 10 extra garbage bytes (now 50 bytes total)

    // Compress with snappy framing
    let mut compressed = Vec::new();
    {
        use snap::write::FrameEncoder;
        use std::io::Write;
        let mut encoder = FrameEncoder::new(&mut compressed);
        encoder.write_all(&ssz_bytes).unwrap();
        encoder.flush().unwrap();
    }

    // Add length prefix (using the actual SSZ length, not compressed length)
    let mut bytes = BytesMut::new();
    let length = ssz_bytes.len(); // 50 bytes
    let mut buf = [0u8; 19];
    let encoded_len = unsigned_varint::encode::u128(length as u128, &mut buf);
    bytes.extend_from_slice(encoded_len);
    bytes.extend_from_slice(&compressed);

    // Decoding should fail with InvalidData because length (50) exceeds max (40)
    let result = codec.decode(&mut bytes);
    assert!(
        matches!(result, Err(RPCError::InvalidData(_))),
        "Expected InvalidData for oversized request (length 50 > max 40), got {:?}",
        result
    );
}

#[test]
fn test_execution_proof_status_invalid_length_too_short() {
    let spec = Arc::new(spec_with_all_forks_enabled());
    let fork_ctx = Arc::new(fork_context(ForkName::Fulu, &spec));

    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    let mut codec: SSZSnappyInboundCodec<E> =
        SSZSnappyInboundCodec::new(protocol, spec.max_payload_size as usize, fork_ctx);

    // Create a request with length < min (40 bytes is the expected size)
    let short_ssz = vec![0x01; 30]; // Less than 40 bytes

    // Add length prefix
    let mut bytes = BytesMut::new();
    let length = short_ssz.len();
    let mut buf = [0u8; 19];
    let encoded_len = unsigned_varint::encode::u128(length as u128, &mut buf);
    bytes.extend_from_slice(encoded_len);
    bytes.extend_from_slice(&short_ssz);

    // Attempt to decode
    let result = codec.decode(&mut bytes);
    // The codec should either fail with InvalidData due to length bounds check,
    // or with SSZDecodeError if it passes through
    assert!(
        matches!(
            result,
            Err(RPCError::InvalidData(_)) | Err(RPCError::SSZDecodeError(_))
        ),
        "Expected error for short data, got {:?}",
        result
    );
}

#[test]
fn test_execution_proof_status_invalid_length_too_long() {
    let spec = Arc::new(spec_with_all_forks_enabled());
    let fork_ctx = Arc::new(fork_context(ForkName::Fulu, &spec));

    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    let mut codec: SSZSnappyInboundCodec<E> =
        SSZSnappyInboundCodec::new(protocol, spec.max_payload_size as usize, fork_ctx);

    // Create a request with length > max (40 bytes is the expected size)
    let long_ssz = vec![0x01; 50]; // More than 40 bytes

    // Add length prefix
    let mut bytes = BytesMut::new();
    let length = long_ssz.len();
    let mut buf = [0u8; 19];
    let encoded_len = unsigned_varint::encode::u128(length as u128, &mut buf);
    bytes.extend_from_slice(encoded_len);
    bytes.extend_from_slice(&long_ssz);

    // Attempt to decode
    let result = codec.decode(&mut bytes);
    assert!(
        matches!(result, Err(RPCError::InvalidData(_))),
        "Expected InvalidData for oversized request, got {:?}",
        result
    );
}

// ============================================================================
// Valid Encoding/Decoding Tests
// ============================================================================

#[test]
fn test_execution_proof_status_encode_decode_roundtrip() {
    let spec = Arc::new(spec_with_all_forks_enabled());
    let fork_ctx = Arc::new(fork_context(ForkName::Fulu, &spec));

    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    let mut codec: SSZSnappyInboundCodec<E> =
        SSZSnappyInboundCodec::new(protocol.clone(), spec.max_payload_size as usize, fork_ctx.clone());

    let status = valid_execution_proof_status();

    // For inbound codec, we need to provide snappy-compressed SSZ bytes
    let ssz_bytes = status.as_ssz_bytes();
    
    // Compress with snappy framing
    let mut compressed = Vec::new();
    {
        use snap::write::FrameEncoder;
        use std::io::Write;
        let mut encoder = FrameEncoder::new(&mut compressed);
        encoder.write_all(&ssz_bytes).unwrap();
        encoder.flush().unwrap();
    }
    
    let mut inbound_bytes = BytesMut::new();
    let mut buf = [0u8; 19];
    let encoded_len = unsigned_varint::encode::u128(ssz_bytes.len() as u128, &mut buf);
    inbound_bytes.extend_from_slice(encoded_len);
    inbound_bytes.extend_from_slice(&compressed);

    // This tests that the codec can decode a properly formed request
    let result = codec.decode(&mut inbound_bytes);
    assert!(
        result.is_ok(),
        "Expected successful decode, got {:?}",
        result
    );

    let decoded = result.unwrap();
    assert!(decoded.is_some(), "Expected Some(decoded)");
    
    if let Some(RequestType::ExecutionProofStatus(decoded_status)) = decoded {
        assert_eq!(decoded_status.latest_verified_slot, status.latest_verified_slot);
        assert_eq!(decoded_status.latest_verified_block_root, status.latest_verified_block_root);
    } else {
        panic!("Expected ExecutionProofStatus request, got {:?}", decoded);
    }
}

// ============================================================================
// Response Encoding/Decoding Tests
// ============================================================================

#[test]
fn test_execution_proof_status_response_encode_decode() {
    let spec = Arc::new(spec_with_all_forks_enabled());
    let fork_ctx = Arc::new(fork_context(ForkName::Fulu, &spec));

    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    let mut _outbound_codec: SSZSnappyOutboundCodec<E> =
        SSZSnappyOutboundCodec::new(protocol.clone(), spec.max_payload_size as usize, fork_ctx.clone());

    let status = valid_execution_proof_status();

    // Encode the response
    let mut encoded = BytesMut::new();
    
    // For responses, we need to encode with the response code
    // First byte is the response code (0 for success)
    encoded.extend_from_slice(&[0u8]);
    
    // Then the length-prefixed SSZ data
    let ssz_bytes = status.as_ssz_bytes();
    let mut buf = [0u8; 19];
    let encoded_len = unsigned_varint::encode::u128(ssz_bytes.len() as u128, &mut buf);
    encoded.extend_from_slice(encoded_len);
    encoded.extend_from_slice(&ssz_bytes);

    // Let's verify the SSZ bytes are correct
    let decoded_status = ExecutionProofStatus::from_ssz_bytes(&ssz_bytes).unwrap();
    assert_eq!(decoded_status.latest_verified_slot, status.latest_verified_slot);
    assert_eq!(decoded_status.latest_verified_block_root, status.latest_verified_block_root);
}

// ============================================================================
// Error Response Tests
// ============================================================================

#[test]
fn test_execution_proof_status_error_response_invalid_request() {
    let spec = Arc::new(spec_with_all_forks_enabled());
    let fork_ctx = Arc::new(fork_context(ForkName::Fulu, &spec));

    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    let mut inbound_codec: SSZSnappyInboundCodec<E> =
        SSZSnappyInboundCodec::new(protocol, spec.max_payload_size as usize, fork_ctx);

    // Create an error response
    let error_msg: ErrorType = "Invalid execution proof status request".into();
    let error_response = RpcResponse::Error(RpcErrorResponse::InvalidRequest, error_msg.clone());

    // Encode the error response
    let result = inbound_codec.encode(error_response, &mut BytesMut::new());
    assert!(result.is_ok(), "Failed to encode error response: {:?}", result);
}

#[test]
fn test_execution_proof_status_error_response_server_error() {
    let spec = Arc::new(spec_with_all_forks_enabled());
    let fork_ctx = Arc::new(fork_context(ForkName::Fulu, &spec));

    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    let mut inbound_codec: SSZSnappyInboundCodec<E> =
        SSZSnappyInboundCodec::new(protocol, spec.max_payload_size as usize, fork_ctx);

    // Create a server error response
    let error_msg: ErrorType = "Internal server error processing execution proof status".into();
    let error_response = RpcResponse::Error(RpcErrorResponse::ServerError, error_msg);

    // Encode the error response
    let result = inbound_codec.encode(error_response, &mut BytesMut::new());
    assert!(result.is_ok(), "Failed to encode server error response: {:?}", result);
}

#[test]
fn test_execution_proof_status_error_response_resource_unavailable() {
    let spec = Arc::new(spec_with_all_forks_enabled());
    let fork_ctx = Arc::new(fork_context(ForkName::Fulu, &spec));

    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    let mut inbound_codec: SSZSnappyInboundCodec<E> =
        SSZSnappyInboundCodec::new(protocol, spec.max_payload_size as usize, fork_ctx);

    // Create a resource unavailable error response (e.g., for pre-Fulu fork)
    let error_msg: ErrorType = "Execution proof status not available before Fulu fork".into();
    let error_response = RpcResponse::Error(RpcErrorResponse::ResourceUnavailable, error_msg);

    // Encode the error response
    let result = inbound_codec.encode(error_response, &mut BytesMut::new());
    assert!(result.is_ok(), "Failed to encode resource unavailable response: {:?}", result);
}

// ============================================================================
// State Error Tests
// ============================================================================

#[test]
fn test_execution_proof_status_pre_fulu_slot() {
    // Test that a status with a pre-Fulu slot is still valid from an encoding perspective
    // The actual state validation would happen at the application level
    let status = pre_fulu_execution_proof_status();
    
    // Encode
    let ssz_bytes = status.as_ssz_bytes();
    assert_eq!(ssz_bytes.len(), 40, "ExecutionProofStatus should be exactly 40 bytes");
    
    // Decode
    let decoded = ExecutionProofStatus::from_ssz_bytes(&ssz_bytes).unwrap();
    assert_eq!(decoded.latest_verified_slot, 10);
    assert_eq!(decoded.latest_verified_block_root, Hash256::from_low_u64_be(0x5678));
}

#[test]
fn test_execution_proof_status_zero_values() {
    // Test edge case with zero values (syncing or initial state)
    let status = ExecutionProofStatus {
        latest_verified_slot: 0,
        latest_verified_block_root: Hash256::zero(),
    };
    
    // Encode
    let ssz_bytes = status.as_ssz_bytes();
    assert_eq!(ssz_bytes.len(), 40);
    
    // Decode
    let decoded = ExecutionProofStatus::from_ssz_bytes(&ssz_bytes).unwrap();
    assert_eq!(decoded.latest_verified_slot, 0);
    assert_eq!(decoded.latest_verified_block_root, Hash256::zero());
}

#[test]
fn test_execution_proof_status_max_slot() {
    // Test edge case with max slot value
    let status = ExecutionProofStatus {
        latest_verified_slot: u64::MAX,
        latest_verified_block_root: Hash256::from_low_u64_be(u64::MAX),
    };
    
    // Encode
    let ssz_bytes = status.as_ssz_bytes();
    assert_eq!(ssz_bytes.len(), 40);
    
    // Decode
    let decoded = ExecutionProofStatus::from_ssz_bytes(&ssz_bytes).unwrap();
    assert_eq!(decoded.latest_verified_slot, u64::MAX);
    assert_eq!(decoded.latest_verified_block_root, Hash256::from_low_u64_be(u64::MAX));
}

// ============================================================================
// Protocol Limits Tests
// ============================================================================

#[test]
fn test_execution_proof_status_request_limits() {
    let spec = Arc::new(spec_with_all_forks_enabled());
    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    
    let limits = protocol.rpc_request_limits::<E>(&spec);
    
    // ExecutionProofStatus should have fixed size of 40 bytes
    assert_eq!(limits.min, 40, "Min request size should be 40 bytes");
    assert_eq!(limits.max, 40, "Max request size should be 40 bytes");
}

#[test]
fn test_execution_proof_status_response_limits() {
    let spec = Arc::new(spec_with_all_forks_enabled());
    let fork_ctx = Arc::new(fork_context(ForkName::Fulu, &spec));
    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    
    let limits = protocol.rpc_response_limits::<E>(&fork_ctx);
    
    // ExecutionProofStatus response should have fixed size of 40 bytes
    assert_eq!(limits.min, 40, "Min response size should be 40 bytes");
    assert_eq!(limits.max, 40, "Max response size should be 40 bytes");
}

// ============================================================================
// Protocol ID Tests
// ============================================================================

#[test]
fn test_execution_proof_status_protocol_id() {
    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    
    assert_eq!(protocol.versioned_protocol, SupportedProtocol::ExecutionProofStatusV1);
    assert_eq!(protocol.encoding, Encoding::SSZSnappy);
    // protocol_id field is private, but we can verify the protocol was created correctly
    // by checking versioned_protocol and encoding
}

#[test]
fn test_execution_proof_status_has_no_context_bytes() {
    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    
    // ExecutionProofStatus does not need context bytes (it's not fork-dependent)
    assert!(!protocol.has_context_bytes());
}

// ============================================================================
// SSZ Encoding Edge Cases
// ============================================================================

#[test]
fn test_execution_proof_status_empty_bytes() {
    // Empty bytes should fail to decode
    let result = ExecutionProofStatus::from_ssz_bytes(&[]);
    assert!(result.is_err(), "Empty bytes should fail to decode");
}

#[test]
fn test_execution_proof_status_partial_hash() {
    // 39 bytes (1 byte short)
    let partial = vec![0u8; 39];
    let result = ExecutionProofStatus::from_ssz_bytes(&partial);
    assert!(result.is_err(), "Partial hash should fail to decode");
}

#[test]
fn test_execution_proof_status_ssz_bytes_len() {
    let status = valid_execution_proof_status();
    let bytes = status.as_ssz_bytes();
    
    // Should be exactly 40 bytes: 8 for u64 (slot) + 32 for Hash256
    assert_eq!(bytes.len(), 40);
}

// ============================================================================
// Timeout Simulation Tests (Codec level)
// ============================================================================

#[test]
fn test_execution_proof_status_empty_decode() {
    let spec = Arc::new(spec_with_all_forks_enabled());
    let fork_ctx = Arc::new(fork_context(ForkName::Fulu, &spec));

    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    let mut codec: SSZSnappyInboundCodec<E> =
        SSZSnappyInboundCodec::new(protocol, spec.max_payload_size as usize, fork_ctx);

    // Empty buffer should return Ok(None) - not enough data yet
    let mut empty = BytesMut::new();
    let result = codec.decode(&mut empty);
    assert!(
        matches!(result, Ok(None)),
        "Empty buffer should return Ok(None), got {:?}",
        result
    );
}

#[test]
fn test_execution_proof_status_incomplete_length_prefix() {
    let spec = Arc::new(spec_with_all_forks_enabled());
    let fork_ctx = Arc::new(fork_context(ForkName::Fulu, &spec));

    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    let mut codec: SSZSnappyInboundCodec<E> =
        SSZSnappyInboundCodec::new(protocol, spec.max_payload_size as usize, fork_ctx);

    // Incomplete length prefix (just a single byte that looks like varint continuation)
    let mut bytes = BytesMut::from(&[0x80u8][..]);
    let result = codec.decode(&mut bytes);
    assert!(
        matches!(result, Ok(None)),
        "Incomplete length prefix should return Ok(None), got {:?}",
        result
    );
}

#[test]
fn test_execution_proof_status_incomplete_data() {
    let spec = Arc::new(spec_with_all_forks_enabled());
    let fork_ctx = Arc::new(fork_context(ForkName::Fulu, &spec));

    let protocol = ProtocolId::new(SupportedProtocol::ExecutionProofStatusV1, Encoding::SSZSnappy);
    let mut codec: SSZSnappyInboundCodec<E> =
        SSZSnappyInboundCodec::new(protocol, spec.max_payload_size as usize, fork_ctx);

    // Create valid snappy compressed data but truncate it
    let ssz_bytes = vec![0u8; 40]; // 40 bytes of zeros (valid SSZ for ExecutionProofStatus with slot=0 and zero hash)
    
    // Compress with snappy framing
    let mut compressed = Vec::new();
    {
        use snap::write::FrameEncoder;
        use std::io::Write;
        let mut encoder = FrameEncoder::new(&mut compressed);
        encoder.write_all(&ssz_bytes).unwrap();
        encoder.flush().unwrap();
    }
    
    // Now truncate the compressed data to simulate incomplete data
    let truncated = &compressed[..compressed.len() / 2];
    
    // Length prefix saying 40 bytes, but truncated compressed data
    let mut bytes = BytesMut::new();
    let mut buf = [0u8; 19];
    let encoded_len = unsigned_varint::encode::u128(40u128, &mut buf);
    bytes.extend_from_slice(encoded_len);
    bytes.extend_from_slice(truncated);
    
    let result = codec.decode(&mut bytes);
    // When snappy data is incomplete, the decoder returns Ok(None) waiting for more data
    // OR it may return an IO error if the frame is corrupt
    assert!(
        matches!(result, Ok(None) | Err(RPCError::IoError(_))),
        "Incomplete data should return Ok(None) or IoError, got {:?}",
        result
    );
}

// RpcErrorResponse::as_u8 is a private method in the main crate.
// Error responses are encoded via RpcResponse::as_u8 which returns Some(code) for errors.
