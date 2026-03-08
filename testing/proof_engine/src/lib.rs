//! A test suite for the proof engine, using a local test network fixture.

#[cfg(test)]
mod test {
    use std::time::Duration;

    use node_test_rig::ProofEngineEvent;
    use simulator::test_utils::*;

    /// A base test network fixture builder for eip-8025 testing.
    ///
    /// This fixture has:
    /// - all forks up to and including fulu activate at genesis
    /// - all nodes configured with 1 second slots to speed up tests
    /// - a minimal genesis time to allow tests to start quickly
    ///
    /// - 1 vanilla beacon node
    /// - 1 proof generator node
    /// - 1 proof verifier node
    fn test_fixture_builder_base() -> TestNetworkFixtureBuilder {
        TestNetworkFixture::builder()
            .map_spec(|spec| {
                spec.seconds_per_slot = 1;
                spec.slot_duration_ms = 1000;
                spec.min_genesis_time = 0;
                spec.altair_fork_epoch = Some(Epoch::new(0));
                spec.bellatrix_fork_epoch = Some(Epoch::new(0));
                spec.capella_fork_epoch = Some(Epoch::new(0));
                spec.deneb_fork_epoch = Some(Epoch::new(0));
                spec.electra_fork_epoch = Some(Epoch::new(0));
                spec.fulu_fork_epoch = Some(Epoch::new(0));
            })
            .with_network_params(LocalNetworkParams {
                validator_count: 4,
                node_count: 1,
                proposer_nodes: 0,
                extra_nodes: 0,
                proof_generator_nodes: 1,
                proof_verifier_nodes: 1,
                genesis_delay: 20,
            })
    }

    #[tokio::test]
    async fn test_proof_engine_basic() -> anyhow::Result<()> {
        let mut fixture = test_fixture_builder_base()
            .with_log_level(LevelFilter::DEBUG)
            .with_log_dir("proof-engine".into())
            .build()
            .await?;
        fixture.payloads_valid();
        fixture.wait_for_genesis().await?;

        // Verify continuous operation
        tokio::time::sleep(Duration::from_secs(60)).await;

        let requests = fixture
            .network
            .proof_engines
            .read()
            .first()
            .unwrap()
            .server
            .get_proof_requests();

        assert!(
            requests.len() >= 2,
            "Should have received multiple proof requests"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_proof_engine_sync() -> anyhow::Result<()> {
        let mut fixture = test_fixture_builder_base()
            .map_spec(|spec| {
                // Collapse all columns onto a single subnet and reduce the total number of
                // custody groups so the small 2-node network can fully cover them.
                spec.data_column_sidecar_subnet_count = 1;
                spec.number_of_custody_groups = 8;
            })
            .map_network_params(|params| {
                params.proof_verifier_nodes = 0;
            })
            .with_log_level(LevelFilter::DEBUG)
            .with_log_dir("proof-engine-sync".into())
            .build()
            .await?;
        fixture.payloads_valid();
        fixture.wait_for_genesis().await?;

        tokio::time::sleep(Duration::from_secs(60)).await;

        // Now lets add a new proof verifier node and observe the sync behaviour.
        let net = fixture.network.clone();
        info!(target: "simulator", "Adding 1 proof verifier beacon nodes to the network");
        fixture.network.executor().spawn(
            async move {
                net.add_beacon_node(
                    fixture.config.client.clone(),
                    fixture.config.execution.clone(),
                    NodeType::ProofVerifier,
                )
                .await
                .map_err(anyhow::Error::msg)
                .expect("should not error");
            },
            "add_proof_verifier",
        );

        tokio::time::sleep(Duration::from_secs(60)).await;

        Ok(())
    }

    /// Test that proof engine events are properly emitted and can be subscribed to.
    ///
    /// This test demonstrates the subscription-based event pattern for observing
    /// proof engine activity without polling.
    #[tokio::test]
    async fn test_proof_engine_event_subscription() -> anyhow::Result<()> {
        let mut fixture = test_fixture_builder_base()
            .with_log_level(LevelFilter::INFO)
            .with_log_dir("proof-engine-events".into())
            .build()
            .await?;
        fixture.payloads_valid();
        fixture.wait_for_genesis().await?;

        // Subscribe to proof engine events using the fixture helper
        let mut subscription = fixture.subscribe_to_proof_engine(0)?;

        // Wait for a proof request event using the helper method
        let record = subscription
            .wait_for_proof_request(None, Duration::from_secs(45))
            .await?;

        tracing::info!(
            target: "test",
            proof_gen_id = ?hex::encode(record.proof_gen_id),
            num_proof_types = record.proof_types.len(),
            "Received proof request event"
        );
        assert!(!record.proof_types.is_empty(), "Should have proof types");

        // Wait for more events to accumulate
        tokio::time::sleep(Duration::from_secs(30)).await;

        // Collect all pending events
        let events = subscription.collect_pending();
        tracing::info!(target: "test", count = events.len(), "Collected pending events");

        // Count proof request and sent events
        let proof_requests: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, ProofEngineEvent::ProofRequestReceived { .. }))
            .collect();
        let proofs_sent: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, ProofEngineEvent::ProofSentToValidator { .. }))
            .collect();

        tracing::info!(
            target: "test",
            proof_requests = proof_requests.len(),
            proofs_sent = proofs_sent.len(),
            "Event summary"
        );

        assert!(
            !proof_requests.is_empty(),
            "Should have received at least one proof request event"
        );

        Ok(())
    }

    /// Test that verifies proof request details are correctly captured.
    #[tokio::test]
    async fn test_proof_request_details() -> anyhow::Result<()> {
        let mut fixture = test_fixture_builder_base()
            .with_log_level(LevelFilter::INFO)
            .with_log_dir("proof-engine-details".into())
            .build()
            .await?;
        fixture.payloads_valid();
        fixture.wait_for_genesis().await?;

        // Subscribe to events before waiting for proofs
        let mut subscription = fixture.subscribe_to_proof_engine(0)?;

        // Wait for network to produce some blocks and proof requests
        tokio::time::sleep(Duration::from_secs(45)).await;

        // Collect proof request records from events
        let events = subscription.collect_pending();
        let proof_requests: Vec<_> = events
            .into_iter()
            .filter_map(|e| match e {
                ProofEngineEvent::ProofRequestReceived { record } => Some(record),
                _ => None,
            })
            .collect();

        // Also get the stored requests
        let proof_engine = fixture
            .network
            .proof_engines
            .read()
            .first()
            .cloned()
            .expect("Should have a proof engine");
        let stored_requests = proof_engine.server.get_proof_requests();

        // Verify that events match stored requests
        assert_eq!(
            proof_requests.len(),
            stored_requests.len(),
            "Event count should match stored request count"
        );

        // Verify each request has valid data
        for request in &stored_requests {
            assert!(
                !request.proof_types.is_empty(),
                "Each request should have at least one proof type"
            );
            assert!(
                request.new_payload_request_root != types::Hash256::ZERO,
                "Request root should not be zero"
            );
            tracing::info!(
                target: "test",
                proof_gen_id = ?hex::encode(request.proof_gen_id),
                num_types = request.proof_types.len(),
                "Verified proof request"
            );
        }

        Ok(())
    }

    /// Test that demonstrates waiting for a specific number of proof events.
    #[tokio::test]
    async fn test_wait_for_multiple_proofs() -> anyhow::Result<()> {
        let mut fixture = test_fixture_builder_base()
            .with_log_level(LevelFilter::INFO)
            .with_log_dir("proof-engine-multiple".into())
            .build()
            .await?;
        fixture.payloads_valid();
        fixture.wait_for_genesis().await?;

        // Subscribe to events using the fixture helper
        let mut subscription = fixture.subscribe_to_proof_engine(0)?;

        // Wait for at least 2 proof request events
        let target_count = 2;
        let timeout_duration = Duration::from_secs(90);

        let received_count = subscription.count_proof_requests(timeout_duration).await;

        assert!(
            received_count >= target_count,
            "Expected at least {} proof requests, received {}",
            target_count,
            received_count
        );

        tracing::info!(
            target: "test",
            received_count,
            "Successfully received expected proof requests"
        );

        Ok(())
    }

    /// Test proof engine with multiple proof generators.
    #[tokio::test]
    async fn test_multiple_proof_generators() -> anyhow::Result<()> {
        let mut fixture = test_fixture_builder_base()
            .map_network_params(|params| {
                params.proof_generator_nodes = 2;
                params.proof_verifier_nodes = 1;
            })
            .with_log_level(LevelFilter::INFO)
            .with_log_dir("proof-engine-multi-gen".into())
            .build()
            .await?;
        fixture.payloads_valid();
        fixture.wait_for_genesis().await?;

        // Subscribe to events from all proof engines using fixture helper
        let mut subscription_0 = fixture.subscribe_to_proof_engine(0)?;
        let mut subscription_1 = fixture.subscribe_to_proof_engine(1)?;

        // Wait for some activity
        tokio::time::sleep(Duration::from_secs(60)).await;

        // Collect events from both proof engines
        let events_0 = subscription_0.collect_pending();
        let events_1 = subscription_1.collect_pending();

        // Count proof requests from each engine
        let count_0 = events_0
            .iter()
            .filter(|e| matches!(e, ProofEngineEvent::ProofRequestReceived { .. }))
            .count();
        let count_1 = events_1
            .iter()
            .filter(|e| matches!(e, ProofEngineEvent::ProofRequestReceived { .. }))
            .count();

        let total_requests = count_0 + count_1;

        tracing::info!(
            target: "test",
            engine_0 = count_0,
            engine_1 = count_1,
            total = total_requests,
            "Proof engine request counts"
        );

        // With multiple proof generators, we expect at least some activity
        assert!(
            total_requests > 0,
            "Should have received proof requests across all engines"
        );

        Ok(())
    }

    /// Test waiting for a specific proof to be sent to validator.
    #[tokio::test]
    async fn test_wait_for_proof_sent_to_validator() -> anyhow::Result<()> {
        let mut fixture = test_fixture_builder_base()
            .with_log_level(LevelFilter::INFO)
            .with_log_dir("proof-engine-sent".into())
            .build()
            .await?;
        fixture.payloads_valid();
        fixture.wait_for_genesis().await?;

        // Subscribe to proof engine events
        let mut subscription = fixture.subscribe_to_proof_engine(0)?;

        // Wait for a proof to be sent to validator
        let execution_proof = subscription
            .wait_for_proof_sent(None, Duration::from_secs(90))
            .await?;

        tracing::info!(
            target: "test",
            proof_type = ?execution_proof.proof_type,
            "Received proof sent to validator event"
        );

        // Verify the proof data
        assert!(
            !execution_proof.proof_data.is_empty(),
            "Proof should have data"
        );
        assert!(
            execution_proof.public_input.new_payload_request_root != types::Hash256::ZERO,
            "Proof should have valid request root"
        );

        Ok(())
    }
}
