//! Test network builder for creating local beacon node networks.
//!
//! Provides a builder pattern for setting up test networks with beacon nodes,
//! validator clients, and execution nodes. Used by simulator tests like
//! `basic_sim` and `proof_service_sim`.

pub use crate::basic_sim::SUGGESTED_FEE_RECIPIENT;
pub use crate::local_network::{LocalNetwork, LocalNetworkParams, NodeType};
pub use environment::LoggerConfig;
pub use environment::test_utils::TestEnvironment;
pub use logging::build_workspace_filter;
pub use node_test_rig::ApiTopic;
pub use node_test_rig::{
    ClientConfig, MockExecutionConfig, ValidatorFiles, environment::EnvironmentBuilder,
    testing_validator_config,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::path::PathBuf;
pub use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};
pub use types::{Address, ChainSpec, Epoch, EthSpec, MinimalEthSpec};

mod builder;
pub use builder::TestNetworkFixtureBuilder;

mod events;
pub use events::{
    wait_for_block, wait_for_finalization, wait_for_head, wait_for_slot, EventSubscription,
    SubscriptionCache,
};

use events::SubscriptionCache;
use tokio::time::Duration;

pub struct TestNetworkFixture<E: EthSpec = MinimalEthSpec> {
    pub env: TestEnvironment<E>,
    pub network: LocalNetwork<E>,
    pub config: TestConfig,
    subscription_cache: SubscriptionCache<E>,
}

pub struct TestConfig {
    pub client: ClientConfig,
    pub execution: MockExecutionConfig,
}

/// Configuration for SSE subscriptions.
#[derive(Debug, Clone)]
pub struct EventConfig {
    /// Whether SSE events are enabled for this test network.
    pub enabled: bool,
}

impl Default for EventConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl TestNetworkFixture {
    pub fn builder() -> TestNetworkFixtureBuilder {
        TestNetworkFixtureBuilder::default()
    }

    /// Mark all payloads as valid on execution nodes.
    pub fn payloads_valid(&mut self) {
        self.network
            .execution_nodes
            .write()
            .iter()
            .for_each(|node| {
                node.server.all_payloads_valid();
            });
    }

    /// Wait for the network to reach genesis by sleeping until the genesis time.
    pub async fn wait_for_genesis(&self) -> anyhow::Result<()> {
        let duration_to_genesis = self
            .network
            .duration_to_genesis()
            .await
            .map_err(anyhow::Error::msg)?;
        tokio::time::sleep(duration_to_genesis).await;
        Ok(())
    }

    /// Subscribe to events from a specific beacon node.
    ///
    /// Uses lazy subscription: the subscription is created on first use and cached
    /// for reuse. The subscription is direct to the `ServerSentEventHandler`,
    /// bypassing HTTP. Returns ALL event types.
    ///
    /// # Arguments
    /// * `node_index` - The index of the beacon node to subscribe to
    pub fn subscribe_to_node(&self, node_index: usize) -> anyhow::Result<EventSubscription<E>> {
        let handler = self
            .get_event_handler(node_index)
            .ok_or_else(|| anyhow::anyhow!("Event handler not available for node {}", node_index))?;

        let subscription = self
            .subscription_cache
            .subscribe_to_node(node_index, || handler.subscribe_all());

        Ok(subscription)
    }

    /// Wait for any event matching a predicate.
    ///
    /// Creates a subscription if needed, then waits for an event matching the predicate.
    ///
    /// # Arguments
    /// * `node_index` - The index of the beacon node to subscribe to
    /// * `predicate` - A function that returns true when the desired event is received
    /// * `timeout_duration` - Maximum time to wait for the event
    pub async fn wait_for_event<F>(
        &self,
        node_index: usize,
        predicate: F,
        timeout_duration: Duration,
    ) -> anyhow::Result<eth2::types::EventKind<E>>
    where
        F: Fn(&eth2::types::EventKind<E>) -> bool,
    {
        let mut subscription = self.subscribe_to_node(node_index)?;
        subscription.wait_for_with_timeout(predicate, timeout_duration).await
    }

    /// Wait for a specific slot to be reached.
    ///
    /// Subscribes to head events and waits for one with the given slot.
    ///
    /// # Arguments
    /// * `node_index` - The index of the beacon node to monitor
    /// * `slot` - The target slot to wait for
    /// * `timeout_duration` - Maximum time to wait
    pub async fn wait_for_slot(
        &self,
        node_index: usize,
        slot: types::Slot,
        timeout_duration: Duration,
    ) -> anyhow::Result<eth2::types::SseHead> {
        let event = self
            .wait_for_event(
                node_index,
                |event| matches!(event, eth2::types::EventKind::Head(head) if head.slot == slot),
                timeout_duration,
            )
            .await?;

        match event {
            eth2::types::EventKind::Head(head) => Ok(head),
            _ => unreachable!("Predicate ensures this is a Head event"),
        }
    }

    /// Wait for a block matching a predicate.
    ///
    /// Subscribes to block events and waits for one matching the predicate.
    ///
    /// # Arguments
    /// * `node_index` - The index of the beacon node to monitor
    /// * `predicate` - A function that returns true when the desired block is received
    /// * `timeout_duration` - Maximum time to wait
    pub async fn wait_for_block<F>(
        &self,
        node_index: usize,
        predicate: F,
        timeout_duration: Duration,
    ) -> anyhow::Result<eth2::types::SseBlock>
    where
        F: Fn(&eth2::types::SseBlock) -> bool,
    {
        let event = self
            .wait_for_event(
                node_index,
                |event| matches!(event, eth2::types::EventKind::Block(block) if predicate(block)),
                timeout_duration,
            )
            .await?;

        match event {
            eth2::types::EventKind::Block(block) => Ok(block),
            _ => unreachable!("Predicate ensures this is a Block event"),
        }
    }

    /// Wait for a head event matching a predicate.
    ///
    /// Subscribes to head events and waits for one matching the predicate.
    ///
    /// # Arguments
    /// * `node_index` - The index of the beacon node to monitor
    /// * `predicate` - A function that returns true when the desired head is received
    /// * `timeout_duration` - Maximum time to wait
    pub async fn wait_for_head<F>(
        &self,
        node_index: usize,
        predicate: F,
        timeout_duration: Duration,
    ) -> anyhow::Result<eth2::types::SseHead>
    where
        F: Fn(&eth2::types::SseHead) -> bool,
    {
        let event = self
            .wait_for_event(
                node_index,
                |event| matches!(event, eth2::types::EventKind::Head(head) if predicate(head)),
                timeout_duration,
            )
            .await?;

        match event {
            eth2::types::EventKind::Head(head) => Ok(head),
            _ => unreachable!("Predicate ensures this is a Head event"),
        }
    }

    /// Wait for a specific epoch to be finalized.
    ///
    /// Subscribes to finalized checkpoint events and waits for one with the given epoch.
    ///
    /// # Arguments
    /// * `node_index` - The index of the beacon node to monitor
    /// * `epoch` - The target epoch to wait for finalization
    /// * `timeout_duration` - Maximum time to wait
    pub async fn wait_for_finalization(
        &self,
        node_index: usize,
        epoch: Epoch,
        timeout_duration: Duration,
    ) -> anyhow::Result<eth2::types::SseFinalizedCheckpoint> {
        let event = self
            .wait_for_event(
                node_index,
                |event| matches!(event, eth2::types::EventKind::FinalizedCheckpoint(checkpoint) if checkpoint.epoch == epoch),
                timeout_duration,
            )
            .await?;

        match event {
            eth2::types::EventKind::FinalizedCheckpoint(checkpoint) => Ok(checkpoint),
            _ => unreachable!("Predicate ensures this is a FinalizedCheckpoint event"),
        }
    }

    /// Get the event handler for a specific beacon node.
    fn get_event_handler(
        &self,
        node_index: usize,
    ) -> Option<beacon_chain::ServerSentEventHandler<E>> {
        let beacon_nodes = self.network.beacon_nodes.read();
        let node = beacon_nodes.get(node_index)?;
        let beacon_chain = node.client.beacon_chain()?;
        beacon_chain.event_handler.clone()
    }

    /// Clear all cached SSE subscriptions.
    pub fn clear_event_subscriptions(&self) {
        self.subscription_cache.clear();
    }

    /// Clear cached SSE subscriptions for a specific node.
    pub fn clear_event_subscriptions_for_node(&self, node_index: usize) {
        self.subscription_cache.clear_for_node(node_index);
    }
}

// Ignore this for now because it conflicts with the `proof_engine` testing crate.
// We should migrate to defaulting to unused ports assigned by the OS instead of hardcoding ports.
#[tokio::test]
#[ignore]
async fn test_network_fixture_build() -> anyhow::Result<()> {
    let mut fixture = TestNetworkFixtureBuilder::default()
        .map_network_params(|params| {
            params.genesis_delay = 20;
        })
        .map_spec(|spec| {
            spec.seconds_per_slot = 1;
            spec.slot_duration_ms = 1000;
            spec.min_genesis_time = 0;
            spec.altair_fork_epoch = Some(Epoch::new(0));
            spec.bellatrix_fork_epoch = Some(Epoch::new(0));
            spec.capella_fork_epoch = Some(Epoch::new(0));
            spec.deneb_fork_epoch = Some(Epoch::new(0));
            spec.electra_fork_epoch = Some(Epoch::new(0));
            spec.fulu_fork_epoch = Some(Epoch::new(2));
        })
        .build()
        .await?;
    fixture.payloads_valid();

    fixture.wait_for_genesis().await?;

    tokio::time::sleep(std::time::Duration::from_secs(60)).await;

    Ok(())
}
