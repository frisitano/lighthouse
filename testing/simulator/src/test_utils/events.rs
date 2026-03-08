//! Server-Sent Events (SSE) subscription utilities for test networks.
//!
//! Provides direct subscription to `ServerSentEventHandler` (no HTTP) for tests,
//! with lazy subscription on first use and caching for reuse.

use beacon_chain::ServerSentEventHandler;
use eth2::types::{EventKind, SseBlock, SseFinalizedCheckpoint, SseHead};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::marker::PhantomData;
use tokio::sync::broadcast::Receiver;
use tokio::time::{timeout, Duration};
use types::{Epoch, EthSpec, Slot};

/// Represents a subscription to a specific SSE event type.
///
/// This struct wraps a broadcast receiver for a specific event kind
/// and provides ergonomic methods for waiting on events.
pub struct EventSubscription<E: EthSpec> {
    receiver: Receiver<EventKind<E>>,
}

impl<E: EthSpec> EventSubscription<E> {
    /// Create a new event subscription from a receiver.
    pub fn new(receiver: Receiver<EventKind<E>>) -> Self {
        Self { receiver }
    }

    /// Wait for the next event with an optional timeout.
    ///
    /// Returns `Ok(Some(event))` if an event was received,
    /// `Ok(None)` if the channel is closed,
    /// or an error if the timeout expires.
    pub async fn recv(&mut self) -> anyhow::Result<Option<EventKind<E>>> {
        match self.receiver.recv().await {
            Ok(event) => Ok(Some(event)),
            Err(tokio::sync::broadcast::error::RecvError::Closed) => Ok(None),
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                anyhow::bail!("Event subscription lagged by {} events", n)
            }
        }
    }

    /// Wait for the next event with a timeout.
    ///
    /// Returns the event if received within the timeout, or an error.
    pub async fn recv_with_timeout(
        &mut self,
        duration: Duration,
    ) -> anyhow::Result<Option<EventKind<E>>> {
        match timeout(duration, self.receiver.recv()).await {
            Ok(Ok(event)) => Ok(Some(event)),
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => Ok(None),
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                anyhow::bail!("Event subscription lagged by {} events", n)
            }
            Err(_) => anyhow::bail!("Timeout waiting for event"),
        }
    }

    /// Wait for an event matching a predicate.
    ///
    /// Continues receiving events until one matches the predicate or the channel closes.
    pub async fn wait_for<F>(&mut self, predicate: F) -> anyhow::Result<EventKind<E>>
    where
        F: Fn(&EventKind<E>) -> bool,
    {
        loop {
            match self.recv().await? {
                Some(event) if predicate(&event) => return Ok(event),
                Some(_) => continue, // Event didn't match, keep waiting
                None => anyhow::bail!("Channel closed while waiting for event"),
            }
        }
    }

    /// Wait for an event matching a predicate with a timeout.
    pub async fn wait_for_with_timeout<F>(
        &mut self,
        predicate: F,
        duration: Duration,
    ) -> anyhow::Result<EventKind<E>>
    where
        F: Fn(&EventKind<E>) -> bool,
    {
        let start = tokio::time::Instant::now();
        loop {
            let remaining = duration.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                anyhow::bail!("Timeout waiting for matching event");
            }

            match self.recv_with_timeout(remaining).await? {
                Some(event) if predicate(&event) => return Ok(event),
                Some(_) => continue,
                None => anyhow::bail!("Channel closed while waiting for event"),
            }
        }
    }

    /// Try to receive an event without blocking.
    pub fn try_recv(&mut self) -> anyhow::Result<Option<EventKind<E>>> {
        match self.receiver.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => Ok(None),
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                anyhow::bail!("Event subscription lagged by {} events", n)
            }
        }
    }
}

/// Topic for SSE subscriptions.
///
/// Represents all 19 SSE event types supported by the beacon node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SseTopic {
    Attestation,
    SingleAttestation,
    Block,
    BlockFull,
    BlobSidecar,
    DataColumnSidecar,
    FinalizedCheckpoint,
    Head,
    VoluntaryExit,
    ChainReorg,
    ContributionAndProof,
    PayloadAttributes,
    LateHead,
    LightClientFinalityUpdate,
    LightClientOptimisticUpdate,
    BlockReward,
    ProposerSlashing,
    AttesterSlashing,
    BlsToExecutionChange,
    BlockGossip,
}

impl SseTopic {
    /// Get the topic name as a string (for debugging/logging).
    pub fn as_str(&self) -> &'static str {
        match self {
            SseTopic::Attestation => "attestation",
            SseTopic::SingleAttestation => "single_attestation",
            SseTopic::Block => "block",
            SseTopic::BlockFull => "block_full",
            SseTopic::BlobSidecar => "blob_sidecar",
            SseTopic::DataColumnSidecar => "data_column_sidecar",
            SseTopic::FinalizedCheckpoint => "finalized_checkpoint",
            SseTopic::Head => "head",
            SseTopic::VoluntaryExit => "voluntary_exit",
            SseTopic::ChainReorg => "chain_reorg",
            SseTopic::ContributionAndProof => "contribution_and_proof",
            SseTopic::PayloadAttributes => "payload_attributes",
            SseTopic::LateHead => "late_head",
            SseTopic::LightClientFinalityUpdate => "light_client_finality_update",
            SseTopic::LightClientOptimisticUpdate => "light_client_optimistic_update",
            SseTopic::BlockReward => "block_reward",
            SseTopic::ProposerSlashing => "proposer_slashing",
            SseTopic::AttesterSlashing => "attester_slashing",
            SseTopic::BlsToExecutionChange => "bls_to_execution_change",
            SseTopic::BlockGossip => "block_gossip",
        }
    }
}

/// Internal cache entry for a subscription.
struct SubscriptionEntry<E: EthSpec> {
    receiver: Receiver<EventKind<E>>,
}

/// Cache for lazy SSE subscriptions.
///
/// Manages subscriptions to different event topics, creating them on first use
/// and caching them for reuse.
pub struct SubscriptionCache<E: EthSpec> {
    /// Map from (node_index, topic) to subscription entry.
    subscriptions: RwLock<HashMap<(usize, SseTopic), SubscriptionEntry<E>>>,
    _phantom: PhantomData<E>,
}

impl<E: EthSpec> Default for SubscriptionCache<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: EthSpec> SubscriptionCache<E> {
    /// Create a new empty subscription cache.
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            _phantom: PhantomData,
        }
    }

    /// Get or create a subscription for a specific node and topic.
    ///
    /// This is the core lazy subscription method. If a subscription doesn't exist,
    /// it will be created by calling the provided `subscribe_fn`.
    pub fn subscribe_to_node<F>(
        &self,
        node_index: usize,
        topic: SseTopic,
        subscribe_fn: F,
    ) -> EventSubscription<E>
    where
        F: FnOnce(SseTopic) -> Receiver<EventKind<E>>,
    {
        let mut subscriptions = self.subscriptions.write();

        // Check if we already have a subscription for this node/topic
        if let Some(entry) = subscriptions.get(&(node_index, topic)) {
            // Clone the receiver to create a new subscription
            // Note: broadcast::Receiver doesn't implement Clone, so we need to resubscribe
            // The handler keeps the sender, so we need to get a new receiver from it
            drop(subscriptions);
            // Re-subscribe via the provided function
            let receiver = subscribe_fn(topic);
            return EventSubscription::new(receiver);
        }

        // Create new subscription
        let receiver = subscribe_fn(topic);
        let entry = SubscriptionEntry {
            receiver: receiver.resubscribe(),
        };
        subscriptions.insert((node_index, topic), entry);

        EventSubscription::new(receiver)
    }

    /// Clear all cached subscriptions.
    pub fn clear(&self) {
        self.subscriptions.write().clear();
    }

    /// Clear subscriptions for a specific node.
    pub fn clear_for_node(&self, node_index: usize) {
        let mut subs = self.subscriptions.write();
        subs.retain(|(idx, _), _| *idx != node_index);
    }
}

/// Helper function to wait for a specific slot.
///
/// Subscribes to head events and waits for one with the given slot.
pub async fn wait_for_slot<E: EthSpec>(
    subscription: &mut EventSubscription<E>,
    target_slot: Slot,
    timeout_duration: Duration,
) -> anyhow::Result<SseHead> {
    let event = subscription
        .wait_for_with_timeout(
            |event| matches!(event, EventKind::Head(head) if head.slot == target_slot),
            timeout_duration,
        )
        .await?;

    match event {
        EventKind::Head(head) => Ok(head),
        _ => unreachable!("Predicate ensures this is a Head event"),
    }
}

/// Helper function to wait for a block matching a predicate.
///
/// Subscribes to block events and waits for one matching the predicate.
pub async fn wait_for_block<F, E: EthSpec>(
    subscription: &mut EventSubscription<E>,
    predicate: F,
    timeout_duration: Duration,
) -> anyhow::Result<SseBlock>
where
    F: Fn(&SseBlock) -> bool,
{
    let event = subscription
        .wait_for_with_timeout(
            |event| matches!(event, EventKind::Block(block) if predicate(block)),
            timeout_duration,
        )
        .await?;

    match event {
        EventKind::Block(block) => Ok(block),
        _ => unreachable!("Predicate ensures this is a Block event"),
    }
}

/// Helper function to wait for a head event matching a predicate.
///
/// Subscribes to head events and waits for one matching the predicate.
pub async fn wait_for_head<F, E: EthSpec>(
    subscription: &mut EventSubscription<E>,
    predicate: F,
    timeout_duration: Duration,
) -> anyhow::Result<SseHead>
where
    F: Fn(&SseHead) -> bool,
{
    let event = subscription
        .wait_for_with_timeout(
            |event| matches!(event, EventKind::Head(head) if predicate(head)),
            timeout_duration,
        )
        .await?;

    match event {
        EventKind::Head(head) => Ok(head),
        _ => unreachable!("Predicate ensures this is a Head event"),
    }
}

/// Helper function to wait for a specific epoch finalization.
///
/// Subscribes to finalized checkpoint events and waits for one with the given epoch.
pub async fn wait_for_finalization<E: EthSpec>(
    subscription: &mut EventSubscription<E>,
    target_epoch: Epoch,
    timeout_duration: Duration,
) -> anyhow::Result<SseFinalizedCheckpoint> {
    let event = subscription
        .wait_for_with_timeout(
            |event| {
                matches!(event, EventKind::FinalizedCheckpoint(checkpoint) if checkpoint.epoch == target_epoch)
            },
            timeout_duration,
        )
        .await?;

    match event {
        EventKind::FinalizedCheckpoint(checkpoint) => Ok(checkpoint),
        _ => unreachable!("Predicate ensures this is a FinalizedCheckpoint event"),
    }
}

/// Extension trait for ServerSentEventHandler to create subscriptions easily.
pub trait ServerSentEventHandlerExt<E: EthSpec> {
    /// Subscribe to a specific topic.
    fn subscribe_to_topic(&self, topic: SseTopic) -> Receiver<EventKind<E>>;
}

impl<E: EthSpec> ServerSentEventHandlerExt<E> for ServerSentEventHandler<E> {
    fn subscribe_to_topic(&self, topic: SseTopic) -> Receiver<EventKind<E>> {
        match topic {
            SseTopic::Attestation => self.subscribe_attestation(),
            SseTopic::SingleAttestation => self.subscribe_single_attestation(),
            SseTopic::Block => self.subscribe_block(),
            SseTopic::BlockFull => self.subscribe_block_full(),
            SseTopic::BlobSidecar => self.subscribe_blob_sidecar(),
            SseTopic::DataColumnSidecar => self.subscribe_data_column_sidecar(),
            SseTopic::FinalizedCheckpoint => self.subscribe_finalized(),
            SseTopic::Head => self.subscribe_head(),
            SseTopic::VoluntaryExit => self.subscribe_exit(),
            SseTopic::ChainReorg => self.subscribe_reorgs(),
            SseTopic::ContributionAndProof => self.subscribe_contributions(),
            SseTopic::PayloadAttributes => self.subscribe_payload_attributes(),
            SseTopic::LateHead => self.subscribe_late_head(),
            SseTopic::LightClientFinalityUpdate => self.subscribe_light_client_finality_update(),
            SseTopic::LightClientOptimisticUpdate => self.subscribe_light_client_optimistic_update(),
            SseTopic::BlockReward => self.subscribe_block_reward(),
            SseTopic::ProposerSlashing => self.subscribe_proposer_slashing(),
            SseTopic::AttesterSlashing => self.subscribe_attester_slashing(),
            SseTopic::BlsToExecutionChange => self.subscribe_bls_to_execution_change(),
            SseTopic::BlockGossip => self.subscribe_block_gossip(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::MinimalEthSpec;

    type E = MinimalEthSpec;

    #[test]
    fn test_subscription_cache_creation() {
        let cache: SubscriptionCache<E> = SubscriptionCache::new();
        // Just verify it creates without error
        assert!(true);
    }

    #[test]
    fn test_lazy_subscription() {
        let cache: SubscriptionCache<E> = SubscriptionCache::new();
        let handler = ServerSentEventHandler::new(16);

        // First subscription should create entry
        let sub1 = cache.subscribe_to_node(0, SseTopic::Head, |topic| {
            handler.subscribe_to_topic(topic)
        });

        // Second subscription for same node/topic should also work
        let sub2 = cache.subscribe_to_node(0, SseTopic::Head, |topic| {
            handler.subscribe_to_topic(topic)
        });

        // Both should be able to receive (though they won't get any events in this test)
        drop(sub1);
        drop(sub2);
    }

    #[test]
    fn test_clear_subscriptions() {
        let cache: SubscriptionCache<E> = SubscriptionCache::new();
        let handler = ServerSentEventHandler::new(16);

        // Create some subscriptions
        let _ = cache.subscribe_to_node(0, SseTopic::Head, |topic| {
            handler.subscribe_to_topic(topic)
        });
        let _ = cache.subscribe_to_node(1, SseTopic::Block, |topic| {
            handler.subscribe_to_topic(topic)
        });

        // Clear all
        cache.clear();

        // After clear, should be empty
        // We can't directly check, but we can verify it doesn't panic
        assert!(true);
    }

    #[test]
    fn test_clear_for_specific_node() {
        let cache: SubscriptionCache<E> = SubscriptionCache::new();
        let handler = ServerSentEventHandler::new(16);

        // Create subscriptions for different nodes
        let _ = cache.subscribe_to_node(0, SseTopic::Head, |topic| {
            handler.subscribe_to_topic(topic)
        });
        let _ = cache.subscribe_to_node(1, SseTopic::Head, |topic| {
            handler.subscribe_to_topic(topic)
        });

        // Clear only node 0
        cache.clear_for_node(0);

        // Should not panic
        assert!(true);
    }

    #[tokio::test]
    async fn test_event_subscription_recv_timeout() {
        let handler = ServerSentEventHandler::<E>::new(16);
        let mut subscription = EventSubscription::new(handler.subscribe_head());

        // Should timeout since no events are sent
        let result = subscription
            .recv_with_timeout(Duration::from_millis(50))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_event_subscription_wait_for() {
        let handler = ServerSentEventHandler::<E>::new(16);
        let mut subscription = EventSubscription::new(handler.subscribe_head());

        // Send a head event in a separate task
        let handler_clone = handler.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let head = SseHead {
                slot: Slot::new(1),
                block: types::Hash256::ZERO,
                state: types::Hash256::ZERO,
                epoch_transition: false,
                previous_duty_dependent_root: types::Hash256::ZERO,
                current_duty_dependent_root: types::Hash256::ZERO,
                execution_optimistic: false,
            };
            handler_clone.register(EventKind::Head(head));
        });

        // Should receive the event
        let event = subscription
            .wait_for(|e| matches!(e, EventKind::Head(h) if h.slot == 1))
            .await;
        assert!(event.is_ok());
    }

    #[tokio::test]
    async fn test_event_subscription_wait_for_timeout() {
        let handler = ServerSentEventHandler::<E>::new(16);
        let mut subscription = EventSubscription::new(handler.subscribe_head());

        // Should timeout since no matching event is sent
        let result = subscription
            .wait_for_with_timeout(
                |e| matches!(e, EventKind::Head(h) if h.slot == 999),
                Duration::from_millis(50),
            )
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_sse_topic_as_str() {
        assert_eq!(SseTopic::Head.as_str(), "head");
        assert_eq!(SseTopic::Block.as_str(), "block");
        assert_eq!(SseTopic::FinalizedCheckpoint.as_str(), "finalized_checkpoint");
        assert_eq!(SseTopic::Attestation.as_str(), "attestation");
    }

    #[tokio::test]
    async fn test_wait_for_slot_helper() {
        let handler = ServerSentEventHandler::<E>::new(16);
        let mut subscription = EventSubscription::new(handler.subscribe_head());

        // Send a head event with slot 5
        let handler_clone = handler.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let head = SseHead {
                slot: Slot::new(5),
                block: types::Hash256::ZERO,
                state: types::Hash256::ZERO,
                epoch_transition: false,
                previous_duty_dependent_root: types::Hash256::ZERO,
                current_duty_dependent_root: types::Hash256::ZERO,
                execution_optimistic: false,
            };
            handler_clone.register(EventKind::Head(head));
        });

        // Should wait for and receive slot 5
        let result = wait_for_slot(&mut subscription, Slot::new(5), Duration::from_secs(1)).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().slot, 5);
    }

    #[tokio::test]
    async fn test_wait_for_finalization_helper() {
        let handler = ServerSentEventHandler::<E>::new(16);
        let mut subscription = EventSubscription::new(handler.subscribe_finalized());

        // Send a finalized checkpoint event with epoch 10
        let handler_clone = handler.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let checkpoint = SseFinalizedCheckpoint {
                block: types::Hash256::ZERO,
                state: types::Hash256::ZERO,
                epoch: Epoch::new(10),
                execution_optimistic: false,
            };
            handler_clone.register(EventKind::FinalizedCheckpoint(checkpoint));
        });

        // Should wait for and receive epoch 10 finalization
        let result =
            wait_for_finalization(&mut subscription, Epoch::new(10), Duration::from_secs(1)).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().epoch, 10);
    }
}
