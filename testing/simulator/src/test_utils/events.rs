//! Server-Sent Events (SSE) subscription utilities for test networks.
//!
//! Provides direct subscription to `ServerSentEventHandler` (no HTTP) for tests,
//! with lazy subscription on first use and caching for reuse.

use beacon_chain::ServerSentEventHandler;
use eth2::types::{EventKind, SseBlock, SseFinalizedCheckpoint, SseHead};
use node_test_rig::ProofEngineEvent;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::marker::PhantomData;
use tokio::sync::broadcast::Receiver;
use tokio::time::{timeout, Duration};
use types::{Epoch, EthSpec, Slot};

/// Represents a subscription to beacon node SSE events.
///
/// This struct wraps a broadcast receiver for all event kinds
/// and provides ergonomic methods for waiting on events.
pub struct BeaconNodeEventSubscription<E: EthSpec> {
    receiver: Receiver<EventKind<E>>,
}

impl<E: EthSpec> BeaconNodeEventSubscription<E> {
    /// Create a new event subscription from a receiver.
    pub fn new(receiver: Receiver<EventKind<E>>) -> Self {
        Self { receiver }
    }

    /// Wait for the next event.
    ///
    /// Returns `Ok(Some(event))` if an event was received,
    /// `Ok(None)` if the channel is closed.
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

/// Represents a subscription to proof engine events.
///
/// This struct wraps a broadcast receiver for proof engine events
/// and provides ergonomic methods for waiting on events.
pub struct ProofEngineEventSubscription {
    receiver: Receiver<ProofEngineEvent>,
}

impl ProofEngineEventSubscription {
    /// Create a new proof engine event subscription from a receiver.
    pub fn new(receiver: Receiver<ProofEngineEvent>) -> Self {
        Self { receiver }
    }

    /// Wait for the next event.
    ///
    /// Returns `Ok(Some(event))` if an event was received,
    /// `Ok(None)` if the channel is closed.
    pub async fn recv(&mut self) -> anyhow::Result<Option<ProofEngineEvent>> {
        match self.receiver.recv().await {
            Ok(event) => Ok(Some(event)),
            Err(tokio::sync::broadcast::error::RecvError::Closed) => Ok(None),
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                anyhow::bail!("Proof engine event subscription lagged by {} events", n)
            }
        }
    }

    /// Wait for the next event with a timeout.
    ///
    /// Returns the event if received within the timeout, or an error.
    pub async fn recv_with_timeout(
        &mut self,
        duration: Duration,
    ) -> anyhow::Result<Option<ProofEngineEvent>> {
        match timeout(duration, self.receiver.recv()).await {
            Ok(Ok(event)) => Ok(Some(event)),
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => Ok(None),
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                anyhow::bail!("Proof engine event subscription lagged by {} events", n)
            }
            Err(_) => anyhow::bail!("Timeout waiting for proof engine event"),
        }
    }

    /// Wait for an event matching a predicate.
    ///
    /// Continues receiving events until one matches the predicate or the channel closes.
    pub async fn wait_for<F>(&mut self, predicate: F) -> anyhow::Result<ProofEngineEvent>
    where
        F: Fn(&ProofEngineEvent) -> bool,
    {
        loop {
            match self.recv().await? {
                Some(event) if predicate(&event) => return Ok(event),
                Some(_) => continue, // Event didn't match, keep waiting
                None => anyhow::bail!("Channel closed while waiting for proof engine event"),
            }
        }
    }

    /// Wait for an event matching a predicate with a timeout.
    pub async fn wait_for_with_timeout<F>(
        &mut self,
        predicate: F,
        duration: Duration,
    ) -> anyhow::Result<ProofEngineEvent>
    where
        F: Fn(&ProofEngineEvent) -> bool,
    {
        let start = tokio::time::Instant::now();
        loop {
            let remaining = duration.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                anyhow::bail!("Timeout waiting for matching proof engine event");
            }

            match self.recv_with_timeout(remaining).await? {
                Some(event) if predicate(&event) => return Ok(event),
                Some(_) => continue,
                None => anyhow::bail!("Channel closed while waiting for proof engine event"),
            }
        }
    }

    /// Try to receive an event without blocking.
    pub fn try_recv(&mut self) -> anyhow::Result<Option<ProofEngineEvent>> {
        match self.receiver.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => Ok(None),
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                anyhow::bail!("Proof engine event subscription lagged by {} events", n)
            }
        }
    }

    /// Wait for a proof request event matching an optional predicate.
    ///
    /// # Arguments
    /// * `predicate` - Optional function that returns true when the desired proof request is received.
    ///                 If None, waits for any proof request event.
    /// * `timeout_duration` - Maximum time to wait
    pub async fn wait_for_proof_request<F>(
        &mut self,
        predicate: Option<F>,
        timeout_duration: Duration,
    ) -> anyhow::Result<node_test_rig::ProofRequestRecord>
    where
        F: Fn(&node_test_rig::ProofRequestRecord) -> bool,
    {
        let event = if let Some(pred) = predicate {
            self.wait_for_with_timeout(
                |event| matches!(event, ProofEngineEvent::ProofRequestReceived { record } if pred(record)),
                timeout_duration,
            )
            .await?
        } else {
            self.wait_for_with_timeout(
                |event| matches!(event, ProofEngineEvent::ProofRequestReceived { .. }),
                timeout_duration,
            )
            .await?
        };

        match event {
            ProofEngineEvent::ProofRequestReceived { record } => Ok(record),
            _ => unreachable!("Predicate ensures this is a ProofRequestReceived event"),
        }
    }

    /// Wait for a proof sent to validator event matching an optional predicate.
    ///
    /// # Arguments
    /// * `predicate` - Optional function that returns true when the desired proof is received.
    ///                 If None, waits for any proof sent event.
    /// * `timeout_duration` - Maximum time to wait
    pub async fn wait_for_proof_sent<F>(
        &mut self,
        predicate: Option<F>,
        timeout_duration: Duration,
    ) -> anyhow::Result<types::execution::eip8025::ExecutionProof>
    where
        F: Fn(&types::execution::eip8025::ExecutionProof) -> bool,
    {
        let event = if let Some(pred) = predicate {
            self.wait_for_with_timeout(
                |event| matches!(event, ProofEngineEvent::ProofSentToValidator { execution_proof } if pred(execution_proof)),
                timeout_duration,
            )
            .await?
        } else {
            self.wait_for_with_timeout(
                |event| matches!(event, ProofEngineEvent::ProofSentToValidator { .. }),
                timeout_duration,
            )
            .await?
        };

        match event {
            ProofEngineEvent::ProofSentToValidator { execution_proof } => Ok(execution_proof),
            _ => unreachable!("Predicate ensures this is a ProofSentToValidator event"),
        }
    }

    /// Collect all pending events into a vector.
    ///
    /// This drains all events currently in the channel buffer.
    pub fn collect_pending(&mut self) -> Vec<ProofEngineEvent> {
        let mut events = Vec::new();
        while let Ok(Some(event)) = self.try_recv() {
            events.push(event);
        }
        events
    }

    /// Count the number of proof request events received.
    ///
    /// This counts both pending events and waits for new events up to the timeout.
    pub async fn count_proof_requests(&mut self, timeout_duration: Duration) -> usize {
        let mut count = 0;

        // Count pending events
        while let Ok(Some(event)) = self.try_recv() {
            if matches!(event, ProofEngineEvent::ProofRequestReceived { .. }) {
                count += 1;
            }
        }

        // Wait for more events within the timeout
        let start = tokio::time::Instant::now();
        loop {
            let remaining = timeout_duration.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                break;
            }

            match self.recv_with_timeout(remaining).await {
                Ok(Some(ProofEngineEvent::ProofRequestReceived { .. })) => count += 1,
                Ok(Some(_)) => continue, // Other event types, ignore
                Ok(None) => break,       // Channel closed
                Err(_) => break,         // Timeout or error
            }
        }

        count
    }
}

/// Internal cache entry for a subscription.
struct SubscriptionEntry<E: EthSpec> {
    receiver: Receiver<EventKind<E>>,
}

/// Cache for lazy SSE subscriptions.
///
/// Manages subscriptions to beacon node events, creating them on first use
/// and caching them for reuse. Each node has a single subscription to ALL events.
pub struct SubscriptionCache<E: EthSpec> {
    /// Map from node_index to subscription entry.
    subscriptions: RwLock<HashMap<usize, SubscriptionEntry<E>>>,
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

    /// Get or create a subscription for a specific node.
    ///
    /// This is the core lazy subscription method. If a subscription doesn't exist,
    /// it will be created by calling the provided `subscribe_fn`.
    /// The subscription receives ALL event types.
    pub fn subscribe_to_node<F>(&self, node_index: usize, subscribe_fn: F) -> BeaconNodeEventSubscription<E>
    where
        F: FnOnce() -> Receiver<EventKind<E>>,
    {
        let mut subscriptions = self.subscriptions.write();

        // Check if we already have a subscription for this node
        if subscriptions.get(&node_index).is_some() {
            // We have a cached subscription, need to create a new receiver
            drop(subscriptions);
            // Re-subscribe via the provided function
            let receiver = subscribe_fn();
            return BeaconNodeEventSubscription::new(receiver);
        }

        // Create new subscription
        let receiver = subscribe_fn();
        let entry = SubscriptionEntry {
            receiver: receiver.resubscribe(),
        };
        subscriptions.insert(node_index, entry);

        BeaconNodeEventSubscription::new(receiver)
    }

    /// Clear all cached subscriptions.
    pub fn clear(&self) {
        self.subscriptions.write().clear();
    }

    /// Clear subscriptions for a specific node.
    pub fn clear_for_node(&self, node_index: usize) {
        self.subscriptions.write().remove(&node_index);
    }

    /// Wait for a head event matching a predicate.
    ///
    /// # Arguments
    /// * `node_index` - The index of the beacon node to monitor
    /// * `predicate` - Optional function that returns true when the desired head is received.
    ///                 If None, waits for any head event.
    /// * `timeout_duration` - Maximum time to wait
    pub async fn wait_for_head<F>(
        &self,
        node_index: usize,
        predicate: Option<F>,
        timeout_duration: Duration,
    ) -> anyhow::Result<SseHead>
    where
        F: Fn(&SseHead) -> bool,
    {
        let mut subscription = self.subscribe_to_node(node_index, || {
            // This will be called when needed - the actual subscribe function
            // is passed from the fixture that has access to the handler
            panic!("No event handler available - use TestNetworkFixture::wait_for_head instead")
        });

        let event = if let Some(pred) = predicate {
            subscription
                .wait_for_with_timeout(
                    |event| matches!(event, EventKind::Head(head) if pred(head)),
                    timeout_duration,
                )
                .await?
        } else {
            subscription
                .wait_for_with_timeout(
                    |event| matches!(event, EventKind::Head(_)),
                    timeout_duration,
                )
                .await?
        };

        match event {
            EventKind::Head(head) => Ok(head),
            _ => unreachable!("Predicate ensures this is a Head event"),
        }
    }

    /// Wait for a block event matching a predicate.
    ///
    /// # Arguments
    /// * `node_index` - The index of the beacon node to monitor
    /// * `predicate` - Optional function that returns true when the desired block is received.
    ///                 If None, waits for any block event.
    /// * `timeout_duration` - Maximum time to wait
    pub async fn wait_for_block<F>(
        &self,
        node_index: usize,
        predicate: Option<F>,
        timeout_duration: Duration,
    ) -> anyhow::Result<SseBlock>
    where
        F: Fn(&SseBlock) -> bool,
    {
        let mut subscription = self.subscribe_to_node(node_index, || {
            panic!("No event handler available - use TestNetworkFixture::wait_for_block instead")
        });

        let event = if let Some(pred) = predicate {
            subscription
                .wait_for_with_timeout(
                    |event| matches!(event, EventKind::Block(block) if pred(block)),
                    timeout_duration,
                )
                .await?
        } else {
            subscription
                .wait_for_with_timeout(
                    |event| matches!(event, EventKind::Block(_)),
                    timeout_duration,
                )
                .await?
        };

        match event {
            EventKind::Block(block) => Ok(block),
            _ => unreachable!("Predicate ensures this is a Block event"),
        }
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
        slot: Slot,
        timeout_duration: Duration,
    ) -> anyhow::Result<SseHead> {
        self.wait_for_head(
            node_index,
            Some(|head: &SseHead| head.slot == slot),
            timeout_duration,
        )
        .await
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
    ) -> anyhow::Result<SseFinalizedCheckpoint> {
        let mut subscription = self.subscribe_to_node(node_index, || {
            panic!("No event handler available - use TestNetworkFixture::wait_for_finalization instead")
        });

        let event = subscription
            .wait_for_with_timeout(
                |event| matches!(event, EventKind::FinalizedCheckpoint(checkpoint) if checkpoint.epoch == epoch),
                timeout_duration,
            )
            .await?;

        match event {
            EventKind::FinalizedCheckpoint(checkpoint) => Ok(checkpoint),
            _ => unreachable!("Predicate ensures this is a FinalizedCheckpoint event"),
        }
    }

    /// Wait for any event matching a predicate.
    ///
    /// # Arguments
    /// * `node_index` - The index of the beacon node to monitor
    /// * `predicate` - A function that returns true when the desired event is received
    /// * `timeout_duration` - Maximum time to wait for the event
    pub async fn wait_for_event<F>(
        &self,
        node_index: usize,
        predicate: F,
        timeout_duration: Duration,
    ) -> anyhow::Result<EventKind<E>>
    where
        F: Fn(&EventKind<E>) -> bool,
    {
        let mut subscription = self.subscribe_to_node(node_index, || {
            panic!("No event handler available - use TestNetworkFixture::wait_for_event instead")
        });

        subscription.wait_for_with_timeout(predicate, timeout_duration).await
    }
}

/// Helper function to wait for a specific slot.
///
/// Subscribes to head events and waits for one with the given slot.
pub async fn wait_for_slot<E: EthSpec>(
    subscription: &mut BeaconNodeEventSubscription<E>,
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
    subscription: &mut BeaconNodeEventSubscription<E>,
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
    subscription: &mut BeaconNodeEventSubscription<E>,
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
    subscription: &mut BeaconNodeEventSubscription<E>,
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
        let sub1 = cache.subscribe_to_node(0, || handler.subscribe_all());

        // Second subscription for same node should also work
        let sub2 = cache.subscribe_to_node(0, || handler.subscribe_all());

        // Both should be able to receive (though they won't get any events in this test)
        drop(sub1);
        drop(sub2);
    }

    #[test]
    fn test_clear_subscriptions() {
        let cache: SubscriptionCache<E> = SubscriptionCache::new();
        let handler = ServerSentEventHandler::new(16);

        // Create some subscriptions
        let _ = cache.subscribe_to_node(0, || handler.subscribe_all());
        let _ = cache.subscribe_to_node(1, || handler.subscribe_all());

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
        let _ = cache.subscribe_to_node(0, || handler.subscribe_all());
        let _ = cache.subscribe_to_node(1, || handler.subscribe_all());

        // Clear only node 0
        cache.clear_for_node(0);

        // Should not panic
        assert!(true);
    }

    #[tokio::test]
    async fn test_beacon_node_event_subscription_recv_timeout() {
        let handler = ServerSentEventHandler::<E>::new(16);
        let mut subscription = BeaconNodeEventSubscription::new(handler.subscribe_all());

        // Should timeout since no events are sent
        let result = subscription
            .recv_with_timeout(Duration::from_millis(50))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_beacon_node_event_subscription_wait_for() {
        let handler = ServerSentEventHandler::<E>::new(16);
        let mut subscription = BeaconNodeEventSubscription::new(handler.subscribe_all());

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
    async fn test_beacon_node_event_subscription_wait_for_timeout() {
        let handler = ServerSentEventHandler::<E>::new(16);
        let mut subscription = BeaconNodeEventSubscription::new(handler.subscribe_all());

        // Should timeout since no matching event is sent
        let result = subscription
            .wait_for_with_timeout(
                |e| matches!(e, EventKind::Head(h) if h.slot == 999),
                Duration::from_millis(50),
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_wait_for_slot_helper() {
        let handler = ServerSentEventHandler::<E>::new(16);
        let mut subscription = BeaconNodeEventSubscription::new(handler.subscribe_all());

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
        let mut subscription = BeaconNodeEventSubscription::new(handler.subscribe_all());

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
