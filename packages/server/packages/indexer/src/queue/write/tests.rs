use {
	crate::{Indexer, IndexerState, queue},
	std::sync::{Arc, Mutex},
};

#[tokio::test]
async fn concurrent_allocators_wait_for_a_durable_reservation() {
	let state = IndexerState {
		available: true,
		queues: queue::Queues::empty(usize::MAX),
		writes: 0,
	};
	let state = Arc::new(Mutex::new(state));
	let changed = Arc::new(tokio::sync::Notify::new());
	let mut requests = tokio::task::JoinSet::new();
	for _ in 0..32 {
		let changed = changed.clone();
		let state = state.clone();
		requests.spawn(async move {
			Indexer::allocate_sequence(&state, &changed, queue::Kind::Index).await
		});
	}
	tokio::task::yield_now().await;
	assert!(requests.try_join_next().is_none());
	let reservation = queue::SequenceReservation {
		end: 32,
		kind: queue::Kind::Index,
	};
	state.lock().unwrap().queues.finish_reservation(reservation);
	changed.notify_waiters();
	let mut sequences = std::collections::BTreeSet::new();
	while let Some(result) = requests.join_next().await {
		assert!(sequences.insert(result.unwrap()));
	}
	assert_eq!(sequences, (0..32).collect());
}
