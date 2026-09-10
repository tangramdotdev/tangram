use {
	super::{Request, RequestState, State},
	std::sync::atomic::{AtomicUsize, Ordering},
	tangram_client::prelude::*,
};

fn insert(
	state: &mut State,
	id: &str,
	request_state: RequestState,
) -> tokio::sync::oneshot::Receiver<tg::Result<()>> {
	let (sender, receiver) = tokio::sync::oneshot::channel();
	let request = Request {
		id: id.into(),
		sender,
		state: request_state,
	};
	state.waits.insert(id.into(), request);
	receiver
}

#[tokio::test]
async fn database_waits_advance_to_compactions() {
	for pending in [false, true] {
		let mut state = State::new();
		let mut receiver = insert(&mut state, "client", RequestState::DatabaseIndexOutbox);
		let batch = crate::database::index::outbox::BatchId::new(10);
		state
			.poll_database_index_outbox(true, |_| async { Ok(pending.then_some(batch)) })
			.await
			.unwrap();
		if pending {
			assert!(matches!(
				receiver.try_recv(),
				Err(tokio::sync::oneshot::error::TryRecvError::Empty)
			));
			state
				.poll_database_index_outbox(true, |target| {
					assert_eq!(target, Some(batch));
					async { Ok(None) }
				})
				.await
				.unwrap();
		}
		assert!(matches!(
			receiver.try_recv(),
			Err(tokio::sync::oneshot::error::TryRecvError::Empty)
		));
		assert!(matches!(
			state.waits["client"].state,
			RequestState::LogCompactions {
				transaction_id: None
			}
		));
	}
}

#[tokio::test]
async fn compactions_advance_to_a_fresh_update_barrier() {
	let mut state = State::new();
	let mut receiver = insert(
		&mut state,
		"compaction",
		RequestState::LogCompactions {
			transaction_id: None,
		},
	);
	state
		.set_log_compaction_target_transactions(async { Ok(10) })
		.await
		.unwrap();
	state
		.poll_log_compactions(async { Ok(Some(10)) })
		.await
		.unwrap();
	assert!(matches!(
		receiver.try_recv(),
		Err(tokio::sync::oneshot::error::TryRecvError::Empty)
	));
	state
		.poll_log_compactions(async { Ok(None) })
		.await
		.unwrap();

	// The final compaction batch can commit updates after the compaction barrier.
	state
		.set_update_target_transactions(async { Ok(20) })
		.await
		.unwrap();
	state
		.poll_updates(|_| async { Ok(Some(15)) })
		.await
		.unwrap();
	assert!(matches!(
		receiver.try_recv(),
		Err(tokio::sync::oneshot::error::TryRecvError::Empty)
	));
	state.poll_updates(|_| async { Ok(None) }).await.unwrap();
	receiver.await.unwrap().unwrap();
}

#[tokio::test]
async fn database_reads_are_shared_without_advancing_later_requests() {
	let mut state = State::new();
	let _receivers = (0..1024)
		.map(|id| {
			insert(
				&mut state,
				&id.to_string(),
				RequestState::DatabaseIndexOutbox,
			)
		})
		.collect::<Vec<_>>();
	let reads = AtomicUsize::new(0);
	let batch = crate::database::index::outbox::BatchId::new(10);
	state
		.poll_database_index_outbox(false, |target| {
			assert!(target.is_none());
			reads.fetch_add(1, Ordering::Relaxed);
			async { Ok(Some(batch)) }
		})
		.await
		.unwrap();
	assert_eq!(reads.load(Ordering::Relaxed), 1);
	let _later = insert(&mut state, "later", RequestState::DatabaseIndexOutbox);
	state
		.poll_database_index_outbox(false, |target| {
			assert_eq!(target, Some(batch));
			reads.fetch_add(1, Ordering::Relaxed);
			async { Ok(None) }
		})
		.await
		.unwrap();
	assert_eq!(reads.load(Ordering::Relaxed), 2);
	assert!(matches!(
		state.waits["0"].state,
		RequestState::Updates {
			transaction_id: None
		}
	));
	assert!(matches!(
		state.waits["later"].state,
		RequestState::DatabaseIndexOutbox
	));
	state
		.poll_database_index_outbox(false, |target| {
			assert!(target.is_none());
			reads.fetch_add(1, Ordering::Relaxed);
			async { Ok(None) }
		})
		.await
		.unwrap();
	assert_eq!(reads.load(Ordering::Relaxed), 3);
	assert!(matches!(
		state.waits["later"].state,
		RequestState::Updates {
			transaction_id: None
		}
	));
}

#[tokio::test]
async fn concurrent_waits_share_update_reads() {
	let mut state = State::new();
	let receivers = (0..1024)
		.map(|id| {
			insert(
				&mut state,
				&id.to_string(),
				RequestState::Updates {
					transaction_id: None,
				},
			)
		})
		.collect::<Vec<_>>();
	let reads = AtomicUsize::new(0);
	state
		.set_update_target_transactions(async {
			reads.fetch_add(1, Ordering::Relaxed);
			Ok(10)
		})
		.await
		.unwrap();
	assert_eq!(reads.load(Ordering::Relaxed), 1);
	state
		.poll_updates(|_| async {
			reads.fetch_add(1, Ordering::Relaxed);
			Ok(Some(10))
		})
		.await
		.unwrap();
	assert_eq!(reads.load(Ordering::Relaxed), 4);
	assert_eq!(state.waits.len(), 1024);
	state
		.set_update_target_transactions(async { panic!("existing targets must not be read again") })
		.await
		.unwrap();
	state
		.poll_updates(|_| async {
			reads.fetch_add(1, Ordering::Relaxed);
			Ok(Some(11))
		})
		.await
		.unwrap();
	assert_eq!(reads.load(Ordering::Relaxed), 7);
	assert!(state.waits.is_empty());
	for receiver in receivers {
		receiver.await.unwrap().unwrap();
	}
	state
		.poll_updates(|_| async { panic!("idle waits must not read progress") })
		.await
		.unwrap();
}

#[tokio::test]
async fn later_requests_keep_their_own_update_targets() {
	let mut state = State::new();
	let first = insert(
		&mut state,
		"first",
		RequestState::Updates {
			transaction_id: None,
		},
	);
	state
		.set_update_target_transactions(async { Ok(10) })
		.await
		.unwrap();
	let mut second = insert(
		&mut state,
		"second",
		RequestState::Updates {
			transaction_id: None,
		},
	);
	state
		.set_update_target_transactions(async { Ok(20) })
		.await
		.unwrap();
	state
		.poll_updates(|_| async { Ok(Some(11)) })
		.await
		.unwrap();
	first.await.unwrap().unwrap();
	assert!(matches!(
		second.try_recv(),
		Err(tokio::sync::oneshot::error::TryRecvError::Empty)
	));
	assert!(matches!(
		state.waits["second"].state,
		RequestState::Updates {
			transaction_id: Some(20)
		}
	));
	state.poll_updates(|_| async { Ok(None) }).await.unwrap();
	second.await.unwrap().unwrap();
}

#[tokio::test]
async fn concurrent_waits_share_log_compaction_reads() {
	let mut state = State::new();
	let _receivers = (0..1024)
		.map(|id| {
			insert(
				&mut state,
				&id.to_string(),
				RequestState::LogCompactions {
					transaction_id: None,
				},
			)
		})
		.collect::<Vec<_>>();
	let reads = AtomicUsize::new(0);
	state
		.set_log_compaction_target_transactions(async {
			reads.fetch_add(1, Ordering::Relaxed);
			Ok(10)
		})
		.await
		.unwrap();
	state
		.poll_log_compactions(async {
			reads.fetch_add(1, Ordering::Relaxed);
			Ok(Some(10))
		})
		.await
		.unwrap();
	assert_eq!(reads.load(Ordering::Relaxed), 2);
	assert!(state.waits.values().all(|request| matches!(
		request.state,
		RequestState::LogCompactions {
			transaction_id: Some(10)
		}
	)));
	state
		.poll_log_compactions(async { Ok(None) })
		.await
		.unwrap();
	assert!(state.waits.values().all(|request| matches!(
		request.state,
		RequestState::Updates {
			transaction_id: None
		}
	)));
}

#[test]
fn later_requests_keep_their_own_queue_targets() {
	let mut state = State::new();
	let _first = insert(&mut state, "first", RequestState::IndexQueue);
	state.poll_index_queue(false, 0, 10);
	let _second = insert(&mut state, "second", RequestState::IndexQueue);
	state.poll_index_queue(false, 10, 20);
	assert!(matches!(
		state.waits["first"].state,
		RequestState::DatabaseIndexOutbox
	));
	assert!(matches!(
		state.waits["second"].state,
		RequestState::IndexQueuePending { sequence: 20 }
	));
	state.poll_index_queue(false, 20, 20);
	assert!(matches!(
		state.waits["second"].state,
		RequestState::DatabaseIndexOutbox
	));
}

#[test]
fn later_requests_do_not_join_an_existing_task_wait() {
	let mut state = State::new();
	let _first = insert(&mut state, "first", RequestState::Tasks);
	let ids = vec!["first".into()];
	let _second = insert(&mut state, "second", RequestState::Tasks);
	state.handle_task_wait(ids);
	assert!(matches!(
		state.waits["first"].state,
		RequestState::IndexQueue
	));
	assert!(matches!(state.waits["second"].state, RequestState::Tasks));
}

#[tokio::test]
async fn read_errors_complete_all_waits_and_allow_new_requests() {
	let mut state = State::new();
	let receivers = (0..16)
		.map(|id| {
			insert(
				&mut state,
				&id.to_string(),
				RequestState::Updates {
					transaction_id: Some(10),
				},
			)
		})
		.collect::<Vec<_>>();
	let error = state
		.poll_updates(|_| async { Err(tg::error!("test read failure")) })
		.await
		.unwrap_err();
	state.fail(&error);
	assert!(state.waits.is_empty());
	for receiver in receivers {
		assert!(receiver.await.unwrap().is_err());
	}
	let receiver = insert(
		&mut state,
		"next",
		RequestState::Updates {
			transaction_id: Some(10),
		},
	);
	state.poll_updates(|_| async { Ok(None) }).await.unwrap();
	receiver.await.unwrap().unwrap();
}
