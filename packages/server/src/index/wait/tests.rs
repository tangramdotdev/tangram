use {
	super::{Progress, Request, RequestState, State},
	futures::StreamExt as _,
	std::sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
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

#[tokio::test]
async fn inputs_run_concurrently_and_share_reads() {
	let mut state = State::new();
	let _receivers = (0..1024)
		.map(|id| insert(&mut state, &id.to_string(), RequestState::new()))
		.collect::<Vec<_>>();
	let barrier = Arc::new(tokio::sync::Barrier::new(2));
	let calls = Arc::new(AtomicUsize::new(0));
	let calls_ = calls.clone();
	let barrier_ = barrier.clone();
	state.start_indexer_wait(async move {
		calls_.fetch_add(1, Ordering::Relaxed);
		barrier_.wait().await;
		Ok(())
	});
	let reads = AtomicUsize::new(0);
	let batch = crate::database::index::queue::BatchId::new(10);
	let poll = state.poll_inputs(|target| {
		assert!(target.is_none());
		async {
			reads.fetch_add(1, Ordering::Relaxed);
			barrier.wait().await;
			Ok(Some(batch))
		}
	});
	tokio::time::timeout(std::time::Duration::from_secs(5), poll)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(calls.load(Ordering::Relaxed), 1);
	assert_eq!(reads.load(Ordering::Relaxed), 1);
	let (ids, result) = state.indexer_waits.next().await.unwrap();
	state.handle_indexer_wait(ids, &result);
	state
		.poll_inputs(|target| {
			assert_eq!(target, Some(batch));
			async {
				reads.fetch_add(1, Ordering::Relaxed);
				Ok(None)
			}
		})
		.await
		.unwrap();
	assert_eq!(reads.load(Ordering::Relaxed), 2);
	state
		.set_update_target_transactions(async { Ok(20) })
		.await
		.unwrap();
	state.poll_updates(|_| async { Ok(None) }).await.unwrap();
	assert!(state.waits.is_empty());
	state
		.poll_inputs(|_| async { panic!("idle waits must not read the queue") })
		.await
		.unwrap();
}

#[tokio::test]
async fn updates_wait_for_both_inputs_in_any_completion_order() {
	for order in [[0, 1], [1, 0]] {
		let mut state = State::new();
		let mut receiver = insert(&mut state, "client", RequestState::new());
		state.start_indexer_wait(async { Ok(()) });
		let (ids, result) = state.indexer_waits.next().await.unwrap();
		let batch = crate::database::index::queue::BatchId::new(10);
		state
			.poll_inputs(|_| async { Ok(Some(batch)) })
			.await
			.unwrap();
		let mut complete = [false; 2];
		for input in order {
			complete[input] = true;
			if input == 0 {
				state.handle_indexer_wait(ids.clone(), &result);
			}
			state
				.poll_inputs(|_| async { Ok((!complete[1]).then_some(batch)) })
				.await
				.unwrap();
			state
				.set_update_target_transactions(async { Ok(20) })
				.await
				.unwrap();
			state.poll_updates(|_| async { Ok(None) }).await.unwrap();
			if complete.iter().all(|complete| *complete) {
				receiver.try_recv().unwrap().unwrap();
			} else {
				assert!(matches!(
					receiver.try_recv(),
					Err(tokio::sync::oneshot::error::TryRecvError::Empty)
				));
			}
		}
	}
}

#[tokio::test]
async fn later_requests_keep_their_own_input_targets() {
	let mut state = State::new();
	let _first = insert(&mut state, "first", RequestState::new());
	state.start_indexer_wait(async { Ok(()) });
	let (ids, result) = state.indexer_waits.next().await.unwrap();
	state.handle_indexer_wait(ids, &result);
	let first = crate::database::index::queue::BatchId::new(10);
	let second = crate::database::index::queue::BatchId::new(20);
	state
		.poll_inputs(|_| async { Ok(Some(first)) })
		.await
		.unwrap();
	let _second = insert(&mut state, "second", RequestState::new());
	state
		.poll_inputs(|target| {
			assert_eq!(target, Some(first));
			async { Ok(None) }
		})
		.await
		.unwrap();
	assert!(matches!(
		state.waits["first"].state,
		RequestState::Inputs {
			database_index_queue: Progress::Complete,
			indexers: Progress::Complete
		}
	));
	assert!(matches!(
		state.waits["second"].state,
		RequestState::Inputs {
			database_index_queue: Progress::Ready,
			indexers: Progress::Ready
		}
	));
	state.start_indexer_wait(async { Ok(()) });
	let (ids, result) = state.indexer_waits.next().await.unwrap();
	assert_eq!(ids, ["second"]);
	state.handle_indexer_wait(ids, &result);
	state
		.poll_inputs(|target| {
			assert!(target.is_none());
			async { Ok(Some(second)) }
		})
		.await
		.unwrap();
	assert_eq!(state.database_index_queue_batch_id, Some(second));
}

#[tokio::test]
async fn canceled_waits_do_not_poll_or_reuse_old_queue_targets() {
	let mut state = State::new();
	let receiver = insert(&mut state, "first", RequestState::new());
	state.start_indexer_wait(std::future::pending());
	state
		.poll_inputs(|_| async { Ok(Some(crate::database::index::queue::BatchId::new(10))) })
		.await
		.unwrap();
	drop(receiver);
	state.remove_closed();
	assert!(state.indexer_waits.is_empty());
	state
		.poll_inputs(|_| async { panic!("idle waits must not read the queue") })
		.await
		.unwrap();
	let receiver = insert(&mut state, "later", RequestState::new());
	state.start_indexer_wait(async { Ok(()) });
	let (ids, result) = state.indexer_waits.next().await.unwrap();
	assert_eq!(ids, ["later"]);
	state.handle_indexer_wait(ids, &result);
	state
		.poll_inputs(|target| {
			assert!(target.is_none());
			async { Ok(None) }
		})
		.await
		.unwrap();
	state
		.set_update_target_transactions(async { Ok(20) })
		.await
		.unwrap();
	state.poll_updates(|_| async { Ok(None) }).await.unwrap();
	receiver.await.unwrap().unwrap();
}

#[tokio::test]
async fn an_indexer_error_only_fails_its_batch() {
	let mut state = State::new();
	let receiver = insert(&mut state, "first", RequestState::new());
	state.start_indexer_wait(async { Err(tg::error!("test failure")) });
	let _later = insert(&mut state, "later", RequestState::new());
	let (ids, result) = state.indexer_waits.next().await.unwrap();
	state.handle_indexer_wait(ids, &result);
	assert!(receiver.await.unwrap().is_err());
	assert!(matches!(
		state.waits["later"].state,
		RequestState::Inputs {
			indexers: Progress::Ready,
			..
		}
	));
}
