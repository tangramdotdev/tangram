use {
	super::{Ack, ClientMessage, Indexer, Request, RequestArg, ServerMessage, queue},
	crate::indexer::State,
	futures::StreamExt as _,
	std::sync::{Arc, Mutex},
};

#[test]
fn subjects_address_an_idless_or_named_indexer() {
	assert_eq!(Indexer::server_subject(None), "indexers.server");
	let id = tangram_client::indexer::Id::new();
	assert_eq!(
		Indexer::server_subject(Some(&id)),
		format!("indexers.{id}.server")
	);
}

#[tokio::test]
async fn draining_waits_for_writes_queue_completions_and_handlers() {
	let request = Request {
		arg: super::RequestArg::Index(super::IndexRequestArg {
			batch: crate::store::index::queue::batch::Id::new([0; 16]),
			fragment: 0,
			fragments: 2,
			payload: bytes::Bytes::new(),
		}),
		id: "fragment".into(),
	};
	let mut queues = queue::Queues::empty();
	let reservation = queue::SequenceReservation {
		end: 1,
		kind: queue::Kind::Index,
	};
	queues.finish_reservation(reservation);
	let sequence = queues.try_allocate_sequence(queue::Kind::Index).unwrap();
	let fragment = crate::store::index::queue::Fragment {
		batch: crate::store::index::queue::batch::Id::new([0; 16]),
		fragment: 0,
		fragments: 2,
		indexer: tangram_client::indexer::Id::new(),
		payload: bytes::Bytes::new(),
		sequence,
	};
	let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
	queues.insert_index_fragment(
		fragment,
		Some(response_sender),
		std::time::Duration::from_secs(60),
	);
	let mut state = State {
		available: true,
		limits: super::limits::Limits::default(),
		queues,
		writes: 1,
	};
	assert!(
		state
			.limits
			.try_insert(&request, &crate::config::IndexerRequest::default())
			.unwrap()
	);
	let state = Arc::new(Mutex::new(state));
	let changed = Arc::new(tokio::sync::Notify::new());
	let mut guard = super::Guard {
		changed: changed.clone(),
		id: request.id,
		state: state.clone(),
		writing: true,
	};
	let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
	sender
		.send(queue::IndexMessage::Delete(vec![99]))
		.await
		.unwrap();
	let drain = Indexer::drain_queues(&state, &changed, &sender);
	tokio::pin!(drain);
	assert!(futures::poll!(&mut drain).is_pending());
	assert!(!state.lock().unwrap().available);
	guard.finish_write();
	assert!(futures::poll!(&mut drain).is_pending());
	assert!(state.try_lock().is_ok());
	receiver.recv().await.unwrap();
	assert!(futures::poll!(&mut drain).is_pending());
	assert!(
		matches!(receiver.recv().await.unwrap(), queue::IndexMessage::Delete(sequences) if sequences == vec![sequence])
	);
	assert!(response_receiver.await.unwrap().is_err());
	state
		.lock()
		.unwrap()
		.queues
		.complete(queue::Completion::Index(vec![sequence]));
	changed.notify_waiters();
	assert!(futures::poll!(&mut drain).is_pending());
	drop(guard);
	assert!(matches!(
		futures::poll!(&mut drain),
		std::task::Poll::Ready(Ok(()))
	));
}

#[tokio::test]
async fn full_reply_channel_does_not_block_reception() {
	let (sender, _receiver) = tokio::sync::mpsc::channel(1);
	let ack = Ack { id: "first".into() };
	sender.send(ClientMessage::Ack(ack)).await.unwrap();
	let duplicate = Request {
		arg: RequestArg::Wait,
		id: "first".into(),
	};
	let next = Request {
		arg: RequestArg::Wait,
		id: "next".into(),
	};
	let messages = futures::stream::iter([
		Ok(ServerMessage::Request(duplicate)),
		Ok(ServerMessage::Request(next)),
	])
	.boxed();
	let options = crate::control::stream_options();
	let mut control = crate::control::Stream::new(messages, sender, options);
	control.acknowledge_now("first".into());
	let message = tokio::time::timeout(
		std::time::Duration::from_secs(1),
		control.recv_without_ack(),
	)
	.await
	.unwrap()
	.unwrap()
	.unwrap();
	assert!(matches!(message, ServerMessage::Request(Request { id, .. }) if id == "next"));
}
#[tokio::test]
async fn concurrent_allocators_wait_for_a_durable_reservation() {
	let state = State {
		available: true,
		limits: super::limits::Limits::default(),
		queues: queue::Queues::empty(),
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
		assert!(sequences.insert(result.unwrap().unwrap()));
	}
	assert_eq!(sequences, (0..32).collect());
}
#[tokio::test]
async fn cancelling_a_handler_releases_its_capacity() {
	let mut state = State {
		available: true,
		limits: super::limits::Limits::default(),
		queues: queue::Queues::empty(),
		writes: 1,
	};
	let arg = super::IndexRequestArg {
		batch: crate::store::index::queue::batch::Id::new([0; 16]),
		fragment: 0,
		fragments: 1,
		payload: bytes::Bytes::new(),
	};
	let request = Request {
		arg: RequestArg::Index(arg),
		id: "index".into(),
	};
	assert!(
		state
			.limits
			.try_insert(&request, &crate::config::IndexerRequest::default())
			.unwrap()
	);
	let state = Arc::new(Mutex::new(state));
	let changed = Arc::new(tokio::sync::Notify::new());
	let guard = super::Guard {
		changed: changed.clone(),
		id: request.id,
		state: state.clone(),
		writing: true,
	};
	let mut requests = tokio::task::JoinSet::new();
	requests.spawn(async move {
		let _guard = guard;
		std::future::pending::<()>().await;
	});
	requests.shutdown().await;
	let state = state.lock().unwrap();
	assert_eq!(state.writes, 0);
	assert!(!state.limits.has_queue_requests());
}
