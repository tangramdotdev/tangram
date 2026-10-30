use {
	super::{Ack, ClientMessage, Indexer, Request, RequestArg, ServerMessage, queue},
	crate::IndexerState,
	futures::StreamExt as _,
	std::sync::{Arc, Mutex},
	tangram_client::prelude::*,
};

#[test]
fn subjects_address_an_idless_or_named_indexer() {
	assert_eq!(Indexer::server_subject(None), "indexers.server");
	let id = tg::indexer::Id::new();
	assert_eq!(
		Indexer::server_subject(Some(&id)),
		format!("indexers.{id}.server")
	);
}

#[tokio::test]
async fn draining_waits_for_writes_queue_completions_and_handlers() {
	let mut queues = queue::Queues::empty(usize::MAX);
	let reservation = queue::SequenceReservation {
		end: 1,
		kind: queue::Kind::Index,
	};
	queues.finish_reservation(reservation);
	let sequence = queues.try_allocate_sequence(queue::Kind::Index).unwrap();
	queues.insert_entry(queue::Kind::Index, sequence, bytes::Bytes::new());
	queues.finish_write(queue::Kind::Index, sequence, true);
	let state = IndexerState {
		available: true,
		enqueues: 1,
		queues,
	};
	let state = Arc::new(Mutex::new(state));
	let changed = Arc::new(tokio::sync::Notify::new());
	let mut guard = super::Guard {
		changed: changed.clone(),
		enqueue: true,
		state: state.clone(),
	};
	let drain = Indexer::drain_queues(&state, &changed);
	tokio::pin!(drain);
	assert!(futures::poll!(&mut drain).is_pending());
	assert!(!state.lock().unwrap().available);
	guard.finish();
	assert!(futures::poll!(&mut drain).is_pending());
	let work = state
		.lock()
		.unwrap()
		.queues
		.pop_entry(queue::Kind::Index)
		.unwrap();
	assert_eq!(work.sequence, sequence);
	assert!(futures::poll!(&mut drain).is_pending());
	state
		.lock()
		.unwrap()
		.queues
		.complete(queue::Kind::Index, sequence);
	changed.notify_waiters();
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
	let options = tangram_server_core::control::stream_options(
		&tangram_server_core::config::Control::default(),
	);
	let mut control = tangram_server_core::control::Stream::new(messages, sender, options);
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
async fn cancelling_a_handler_releases_its_write() {
	let state = IndexerState {
		available: true,
		enqueues: 1,
		queues: queue::Queues::empty(usize::MAX),
	};
	let state = Arc::new(Mutex::new(state));
	let changed = Arc::new(tokio::sync::Notify::new());
	let guard = super::Guard {
		changed: changed.clone(),
		enqueue: true,
		state: state.clone(),
	};
	let mut requests = tokio::task::JoinSet::new();
	requests.spawn(async move {
		let _guard = guard;
		std::future::pending::<()>().await;
	});
	requests.shutdown().await;
	assert_eq!(state.lock().unwrap().enqueues, 0);
}

#[test]
fn messages_round_trip() {
	let arg = super::EnqueueRequestArg {
		archive: Some(bytes::Bytes::from_static(b"archive")),
		index: None,
	};
	let request = Request {
		arg: RequestArg::Enqueue(arg),
		id: "request".into(),
	};
	let message = ServerMessage::Request(request);
	let bytes = tangram_messenger::Payload::serialize(&message).unwrap();
	let message = <ServerMessage as tangram_messenger::Payload>::deserialize(bytes).unwrap();
	let ServerMessage::Request(Request {
		arg: RequestArg::Enqueue(arg),
		id,
	}) = message
	else {
		panic!("expected an enqueue request");
	};
	assert_eq!(id, "request");
	assert_eq!(arg.archive.unwrap(), &b"archive"[..]);
	assert!(arg.index.is_none());
}
