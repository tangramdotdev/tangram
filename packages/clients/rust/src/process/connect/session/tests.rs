use super::*;

#[tokio::test]
async fn responses_are_acknowledged_when_the_request_queue_is_full() {
	let (acks, mut ack_receiver) = mpsc::channel(64);
	let (sender, mut receiver) = mpsc::channel(1);
	let message = ClientMessage::Request(ClientRequest {
		arg: ClientRequestArg::Detach,
		id: 2,
	});
	sender.try_send(Ok(message)).unwrap();
	let (response, response_receiver) = oneshot::channel();
	let (wait, _) = watch::channel(None);
	let state = State {
		acks,
		closed: AtomicBool::new(false),
		confirmed: AtomicBool::new(true),
		credit: Notify::new(),
		error: Mutex::new(None),
		initial: Mutex::new(Vec::new()),
		next_id: AtomicU64::new(3),
		output: Mutex::new(None),
		reads: Mutex::new(BTreeMap::new()),
		requests: Mutex::new(BTreeMap::from([(1, response)])),
		sender,
		unacknowledged: Mutex::new(BTreeSet::new()),
		wait,
	};
	let response = ServerResponse {
		error: None,
		id: 1,
		output: Some(ServerResponseOutput::Detach),
	};
	let output = stream::iter([Ok(ServerMessage::Response(response))]).boxed();
	let mut progress = None;
	tokio::time::timeout(
		std::time::Duration::from_secs(1),
		Session::task(&state, output, &mut progress),
	)
	.await
	.unwrap()
	.unwrap();
	assert!(matches!(
		ack_receiver.try_recv().unwrap().unwrap(),
		ClientMessage::Ack(Ack { id: 1 })
	));
	assert!(matches!(
		response_receiver.await.unwrap().unwrap(),
		ServerResponseOutput::Detach
	));
	assert!(matches!(
		receiver.try_recv().unwrap().unwrap(),
		ClientMessage::Request(ClientRequest { id: 2, .. })
	));
}

#[tokio::test]
async fn read_reports_disconnect_after_yielding_a_chunk() {
	// Open an initial stdout read.
	let (acks, _ack_receiver) = mpsc::channel(64);
	let (sender, receiver) = mpsc::channel(64);
	let (read_sender, read_receiver) = mpsc::channel(4);
	let (wait, _) = watch::channel(None);
	let arg = tg::process::stdio::read::Arg {
		streams: vec![tg::process::stdio::Stream::Stdout],
		..Default::default()
	};
	let state = State {
		acks,
		closed: AtomicBool::new(false),
		confirmed: AtomicBool::new(true),
		credit: Notify::new(),
		error: Mutex::new(None),
		initial: Mutex::new(vec![(1, arg.clone(), read_receiver)]),
		next_id: AtomicU64::new(2),
		output: Mutex::new(None),
		reads: Mutex::new(BTreeMap::from([(1, read_sender.clone())])),
		requests: Mutex::new(BTreeMap::new()),
		sender,
		unacknowledged: Mutex::new(BTreeSet::new()),
		wait,
	};
	let state = Arc::new(state);
	let connection = Session {
		state: state.clone(),
		task: Arc::new(Task::spawn(|_| futures::future::pending())),
	};
	let id = tg::process::Id::new();
	let client = tg::Client::new(tg::Arg::default()).unwrap();
	let connection = super::super::Connection::with_session(&client, connection);
	let handle = tg::handle::dynamic::Handle::with_connection(client, id.clone(), connection);
	let mut output = handle
		.try_read_process_stdio_all(&id, arg)
		.await
		.unwrap()
		.unwrap()
		.boxed();
	let chunk = tg::process::stdio::Chunk {
		bytes: bytes::Bytes::from_static(b"a"),
		combined_position: 0,
		stream: tg::process::stdio::Stream::Stdout,
		stream_position: 0,
		timestamp: None,
	};
	let message = tg::process::stdio::read::ServerMessage::Notification(
		tg::process::stdio::read::Event::Chunk(chunk),
	);
	read_sender.send(Ok(message)).await.unwrap();
	assert!(output.try_next().await.unwrap().is_some());

	// A failed HTTP connection closes its request body and response task.
	drop(receiver);
	state.fail(Some(tg::error!("the transport failed")));
	drop(read_sender);
	tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	let result =
		tokio::time::timeout(std::time::Duration::from_millis(500), output.try_next()).await;
	let error = result
		.expect("the read must not retry a closed connection")
		.unwrap_err();
	assert!(error.to_string().contains("transport failed"));
}

#[tokio::test]
async fn writes_fill_the_window_without_waiting_for_receipt_or_completion() {
	let (acks, mut ack_receiver) = mpsc::channel(64);
	let (sender, mut receiver) = mpsc::channel(64);
	let (wait, _) = watch::channel(None);
	let state = Arc::new(State {
		acks,
		closed: AtomicBool::new(false),
		confirmed: AtomicBool::new(true),
		credit: Notify::new(),
		error: Mutex::new(None),
		initial: Mutex::new(Vec::new()),
		next_id: AtomicU64::new(1),
		output: Mutex::new(None),
		reads: Mutex::new(BTreeMap::new()),
		requests: Mutex::new(BTreeMap::new()),
		sender,
		unacknowledged: Mutex::new(BTreeSet::new()),
		wait,
	});
	let connection = Session {
		state: state.clone(),
		task: Arc::new(Task::spawn(|_| futures::future::pending())),
	};
	let (input, input_receiver) = async_channel::bounded(64);
	for id in 0..64 {
		let chunk = tg::process::stdio::Chunk {
			bytes: bytes::Bytes::from_static(b"x"),
			combined_position: id,
			stream: tg::process::stdio::Stream::Stdin,
			stream_position: id,
			timestamp: None,
		};
		let request = tg::process::stdio::write::Request {
			arg: tg::process::stdio::write::Data::Chunk(chunk),
			id,
		};
		input
			.send(Ok(tg::process::stdio::write::ClientMessage::Request(
				request,
			)))
			.await
			.unwrap();
	}
	let arg = tg::process::stdio::write::stream::Arg {
		streams: vec![tg::process::stdio::Stream::Stdin],
		..Default::default()
	};
	let client = tg::Client::new(tg::Arg::default()).unwrap();
	let connection = super::super::Connection::with_session(&client, connection);
	let mut output = connection.write(arg, input_receiver.boxed()).await.unwrap();
	let task = tokio::spawn(async move { output.try_next().await });
	for id in 1..=64 {
		let request = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
			.await
			.unwrap()
			.unwrap()
			.unwrap();
		assert!(
			matches!(request, ClientMessage::Request(ClientRequest { arg: ClientRequestArg::Write(_), id: request_id }) if request_id == id)
		);
	}
	let output = stream::iter([Ok(ServerMessage::Ack(Ack { id: 1 }))]).boxed();
	Session::task(&state, output, &mut None).await.unwrap();
	assert!(!task.is_finished());
	let response = ServerResponse {
		error: None,
		id: 1,
		output: Some(ServerResponseOutput::Write(
			tg::process::stdio::write::Output {
				closed: false,
				length: 1,
			},
		)),
	};
	Session::task(
		&state,
		stream::iter([Ok(ServerMessage::Response(response))]).boxed(),
		&mut None,
	)
	.await
	.unwrap();
	assert!(matches!(
		ack_receiver.recv().await.unwrap().unwrap(),
		ClientMessage::Ack(Ack { id: 1 })
	));
	let output = task.await.unwrap().unwrap().unwrap();
	assert!(matches!(
		output,
		tg::process::stdio::write::ServerMessage::Response(tg::process::stdio::write::Response {
			id: 0,
			output: Some(tg::process::stdio::write::Output { length: 1, .. }),
			..
		})
	));
}

#[tokio::test]
async fn receipt_acknowledgments_replenish_the_request_window() {
	let (acks, _ack_receiver) = mpsc::channel(1);
	let (sender, mut receiver) = mpsc::channel(1);
	let (wait, _) = watch::channel(None);
	let state = State {
		acks,
		closed: AtomicBool::new(false),
		confirmed: AtomicBool::new(true),
		credit: Notify::new(),
		error: Mutex::new(None),
		initial: Mutex::new(Vec::new()),
		next_id: AtomicU64::new(1),
		output: Mutex::new(None),
		reads: Mutex::new(BTreeMap::new()),
		requests: Mutex::new(BTreeMap::new()),
		sender,
		unacknowledged: Mutex::new(BTreeSet::new()),
		wait,
	};
	// Close requests also consume credit even though callers do not await their responses.
	for id in 1..=REQUEST_WINDOW as u64 {
		let request = ClientRequest {
			arg: ClientRequestArg::Close(0),
			id,
		};
		state.send_request(request).await.unwrap();
		receiver.recv().await.unwrap().unwrap();
	}
	let request = ClientRequest {
		arg: ClientRequestArg::Read(tg::process::stdio::read::Arg::default()),
		id: 1000,
	};
	let sending = state.send_request(request);
	tokio::pin!(sending);
	assert!(futures::poll!(&mut sending).is_pending());

	// A receipt acknowledgment restores credit without requiring operation completion.
	let output = stream::iter([Ok(ServerMessage::Ack(Ack { id: 1 }))]).boxed();
	Session::task(&state, output, &mut None).await.unwrap();
	sending.await.unwrap();
	receiver.recv().await.unwrap().unwrap();

	// Reserve one additional request for detachment, even with a full ordinary window.
	let request = ClientRequest {
		arg: ClientRequestArg::Detach,
		id: 1001,
	};
	state.send_request(request).await.unwrap();
	receiver.recv().await.unwrap().unwrap();
	let request = ClientRequest {
		arg: ClientRequestArg::Detach,
		id: 1002,
	};
	let sending = state.send_request(request);
	tokio::pin!(sending);
	assert!(futures::poll!(&mut sending).is_pending());
	state.fail(Some(tg::error!("the transport failed")));
	assert!(
		sending
			.await
			.unwrap_err()
			.to_string()
			.contains("transport failed")
	);
}
