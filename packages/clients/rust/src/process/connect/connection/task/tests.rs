use super::*;

struct Fixture {
	input: mpsc::Sender<tg::Result<ServerMessage>>,
	receiver: mpsc::Receiver<tg::Result<ClientMessage>>,
	session: Session,
}

#[tokio::test]
async fn closing_reads_does_not_retain_requests_without_waiters() {
	let mut fixture = fixture();
	for _ in 0..192 {
		let read = fixture
			.session
			.read(read::Arg::default(), futures::stream::pending().boxed())
			.await
			.unwrap();
		assert!(matches!(
			next_request(&mut fixture).await.arg,
			ClientRequestArg::Read(_)
		));
		drop(read);
		assert!(matches!(
			next_request(&mut fixture).await.arg,
			ClientRequestArg::Close(_)
		));
	}
	let detach = fixture
		.session
		.start_request(ClientRequestArg::Detach)
		.await
		.unwrap();
	assert!(matches!(
		next_request(&mut fixture).await.arg,
		ClientRequestArg::Detach
	));
	drop(detach);
}

#[tokio::test]
async fn disconnect_fails_a_pending_request() {
	let mut fixture = fixture();
	let response = fixture
		.session
		.start_request(ClientRequestArg::Detach)
		.await
		.unwrap();
	assert_eq!(next_request(&mut fixture).await.id, 1);
	fixture
		.input
		.send(Err(tg::error!("the transport failed")))
		.await
		.unwrap();
	let error = tokio::time::timeout(std::time::Duration::from_secs(1), response)
		.await
		.unwrap()
		.unwrap_err();
	assert!(error.to_string().contains("the transport failed"));
}

#[tokio::test]
async fn requests_do_not_wait_for_receipt_acknowledgments() {
	let mut fixture = fixture();
	let mut responses = Vec::new();
	for _ in 0..64 {
		responses.push(
			fixture
				.session
				.start_request(ClientRequestArg::Close(0))
				.await
				.unwrap(),
		);
		next_request(&mut fixture).await;
		responses.push(
			fixture
				.session
				.start_request(ClientRequestArg::Read(read::Arg::default()))
				.await
				.unwrap(),
		);
		next_request(&mut fixture).await;
	}
	let arg = tg::process::cancel::Arg {
		lease: "lease".into(),
		location: None,
	};
	responses.push(
		fixture
			.session
			.start_request(ClientRequestArg::Cancel(arg))
			.await
			.unwrap(),
	);
	assert_eq!(next_request(&mut fixture).await.id, 129);
}

#[tokio::test]
async fn pending_limits_are_independent_and_released_by_responses() {
	let mut fixture = fixture();
	let mut responses = Vec::new();
	for _ in 0..64 {
		responses.push(
			fixture
				.session
				.start_request(ClientRequestArg::Close(0))
				.await
				.unwrap(),
		);
		next_request(&mut fixture).await;
	}
	assert!(
		fixture
			.session
			.start_request(ClientRequestArg::Close(0))
			.await
			.is_err()
	);
	responses.push(
		fixture
			.session
			.start_request(ClientRequestArg::Detach)
			.await
			.unwrap(),
	);
	assert!(matches!(
		next_request(&mut fixture).await.arg,
		ClientRequestArg::Detach
	));
	fixture
		.input
		.send(Ok(ServerMessage::Ack(Ack { id: 1 })))
		.await
		.unwrap();
	assert!(
		fixture
			.session
			.start_request(ClientRequestArg::Close(0))
			.await
			.is_err()
	);
	let response = ServerResponse {
		error: None,
		id: 1,
		output: Some(ServerResponseOutput::Close),
	};
	fixture
		.input
		.send(Ok(ServerMessage::Response(response)))
		.await
		.unwrap();
	responses.remove(0).await.unwrap();
	responses.push(
		fixture
			.session
			.start_request(ClientRequestArg::Close(0))
			.await
			.unwrap(),
	);
	assert!(matches!(
		next_request(&mut fixture).await.arg,
		ClientRequestArg::Close(0)
	));
}

#[tokio::test]
async fn detach_precedes_queued_requests() {
	let mut fixture = fixture();
	let first = fixture
		.session
		.start_request(ClientRequestArg::Close(0))
		.await
		.unwrap();
	let second = fixture
		.session
		.start_request(ClientRequestArg::Close(0))
		.await
		.unwrap();
	let detach = fixture
		.session
		.start_request(ClientRequestArg::Detach)
		.await
		.unwrap();
	assert_eq!(next_request(&mut fixture).await.id, 1);
	assert_eq!(next_request(&mut fixture).await.id, 3);
	assert_eq!(next_request(&mut fixture).await.id, 2);
	drop((first, second, detach));
}

#[tokio::test]
async fn receipt_does_not_complete_a_request() {
	let mut fixture = fixture();
	let response = fixture
		.session
		.start_request(ClientRequestArg::Detach)
		.await
		.unwrap();
	let request = next_request(&mut fixture).await;
	assert_eq!(request.id, 1);
	fixture
		.input
		.send(Ok(ServerMessage::Ack(Ack { id: 1 })))
		.await
		.unwrap();
	let mut response = response;
	assert!(futures::poll!(&mut response).is_pending());
	let output = ServerResponse {
		error: None,
		id: 1,
		output: Some(ServerResponseOutput::Detach),
	};
	fixture
		.input
		.send(Ok(ServerMessage::Response(output)))
		.await
		.unwrap();
	assert!(matches!(
		response.await.unwrap(),
		Some(ServerResponseOutput::Detach)
	));
}

#[tokio::test]
async fn responses_progress_while_the_request_body_is_full() {
	let mut fixture = fixture();
	let response = fixture
		.session
		.start_request(ClientRequestArg::Detach)
		.await
		.unwrap();
	assert_eq!(next_request(&mut fixture).await.id, 1);
	let _second = fixture
		.session
		.start_request(ClientRequestArg::Detach)
		.await
		.unwrap();
	let _third = fixture
		.session
		.start_request(ClientRequestArg::Detach)
		.await
		.unwrap();
	let output = ServerResponse {
		error: None,
		id: 1,
		output: Some(ServerResponseOutput::Detach),
	};
	fixture
		.input
		.send(Ok(ServerMessage::Response(output)))
		.await
		.unwrap();
	let output = tokio::time::timeout(std::time::Duration::from_secs(1), response)
		.await
		.unwrap()
		.unwrap();
	assert!(matches!(output, Some(ServerResponseOutput::Detach)));
}

#[tokio::test]
async fn requests_are_written_in_registration_order() {
	let mut fixture = fixture();
	let mut responses = Vec::new();
	for _ in 0..64 {
		let response = fixture
			.session
			.start_request(ClientRequestArg::Detach)
			.await
			.unwrap();
		responses.push(response);
	}
	for id in 1..=64 {
		assert_eq!(next_request(&mut fixture).await.id, id);
	}
}

#[tokio::test]
async fn read_reports_disconnect_after_yielding_a_chunk() {
	let mut fixture = fixture();
	let (sender, input) = async_channel::bounded(4);
	let arg = read::Arg {
		streams: vec![tg::process::stdio::Stream::Stdout],
		..Default::default()
	};
	let mut stream = fixture.session.read(arg, input.boxed()).await.unwrap();
	let request = next_request(&mut fixture).await;
	let chunk = tg::process::stdio::Chunk {
		bytes: bytes::Bytes::from_static(b"a"),
		combined_position: 0,
		stream: tg::process::stdio::Stream::Stdout,
		stream_position: 0,
		timestamp: None,
	};
	let notification = ReadServerNotification {
		id: request.id,
		event: read::Event::Chunk(chunk),
	};
	fixture
		.input
		.send(Ok(ServerMessage::Notification(ServerNotification::Read(
			notification,
		))))
		.await
		.unwrap();
	assert!(stream.try_next().await.unwrap().is_some());
	fixture
		.input
		.send(Err(tg::error!("the transport failed")))
		.await
		.unwrap();
	let output = tokio::time::timeout(std::time::Duration::from_secs(1), stream.try_next())
		.await
		.unwrap()
		.unwrap_err();
	assert!(output.to_string().contains("transport failed"));
	drop(sender);
}

#[tokio::test]
async fn read_acknowledgment_waits_for_consumption() {
	let mut fixture = fixture();
	let (sender, input) = async_channel::bounded(4);
	let arg = read::Arg {
		streams: vec![tg::process::stdio::Stream::Stdout],
		..Default::default()
	};
	let mut stream = fixture.session.read(arg, input.boxed()).await.unwrap();
	let request = next_request(&mut fixture).await;
	let output = read::Output::End(tg::process::stdio::End {
		combined_position: 0,
		stream_positions: BTreeMap::new(),
	});
	let response = ServerResponse {
		error: None,
		id: request.id,
		output: Some(ServerResponseOutput::Read(output)),
	};
	fixture
		.input
		.send(Ok(ServerMessage::Response(response)))
		.await
		.unwrap();
	assert!(matches!(
		stream.try_next().await.unwrap(),
		Some(read::ServerMessage::Response(_))
	));
	assert!(fixture.receiver.try_recv().is_err());
	sender.send(Ok(read::ClientMessage::Ack)).await.unwrap();
	let message = fixture.receiver.recv().await.unwrap().unwrap();
	assert!(matches!(message, ClientMessage::Ack(Ack { id }) if id == request.id));
}

fn fixture() -> Fixture {
	let (sender, messages) = mpsc::channel(64);
	let (updates, update_receiver) = mpsc::unbounded_channel();
	let (input, input_receiver) = mpsc::channel(64);
	let (output_sender, receiver) = mpsc::channel(1);
	let (outcome, outcome_receiver) = watch::channel(None);
	let (output, output_receiver) = watch::channel(None);
	let (status, _) = watch::channel(Status::default());
	let state = State {
		high: VecDeque::new(),
		initial: Vec::new(),
		input: ReceiverStream::new(input_receiver).boxed(),
		next_id: 1,
		outcome,
		output,
		pending: IndexMap::new(),
		progress: None,
		reads: BTreeMap::new(),
		ready: true,
		sender: output_sender,
		status: status.clone(),
	};
	let update_sender = updates.clone();
	let task = Task::spawn(move |_| async move {
		state.run(messages, update_receiver, update_sender).await;
	});
	let session = Session {
		initial: Arc::new(std::sync::Mutex::new(Vec::new())),
		outcome: outcome_receiver,
		output: output_receiver,
		sender,
		status,
		task: Arc::new(task),
		updates,
	};
	Fixture {
		input,
		receiver,
		session,
	}
}

async fn next_request(fixture: &mut Fixture) -> ClientRequest {
	loop {
		let message =
			tokio::time::timeout(std::time::Duration::from_secs(1), fixture.receiver.recv())
				.await
				.unwrap()
				.unwrap()
				.unwrap();
		if let ClientMessage::Request(request) = message {
			return request;
		}
	}
}
