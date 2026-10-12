use super::*;

struct Fixture {
	input: mpsc::Sender<tg::Result<ServerMessage>>,
	receiver: mpsc::Receiver<tg::Result<ClientMessage>>,
	session: Session,
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
	for _ in 0..63 {
		responses.push(fixture.session.start_request(signal()).await.unwrap());
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
	assert_eq!(next_request(&mut fixture).await.id, 64);
}

#[tokio::test]
async fn pending_requests_share_a_limit_released_by_responses() {
	let mut fixture = fixture();
	let mut responses = Vec::new();
	for _ in 0..64 {
		responses.push(fixture.session.start_request(signal()).await.unwrap());
		next_request(&mut fixture).await;
	}
	assert!(
		fixture
			.session
			.start_request(ClientRequestArg::Detach)
			.await
			.is_err()
	);
	fixture
		.input
		.send(Ok(ServerMessage::Ack(Ack { id: 1 })))
		.await
		.unwrap();
	assert!(fixture.session.start_request(signal()).await.is_err());
	let response = ServerResponse {
		error: None,
		id: 1,
		output: Some(ServerResponseOutput::Signal),
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
			.start_request(ClientRequestArg::Detach)
			.await
			.unwrap(),
	);
	assert!(matches!(
		next_request(&mut fixture).await.arg,
		ClientRequestArg::Detach
	));
}

#[tokio::test]
async fn detach_precedes_queued_requests() {
	let mut fixture = fixture();
	let first = fixture.session.start_request(signal()).await.unwrap();
	let second = fixture.session.start_request(signal()).await.unwrap();
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

fn fixture() -> Fixture {
	let (sender, messages) = mpsc::channel(64);
	let (update_sender, update_receiver) = mpsc::unbounded_channel();
	let (input, input_receiver) = mpsc::channel(64);
	let (output_sender, receiver) = mpsc::channel(1);
	let (outcome, outcome_receiver) = watch::channel(None);
	let (output, output_receiver) = watch::channel(None);
	let (status, _) = watch::channel(Status::default());
	let state = State {
		config: tg::process::connect::Config::default(),
		high: VecDeque::new(),
		input: ReceiverStream::new(input_receiver).boxed(),
		next_id: 1,
		outcome,
		output,
		pending: IndexMap::new(),
		progress: None,
		ready: true,
		sender: output_sender,
		status: status.clone(),
	};
	let task = Task::spawn(move |_| async move {
		state.run(messages, update_receiver, update_sender).await;
	});
	let session = Session {
		outcome: outcome_receiver,
		output: output_receiver,
		sender,
		status,
		task: Arc::new(task),
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

fn signal() -> ClientRequestArg {
	let arg = tg::process::signal::post::Arg {
		location: None,
		signal: tg::process::Signal::SIGTERM,
		tokens: tg::authorization::Tokens::default(),
	};
	ClientRequestArg::Signal(arg)
}
