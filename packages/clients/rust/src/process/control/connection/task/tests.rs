use {super::*, futures::FutureExt as _, tokio_stream::wrappers::ReceiverStream};

#[tokio::test]
async fn reconnect_replays_pending_requests_before_new_requests() {
	let (input, receiver) = mpsc::channel(8);
	let connection = Connection::with_input(ReceiverStream::new(receiver).boxed());
	let sender = connection.sender();
	let first = sender
		.request(request("first"), Priority::Low)
		.await
		.unwrap();
	let second = sender
		.request(request("second"), Priority::Low)
		.await
		.unwrap();
	let (_, mut transport) = sender.attach().await.unwrap();
	assert_eq!(next_request(&mut transport).await, "first");
	assert_eq!(next_request(&mut transport).await, "second");
	input
		.send(Ok(tg::control::Event::Message(ServerMessage::Ack(
			ServerAck { id: "first".into() },
		))))
		.await
		.unwrap();
	// Queue another request while reconnect replaces the old transport.
	let (_, mut next) = sender.attach().await.unwrap();
	let third = sender
		.request(request("third"), Priority::Low)
		.await
		.unwrap();
	assert_eq!(next_request(&mut next).await, "first");
	assert_eq!(next_request(&mut next).await, "second");
	assert_eq!(next_request(&mut next).await, "third");
	assert!(first.now_or_never().is_none());
	drop((second, third));
}

#[tokio::test]
async fn response_completes_when_the_outgoing_stream_is_full() {
	let (input, receiver) = mpsc::channel(8);
	let connection = Connection::with_input(ReceiverStream::new(receiver).boxed());
	let sender = connection.sender();
	let (_, mut transport) = sender.attach().await.unwrap();
	let response = sender
		.request(request("first"), Priority::High)
		.await
		.unwrap();
	assert_eq!(next_request(&mut transport).await, "first");
	let _second = sender
		.request(request("second"), Priority::High)
		.await
		.unwrap();
	let _third = sender
		.request(request("third"), Priority::High)
		.await
		.unwrap();
	let message = ServerMessage::Response(ServerResponse {
		error: None,
		id: "first".into(),
		output: Some(ServerResponseOutput::Write(
			tg::process::stdio::write::Output {
				closed: false,
				length: 0,
			},
		)),
	});
	input
		.send(Ok(tg::control::Event::Message(message)))
		.await
		.unwrap();
	let message = tokio::time::timeout(Duration::from_secs(1), response)
		.await
		.unwrap()
		.unwrap();
	assert!(matches!(message, ServerMessage::Response(ServerResponse { id, .. }) if id == "first"));
}

#[tokio::test]
async fn cancellation_removes_requests_before_replay() {
	let (_input, receiver) = mpsc::channel(8);
	let connection = Connection::with_input(ReceiverStream::new(receiver).boxed());
	let sender = connection.sender();
	let response = sender
		.request(request("cancelled"), Priority::High)
		.await
		.unwrap();
	drop(response);
	sender.wait_for_empty().await;
	let (_, mut transport) = sender.attach().await.unwrap();
	let response = sender
		.request(request("live"), Priority::High)
		.await
		.unwrap();
	assert_eq!(next_request(&mut transport).await, "live");
	drop(response);
}

#[tokio::test]
async fn start_requires_a_successful_response_before_resume() {
	for success in [false, true] {
		let (events, _receiver) = mpsc::channel(64);
		let mut state = State::new(stream::pending().boxed(), None, None, events);
		let value = serde_json::json!({
			"command": tg::command::Id::new(b"command"),
			"created_at": 0,
			"host": "test",
			"status": "started",
		});
		let start = StartClientRequestArg {
			data: serde_json::from_value(value).unwrap(),
			lease: "lease".into(),
			options: tg::referent::Options::default(),
			parent: Some(tg::process::Id::new()),
			sandbox: None,
		};
		let request = ClientRequest {
			arg: ClientRequestArg::Start(start),
			id: "start".into(),
		};
		let message = Message::Send {
			message: ClientMessage::Request(request),
			priority: Priority::High,
			response: None,
		};
		state.handle_message(message).unwrap();
		assert!(state.take_message(Priority::High).is_some());
		let ack = ServerAck { id: "start".into() };
		state.handle_server_message(tg::control::Event::Message(ServerMessage::Ack(ack)));

		// A receipt leaves the original Start available for replay.
		let lease = attach(&mut state);
		assert!(lease.is_none());
		assert!(matches!(
			state.take_message(Priority::High),
			Some(ClientMessage::Request(ClientRequest { arg: ClientRequestArg::Start(_), id }))
			if id == "start"
		));

		let response = ServerResponse {
			error: (!success).then(tg::error::Data::default),
			id: "start".into(),
			output: success.then_some(ServerResponseOutput::Start(StartServerResponseOutput {})),
		};
		state.handle_server_message(tg::control::Event::Message(ServerMessage::Response(
			response,
		)));
		let lease = attach(&mut state);
		assert_eq!(lease.as_deref(), success.then_some("lease"));
		if success {
			assert!(state.take_message(Priority::High).is_none());
		}
	}
}

fn attach(state: &mut State) -> Option<String> {
	let (high, _high) = mpsc::channel(1);
	let (low, _low) = mpsc::channel(1);
	let (sender, mut receiver) = oneshot::channel();
	let update = Update::Attach { high, low, sender };
	state.handle_update(update);
	receiver.try_recv().unwrap()
}

fn request(id: &str) -> ClientMessage {
	let end = tg::process::stdio::End {
		combined_position: 0,
		stream_positions: BTreeMap::new(),
	};
	ClientMessage::Request(ClientRequest {
		arg: ClientRequestArg::Write(tg::process::stdio::write::Data::End(end)),
		id: id.into(),
	})
}

async fn next_request(transport: &mut BoxStream<'static, tg::Result<ClientMessage>>) -> String {
	loop {
		let message = tokio::time::timeout(Duration::from_secs(1), transport.try_next())
			.await
			.unwrap()
			.unwrap()
			.unwrap();
		if let ClientMessage::Request(request) = message {
			return request.id;
		}
	}
}
