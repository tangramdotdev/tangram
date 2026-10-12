use super::*;

fn chunk(position: u64) -> read::ServerMessage {
	let chunk = tg::process::stdio::Chunk {
		bytes: bytes::Bytes::from_static(b"a"),
		combined_position: position,
		stream: tg::process::stdio::Stream::Stdout,
		stream_position: position,
		timestamp: None,
	};
	read::ServerMessage::Notification(read::Event::Chunk(chunk))
}

#[tokio::test]
async fn read_delivers_output_after_control_retires() {
	let (local, mut receiver) = Local::new(256);
	let task = tokio::spawn(async move {
		let Some(Message::Read { sender, .. }) = receiver.recv().await else {
			panic!("expected a read");
		};

		// Retire control while the output is still being delivered.
		drop(receiver);
		for position in 0..4 {
			sender.send(Ok(chunk(position))).await.unwrap();
		}
		let end = tg::process::stdio::End {
			combined_position: 4,
			stream_positions: [(tg::process::stdio::Stream::Stdout, 4)].into(),
		};
		let response = read::ServerMessage::Response(read::Output::End(end));
		sender.send(Ok(response)).await.unwrap();
	});
	let arg = read::Arg {
		streams: vec![tg::process::stdio::Stream::Stdout],
		..Default::default()
	};
	let messages = local
		.read(arg, futures::stream::empty().boxed())
		.try_collect::<Vec<_>>()
		.await
		.unwrap();
	assert_eq!(messages.len(), 5);
	assert!(matches!(
		messages.last(),
		Some(read::ServerMessage::Response(read::Output::End(_)))
	));
	task.await.unwrap();
}

#[tokio::test]
async fn read_holds_back_the_runner_and_ends_when_dropped() {
	let (local, mut receiver) = Local::new(256);
	let arg = read::Arg {
		streams: vec![tg::process::stdio::Stream::Stdout],
		..Default::default()
	};
	let mut stream = local.read(arg, futures::stream::empty().boxed());
	assert!(stream.next().now_or_never().is_none());
	let Message::Read { sender, .. } = receiver.try_recv().unwrap() else {
		panic!("expected a read");
	};

	// The channel holds one message until the reader consumes it.
	sender.try_send(Ok(chunk(0))).unwrap();
	assert!(sender.try_send(Ok(chunk(1))).is_err());
	assert!(matches!(
		stream.try_next().await.unwrap(),
		Some(read::ServerMessage::Notification(read::Event::Chunk(_)))
	));
	sender.try_send(Ok(chunk(1))).unwrap();

	// Dropping the reader closes the channel.
	drop(stream);
	tokio::time::timeout(std::time::Duration::from_secs(1), sender.closed())
		.await
		.unwrap();
}

#[tokio::test]
async fn send_request_enqueues_before_waiting_and_preserves_response_errors() {
	let (local, mut receiver) = Local::new(256);
	let arg =
		tg::process::control::ServerRequestArg::Get(tg::process::control::GetServerRequestArg {});
	let response = local.send_request(arg).await.unwrap();
	let Message::Request { request, sender } = receiver.try_recv().unwrap() else {
		panic!("expected a get request");
	};
	let error = tg::error::Data {
		message: Some("the request failed".into()),
		..Default::default()
	};
	let message = tg::process::control::ClientResponse {
		error: Some(error),
		id: request.id,
		output: None,
	};
	sender
		.send(tg::process::control::ClientMessage::Response(message))
		.await
		.unwrap();
	assert!(response.await.unwrap().is_err());
}

#[tokio::test]
async fn send_request_distinguishes_transport_errors_and_abandoned_callers() {
	let (local, mut receiver) = Local::new(256);
	let arg =
		tg::process::control::ServerRequestArg::Get(tg::process::control::GetServerRequestArg {});
	let response = local.send_request(arg.clone()).await.unwrap();
	drop(receiver.try_recv().unwrap());
	assert!(response.await.is_err());
	let response = local.send_request(arg.clone()).await.unwrap();
	drop(response);
	let Message::Request { request, sender } = receiver.try_recv().unwrap() else {
		panic!("expected a get request");
	};
	let message = tg::process::control::ClientResponse {
		error: None,
		id: request.id,
		output: None,
	};
	sender
		.send(tg::process::control::ClientMessage::Response(message))
		.await
		.unwrap();
	drop(sender);
	drop(receiver);
	assert!(local.send_request(arg).await.is_err());
}
