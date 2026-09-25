use super::*;

#[tokio::test]
async fn buffered_output_survives_control_retirement() {
	let (local, mut receiver) = Local::new();
	let task = tokio::spawn(async move {
		let Some(Message::Request { request, sender }) = receiver.recv().await else {
			panic!("expected a read request");
		};
		let bytes = bytes::Bytes::from(vec![0; flow::CHUNK_SIZE]);
		for index in 0..flow::MAX_CHUNKS {
			let position = (index * flow::CHUNK_SIZE) as u64;
			let chunk = tg::process::stdio::Chunk {
				bytes: bytes.clone(),
				combined_position: position,
				stream: tg::process::stdio::Stream::Stdout,
				stream_position: position,
				timestamp: None,
			};
			let notification = tg::process::control::ReadClientNotification {
				event: read::Event::Chunk(chunk),
				id: request.id.clone(),
			};
			let message = tg::process::control::ClientMessage::Notification(
				tg::process::control::ClientNotification::Read(notification),
			);
			sender.send_low(message).await.unwrap();
		}
		let end = tg::process::stdio::End {
			combined_position: flow::WINDOW,
			stream_positions: [(tg::process::stdio::Stream::Stdout, flow::WINDOW)].into(),
		};
		let response = tg::process::control::ClientResponse {
			error: None,
			id: request.id,
			output: Some(tg::process::control::ClientResponseOutput::Read(
				read::Output::End(end),
			)),
		};
		sender
			.send_low(tg::process::control::ClientMessage::Response(response))
			.await
			.unwrap();

		// Retire control before the consumer acknowledges the queued output.
		drop(receiver);
	});
	let arg = read::Arg {
		streams: vec![tg::process::stdio::Stream::Stdout],
		..Default::default()
	};
	let messages = local.read(arg).try_collect::<Vec<_>>().await.unwrap();
	assert_eq!(messages.len(), flow::MAX_CHUNKS + 1);
	assert!(matches!(
		messages.last(),
		Some(read::ServerMessage::Response(read::Output::End(_)))
	));
	task.await.unwrap();
}

#[tokio::test]
async fn read_reports_progress_and_closes() {
	let (control_sender, mut receiver) = Local::new();
	let arg = read::Arg {
		streams: vec![tg::process::stdio::Stream::Stdout],
		..Default::default()
	};
	let mut stream = control_sender.read(arg);
	assert!(stream.next().now_or_never().is_none());
	let Message::Request { request, sender } = receiver.try_recv().unwrap() else {
		panic!("expected a read request");
	};
	for batch in 0..2 {
		for index in 0..flow::MAX_CHUNKS / 2 {
			let position = ((batch * flow::MAX_CHUNKS / 2 + index) * flow::CHUNK_SIZE) as u64;
			let chunk = tg::process::stdio::Chunk {
				bytes: bytes::Bytes::from(vec![0; flow::CHUNK_SIZE]),
				combined_position: position,
				stream: tg::process::stdio::Stream::Stdout,
				stream_position: position,
				timestamp: None,
			};
			let notification = tg::process::control::ReadClientNotification {
				event: read::Event::Chunk(chunk),
				id: request.id.clone(),
			};
			let message = tg::process::control::ClientMessage::Notification(
				tg::process::control::ClientNotification::Read(notification),
			);
			sender.send_low(message).await.unwrap();
			assert!(matches!(
				stream.try_next().await.unwrap(),
				Some(read::ServerMessage::Notification(read::Event::Chunk(_)))
			));
		}
		assert!(stream.next().now_or_never().is_none());
		let Message::Progress(notification) = receiver.try_recv().unwrap() else {
			panic!("expected read progress");
		};
		assert_eq!(notification.id, request.id);
		assert_eq!(
			notification.progress.consumed,
			(batch as u64 + 1) * flow::WINDOW / 2
		);
	}
	drop(stream);
	let message = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
		.await
		.unwrap()
		.unwrap();
	let Message::Close(id) = message else {
		panic!("expected a read close");
	};
	assert_eq!(id, request.id);
}

#[tokio::test]
async fn send_request_enqueues_before_waiting_and_preserves_response_errors() {
	let (local, mut receiver) = Local::new();
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
	let (local, mut receiver) = Local::new();
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
