use {super::*, tangram_http::request::Ext as _};

#[tokio::test]
async fn decode_with_output_preserves_error_trailers() {
	let stream =
		futures::stream::iter([Ok(7_u64), Err(tg::error!("the test stream failed"))]).boxed();
	let body = encode(stream, 1024);
	let body = tangram_http::body::output::set(body, &42_u64).unwrap();
	let (output, mut stream) = decode_with_output::<u64, u64>(body, 1024).await.unwrap();

	assert_eq!(output, 42);
	assert_eq!(stream.try_next().await.unwrap(), Some(7));
	assert!(stream.try_next().await.is_err());
}

#[tokio::test]
async fn decode_transport_failure_ends_the_attempt() {
	for tail in [
		Bytes::new(),
		Bytes::from_static(&[0x80]),
		Bytes::from_static(&[0x10, 0]),
	] {
		let bytes = tangram_serialize::to_vec(&7_u64).unwrap();
		let mut frame = vec![u8::try_from(bytes.len()).unwrap()];
		frame.extend(bytes);
		let error = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
		let stream = futures::stream::iter([Ok(Bytes::from(frame)), Ok(tail), Err(error)]);
		let body = Boxed::with_data_stream(stream);
		let mut stream = decode::<u64>(body, 1024);

		assert_eq!(stream.try_next().await.unwrap(), Some(7));
		assert_eq!(stream.try_next().await.unwrap(), None);
	}
}

#[tokio::test]
async fn reconnect_preserves_the_resolved_reverse_window() {
	for (position, length) in [(4, -3), (50, -49), (100, -99)] {
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let url = format!("http://{}", listener.local_addr().unwrap())
			.parse()
			.unwrap();
		let (sender, receiver) = async_channel::unbounded();
		let requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let server = Task::spawn(move |_| async move {
			let mut connections = tokio::task::JoinSet::new();
			while let Ok((socket, _)) = listener.accept().await {
				let sender = sender.clone();
				let requests = requests.clone();
				let service = hyper::service::service_fn(
					move |request: http::Request<hyper::body::Incoming>| {
						let sender = sender.clone();
						let requests = requests.clone();
						async move {
							let (arg, request) = request.arg::<read::Arg>().await.unwrap();
							let arg = arg.unwrap();
							sender.try_send(arg).unwrap();
							let attempt =
								requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
							let messages = if attempt == 0 {
								let chunk = Chunk {
									bytes: Bytes::from_static(b"x"),
									combined_position: position - 1,
									stream: Stream::Stdout,
									stream_position: position - 1,
									timestamp: None,
								};
								vec![
									read::ServerMessage::Notification(
										read::ServerNotification::Position {
											length: Some(length),
											position,
										},
									),
									read::ServerMessage::Notification(
										read::ServerNotification::Chunk(chunk),
									),
									read::ServerMessage::Notification(
										read::ServerNotification::Stop,
									),
								]
							} else {
								vec![read::ServerMessage::Request(read::ServerRequest::End)]
							};
							let input = Task::spawn(move |_| async move {
								BodyStream::new(request.into_body())
									.try_collect::<Vec<_>>()
									.await
									.ok();
							});
							let stream = stream::iter(messages.into_iter().map(Ok))
								.chain(stream::pending())
								.attach(input)
								.boxed();
							let body = encode(stream, 1024);
							let response = http::Response::builder()
								.header(http::header::CONTENT_TYPE, TANGRAM_CONTENT_TYPE)
								.body(body)
								.unwrap();
							Ok::<_, std::convert::Infallible>(response)
						}
					},
				);
				connections.spawn(async move {
					let io = hyper_util::rt::TokioIo::new(socket);
					hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
						.serve_connection(io, service)
						.await
						.ok();
				});
			}
		});
		let arg = tg::Arg {
			url: Some(url),
			..Default::default()
		};
		let client = tg::Client::new(arg).unwrap();
		let id = tg::process::Id::new();
		let arg = read::Arg {
			length: Some(-99),
			position: Some(std::io::SeekFrom::End(96)),
			streams: vec![Stream::Stdout],
			..Default::default()
		};
		let stream = tokio::time::timeout(
			std::time::Duration::from_secs(5),
			client.try_read_process_stdio_all(&id, arg),
		)
		.await
		.unwrap()
		.unwrap()
		.unwrap();
		let chunks = tokio::time::timeout(
			std::time::Duration::from_secs(5),
			stream.try_collect::<Vec<_>>(),
		)
		.await
		.unwrap()
		.unwrap();
		assert_eq!(chunks.len(), 1);
		assert_eq!(chunks[0].bytes, b"x".as_slice());
		let initial = receiver.try_recv().unwrap();
		assert_eq!(initial.position, Some(std::io::SeekFrom::End(96)));
		let resumed = receiver.try_recv().unwrap();
		assert_eq!(
			resumed.position,
			Some(std::io::SeekFrom::Start(position - 1))
		);
		assert_eq!(resumed.length, Some(length + 1));
		server.abort();
	}
}
