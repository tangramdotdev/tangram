use {
	super::*,
	crate::process::stdio::{
		self, End, Stream,
		read::{self, Output},
	},
	futures::TryStreamExt as _,
	std::{collections::BTreeMap, io::SeekFrom, time::Duration},
};

struct MockConnection {
	input: BoxStream<'static, tg::Result<ClientMessage>>,
	output: async_channel::Sender<tg::Result<ServerMessage>>,
}

impl MockConnection {
	async fn connect(&mut self, id: &tg::process::Id) -> Arg {
		let request = self.request().await;
		assert_eq!(request.id, 0);
		let ClientRequestArg::Connect(arg) = request.arg else {
			panic!("expected the opening request");
		};
		let tg::Either::Right(process_id) = &arg.process else {
			panic!("expected to connect to the existing process");
		};
		assert_eq!(process_id, id);
		assert_eq!(arg.mode, Mode::Run);
		let output = tg::process::spawn::Output {
			cached: false,
			lease: None,
			location: None,
			process: tg::Either::Right(id.clone()),
			tokens: tg::Tokens::default(),
			wait: None,
		};
		self.respond(0, ServerResponseOutput::Connect(output)).await;
		arg
	}

	async fn request(&mut self) -> ClientRequest {
		loop {
			if let ClientMessage::Request(request) = self.input.try_next().await.unwrap().unwrap() {
				return request;
			}
		}
	}

	async fn respond(&mut self, id: u64, output: ServerResponseOutput) {
		let response = ServerResponse {
			error: None,
			id,
			output: Some(output),
		};
		self.output
			.send(Ok(ServerMessage::Response(response)))
			.await
			.unwrap();
	}
}

#[tokio::test]
async fn handles_preserve_not_found() {
	let (client, _, _server) = mock_server(http::StatusCode::NOT_FOUND).await;
	let session = client.session(&client.context);
	let handles = [tg::Either::Left(client), tg::Either::Right(session)];
	let input = || {
		let arg = Arg {
			lease: None,
			location: None,
			mode: Mode::Run,
			process: tg::Either::Right(tg::process::Id::new()),
			reads: BTreeMap::new(),
			tokens: tg::Tokens::default(),
		};
		let request = ClientRequest {
			arg: ClientRequestArg::Connect(arg),
			id: 0,
		};
		futures::stream::iter([Ok(ClientMessage::Request(request))]).boxed()
	};
	for handle in handles {
		let handle = tg::handle::dynamic::Handle::new(handle);
		let output =
			tokio::time::timeout(Duration::from_secs(5), handle.try_connect_process(input()))
				.await
				.unwrap()
				.unwrap();
		assert!(output.is_none());
		let result = tokio::time::timeout(Duration::from_secs(5), handle.connect_process(input()))
			.await
			.unwrap();
		assert!(result.is_err());
	}
}

#[tokio::test]
async fn reconnect_preserves_the_process_and_read_cursor() {
	tokio::time::timeout(Duration::from_secs(10), async {
		let (client, connections, _server) = mock_server(http::StatusCode::OK).await;
		let id = tg::process::Id::new();
		let server_id = id.clone();
		let driver = tokio::spawn(async move {
			for attempt in 0..3 {
				let mut connection = connections.recv().await.unwrap();
				let arg = connection.connect(&server_id).await;
				if attempt == 0 {
					assert_eq!(arg.reads[&1].streams, [stdio::Stream::Stderr]);
					let request = connection.request().await;
					assert!(matches!(request.arg, ClientRequestArg::Close(1)));
					connection
						.respond(request.id, ServerResponseOutput::Close)
						.await;
				} else {
					assert_eq!(arg.reads[&1].streams, [stdio::Stream::Stdout]);
					assert_eq!(
						arg.reads[&1].position.unwrap_or(SeekFrom::Start(0)),
						SeekFrom::Start(attempt - 1)
					);
					let chunk = stdio::Chunk {
						bytes: bytes::Bytes::from_static(if attempt == 1 { b"a" } else { b"b" }),
						combined_position: attempt - 1,
						stream: stdio::Stream::Stdout,
						stream_position: attempt - 1,
						timestamp: None,
					};
					let notification = ReadServerNotification {
						event: read::Event::Chunk(chunk),
						id: 1,
					};
					connection
						.output
						.send(Ok(ServerMessage::Notification(ServerNotification::Read(
							notification,
						))))
						.await
						.unwrap();
					if attempt == 2 {
						let end = stdio::End {
							combined_position: 2,
							stream_positions: [(stdio::Stream::Stdout, 2)].into(),
						};
						connection
							.respond(1, ServerResponseOutput::Read(read::Output::End(end)))
							.await;
					}
				}
				let output = tg::process::wait::Output {
					error: None,
					exit: 0,
					output: None,
				};
				connection
					.output
					.send(Ok(ServerMessage::Notification(ServerNotification::Wait(
						output,
					))))
					.await
					.unwrap();
				// Ending the response body closes this physical connection while preserving buffered messages.
			}
		});
		let options = Options {
			reads: vec![read::Options {
				streams: vec![stdio::Stream::Stderr],
				..Default::default()
			}],
			..Default::default()
		};
		let process = tg::Process::<tg::Value>::connect_with_handle(&client, id, options)
			.await
			.unwrap();
		process.stderr().close().await.unwrap();
		assert_eq!(
			process
				.wait_with_handle(&client, tg::process::wait::Options::default())
				.await
				.unwrap()
				.exit,
			0
		);
		let options = read::Options {
			streams: vec![stdio::Stream::Stdout],
			..Default::default()
		};
		let chunks = process
			.try_read_stdio_with_handle(&client, options)
			.await
			.unwrap()
			.unwrap()
			.try_collect::<Vec<_>>()
			.await
			.unwrap();
		assert_eq!(
			chunks
				.into_iter()
				.flat_map(|chunk| chunk.bytes.to_vec())
				.collect::<Vec<_>>(),
			b"ab"
		);
		driver.await.unwrap();
	})
	.await
	.unwrap();
}

#[tokio::test]
async fn reconnect_resends_only_unconfirmed_writes() {
	tokio::time::timeout(Duration::from_secs(10), async {
		let (client, connections, _server) = mock_server(http::StatusCode::OK).await;
		let id = tg::process::Id::new();
		let server_id = id.clone();
		let driver = tokio::spawn(async move {
			for attempt in 0..2 {
				let mut connection = connections.recv().await.unwrap();
				connection.connect(&server_id).await;
				let request = connection.request().await;
				let ClientRequestArg::Write(stdio::write::Arg {
					data: stdio::write::Data::Chunk(chunk),
					..
				}) = request.arg
				else {
					panic!("expected the write request");
				};
				assert_eq!(chunk.bytes, b"abc".as_slice());
				assert_eq!(chunk.stream_position, 0);
				assert_eq!(chunk.combined_position, 0);
				if attempt == 0 {
					connection
						.output
						.send(Ok(ServerMessage::Ack(Ack { id: request.id })))
						.await
						.unwrap();
					continue;
				}
				let output = stdio::write::Output {
					closed: false,
					length: 3,
				};
				connection
					.respond(request.id, ServerResponseOutput::Write(output))
					.await;
				let request = connection.request().await;
				let ClientRequestArg::Write(stdio::write::Arg {
					data: stdio::write::Data::End(end),
					..
				}) = request.arg
				else {
					panic!("expected the EOF request");
				};
				assert_eq!(end.combined_position, 3);
				assert_eq!(end.stream_positions[&stdio::Stream::Stdin], 3);
				let output = stdio::write::Output {
					closed: true,
					length: 0,
				};
				connection
					.respond(request.id, ServerResponseOutput::Write(output))
					.await;
			}
		});
		let process =
			tg::Process::<tg::Value>::connect_with_handle(&client, id, Options::default())
				.await
				.unwrap();
		let mut stdin = process.stdin();
		assert_eq!(stdin.write_with_handle(&client, b"abc").await.unwrap(), 3);
		stdin.close_with_handle(&client).await.unwrap();
		driver.await.unwrap();
	})
	.await
	.unwrap();
}

#[test]
fn opening_metadata_preserves_read_options() {
	let read = tg::process::stdio::read::Arg {
		length: Some(-7),
		position: Some(SeekFrom::End(-3)),
		size: Some(1024),
		streams: vec![tg::process::stdio::Stream::Stdout],
		timeout: Some(Duration::new(2, 1)),
		..Default::default()
	};
	let id = tg::process::Id::new();
	let arg = Arg {
		lease: Some("lease".to_owned()),
		location: Some("remote:test".parse().unwrap()),
		mode: Mode::Run,
		process: tg::Either::Right(id.clone()),
		reads: BTreeMap::from([(1, read)]),
		tokens: tg::Tokens::default(),
	};
	let json = serde_json::to_value(&arg).unwrap();
	assert_eq!(json["process"], id.to_string());
	assert_eq!(json["mode"], "run");
	assert_eq!(json["lease"], "lease");
	assert_eq!(json["location"], "remote:test");
	assert!(json.get("target").is_none());
	let request = ClientRequest {
		arg: ClientRequestArg::Connect(arg),
		id: 0,
	};
	let message = ClientMessage::Request(request);
	assert_roundtrip(&message);
}

#[test]
fn spawn_metadata_uses_native_types() {
	let arg = serde_json::json!({
		"cache_location": "local",
		"cached": false,
		"command": {"node": {
			"args": [
				{"kind": "string", "value": "hello"},
				{"kind": "value", "value": null}
			],
			"cwd": "/tmp",
			"env": {"EXAMPLE": {"kind": "value", "value": true}},
			"executable": {"node": {"path": "/bin/echo"}},
			"host": "aarch64-darwin",
			"user": "test"
		}},
		"debug": {"addr": "127.0.0.1:9229", "mode": "wait"},
		"location": "remote:test",
		"parent": tg::process::Id::new(),
		"public": true,
		"retry": true,
		"sandbox": {
			"cpu": 2,
			"host": "aarch64-darwin",
			"hostname": "test",
			"isolation": {"kind": "seatbelt"},
			"location": "local",
			"memory": 1_048_576,
			"network": {"kind": "host"},
			"ttl": 60.125
		},
		"stderr": "log",
		"stdin": "pipe",
		"stdout": "pipe",
		"tty": {"size": {"cols": 80, "rows": 24}}
	});
	let mut arg: tg::process::spawn::Arg = serde_json::from_value(arg).unwrap();
	for mode in [Mode::Run, Mode::Spawn] {
		let request = Arg {
			lease: None,
			location: Some("remote:test".parse().unwrap()),
			mode,
			process: tg::Either::Left(Box::new(arg.clone())),
			reads: BTreeMap::new(),
			tokens: tg::Tokens::default(),
		};
		let json = serde_json::to_value(&request).unwrap();
		assert_eq!(json["process"], serde_json::to_value(&arg).unwrap());
		assert_eq!(json["mode"], serde_json::to_value(mode).unwrap());
		assert!(json.get("target").is_none());
		assert_roundtrip(&ClientRequestArg::Connect(request));
		arg.command.node = tg::Either::Right(tg::command::Id::new(b"command"));
		arg.sandbox = Some(tg::Either::Right(tg::sandbox::Id::new()));
		arg.tty = Some(tg::Either::Left(true));
	}
}

#[test]
fn operation_payloads_are_native_structs() {
	let args = [
		serde_json::json!({"kind": "cancel", "value": {"lease": "lease", "location": "local"}}),
		serde_json::json!({"kind": "read", "value": {"position": "current.5", "streams": "stdout,stderr"}}),
		serde_json::json!({"kind": "signal", "value": {"signal": "TERM"}}),
		serde_json::json!({"kind": "tty", "value": {"size": {"cols": 80, "rows": 24}}}),
		serde_json::json!({"kind": "write", "value": {"data": {"kind": "end", "value": {"combined_position": 7, "stream_positions": {"stdin": 7}}}}}),
	];
	for arg in args {
		let arg: ClientRequestArg = serde_json::from_value(arg).unwrap();
		assert_roundtrip(&arg);
		let bytes = tangram_serialize::to_vec(&arg).unwrap();
		let tangram_serialize::Value::Enum(value) = tangram_serialize::from_slice(&bytes).unwrap()
		else {
			panic!("expected a native enum");
		};
		assert!(matches!(*value.value, tangram_serialize::Value::Struct(_)));
	}
}

#[test]
fn progress_preserves_variants() {
	let events = [
		serde_json::json!({"kind": "diagnostic", "value": {"message": "diagnostic", "severity": "warning"}}),
		serde_json::json!({"kind": "indicators", "value": [
			{"current": 1, "format": "bytes", "name": "download", "title": "Downloading", "total": null},
			{"current": null, "format": "normal", "name": "build", "title": "Building", "total": 2}
		]}),
		serde_json::json!({"kind": "log", "value": {"level": null, "message": "log"}}),
		serde_json::json!({"kind": "log", "value": {"level": "error", "message": "error"}}),
		serde_json::json!({"kind": "log", "value": {"level": "info", "message": "info"}}),
		serde_json::json!({"kind": "log", "value": {"level": "success", "message": "success"}}),
		serde_json::json!({"kind": "log", "value": {"level": "warning", "message": "warning"}}),
		serde_json::json!({"kind": "output", "value": null}),
	];
	for event in events {
		let event: tg::progress::Event<()> = serde_json::from_value(event).unwrap();
		assert_roundtrip(&ServerNotification::Progress(event));
	}
}

#[test]
fn responses_preserve_errors_and_optional_null_outputs() {
	let error = tg::error::Data {
		message: Some("failed".to_owned()),
		..Default::default()
	};
	let errors = [
		None,
		Some(tg::Either::Left(error)),
		Some(tg::Either::Right(tg::Referent::with_node(
			tg::error::Id::new(b"error"),
		))),
	];
	for error in errors {
		for value in [None, Some(tg::value::Data::Null)] {
			let wait = tg::process::wait::Output {
				error: error.clone(),
				exit: 1,
				output: value,
			};
			let decoded = assert_roundtrip(&wait);
			assert_eq!(decoded.output.is_some(), wait.output.is_some());
			assert_roundtrip(&ServerNotification::Wait(wait.clone()));
			let output = tg::process::spawn::Output {
				cached: true,
				lease: Some("lease".to_owned()),
				location: Some("remote:test".parse().unwrap()),
				process: tg::Either::Right(tg::process::Id::new()),
				tokens: tg::Tokens::default(),
				wait: Some(wait),
			};
			assert_roundtrip(&ServerResponseOutput::Connect(output));
		}
	}
	let output = tg::process::cancel::Output { released: true };
	assert_roundtrip(&ServerResponseOutput::Cancel(output));
}

#[test]
fn stdio_keeps_binary_bytes_and_eof_positions() {
	let chunk = tg::process::stdio::Chunk {
		bytes: bytes::Bytes::from_static(&[0, 255, 42]),
		combined_position: 42,
		stream: tg::process::stdio::Stream::Stdin,
		stream_position: 42,
		timestamp: None,
	};
	let messages = [
		tg::process::stdio::write::Data::Chunk(chunk),
		tg::process::stdio::write::Data::End(tg::process::stdio::End {
			combined_position: 45,
			stream_positions: [(tg::process::stdio::Stream::Stdin, 45)].into(),
		}),
	];
	for message in messages {
		let arg = tg::process::stdio::write::Arg {
			data: message,
			location: None,
			tokens: tg::Tokens::default(),
		};
		let request = ClientRequest {
			arg: ClientRequestArg::Write(arg),
			id: 9,
		};
		let message = ClientMessage::Request(request);
		assert_roundtrip(&message);
	}
}

#[test]
fn read_completion_preserves_and_validates_positions() {
	let end = End {
		combined_position: 12,
		stream_positions: [(Stream::Stderr, 5), (Stream::Stdout, 7)].into(),
	};
	let output = Output::End(end);
	assert_roundtrip(&output);
	assert!(output.validate(&[Stream::Stdout], 7).is_ok());
	assert!(output.validate(&[Stream::Stderr], 5).is_ok());
	assert!(
		output
			.validate(&[Stream::Stdout, Stream::Stderr], 12)
			.is_ok()
	);
	assert!(output.validate(&[Stream::Stdout], 6).is_err());
	assert!(
		output
			.validate(&[Stream::Stdout, Stream::Stderr], 11)
			.is_err()
	);
	assert!(output.validate(&[Stream::Stdin], 0).is_err());
	assert!(output.validate(&[Stream::Stdout], 100).is_ok());
	for output in [
		Output::Limit { position: 7 },
		Output::Timeout { position: 7 },
	] {
		assert_roundtrip(&output);
		assert!(output.validate(&[Stream::Stdout], 7).is_ok());
		assert!(output.validate(&[Stream::Stdout], 6).is_err());
		assert!(output.validate(&[Stream::Stdout], 8).is_err());
	}
}

fn assert_roundtrip<T>(value: &T) -> T
where
	T: serde::Serialize
		+ tangram_serialize::Serialize
		+ for<'de> tangram_serialize::Deserialize<'de>,
{
	let bytes = tangram_serialize::to_vec(value).unwrap();
	let decoded: T = tangram_serialize::from_slice(&bytes).unwrap();
	assert_eq!(
		serde_json::to_value(value).unwrap(),
		serde_json::to_value(&decoded).unwrap()
	);
	assert_eq!(bytes, tangram_serialize::to_vec(&decoded).unwrap());
	decoded
}

async fn mock_server(
	status: http::StatusCode,
) -> (
	tg::Client,
	async_channel::Receiver<MockConnection>,
	tangram_futures::task::Task<()>,
) {
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
	let url = format!("http://{}", listener.local_addr().unwrap())
		.parse()
		.unwrap();
	let (sender, receiver) = async_channel::unbounded();
	let server = tangram_futures::task::Task::spawn(move |_| async move {
		let mut tasks = tokio::task::JoinSet::new();
		while let Ok((socket, _)) = listener.accept().await {
			let sender = sender.clone();
			let service =
				hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
					let sender = sender.clone();
					async move {
						assert_eq!(request.uri().path(), "/processes/connect");
						if !status.is_success() {
							let response = http::Response::builder()
								.status(status)
								.body(tangram_http::body::Boxed::empty())
								.unwrap();
							return Ok(response);
						}
						let body = tangram_http::body::Boxed::new(request.into_body());
						let input = stdio::decode(body, 1024 * 1024).boxed();
						let (output, receiver) = async_channel::unbounded();
						let connection = MockConnection { input, output };
						sender.send(connection).await.unwrap();
						let body = stdio::encode(receiver.boxed(), 1024 * 1024);
						let response = http::Response::builder()
							.header(http::header::CONTENT_TYPE, TANGRAM_CONTENT_TYPE)
							.body(body)
							.unwrap();
						Ok::<_, std::convert::Infallible>(response)
					}
				});
			tasks.spawn(async move {
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
	(client, receiver, server)
}
