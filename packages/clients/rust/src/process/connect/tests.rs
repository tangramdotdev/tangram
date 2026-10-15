use {
	super::*,
	crate::process::stdio::{self, End, Stream, read::Output},
	futures::TryStreamExt as _,
	std::time::Duration,
};

struct MockConnection {
	arg: Arg,
	// Hold the request body open for the lifetime of the connection.
	_input: BoxStream<'static, tg::Result<ClientMessage>>,
	output: async_channel::Sender<tg::Result<ServerMessage>>,
}

impl MockConnection {
	async fn connect(&mut self, id: &tg::process::Id) -> Arg {
		let arg = self.arg.clone();
		let tg::Either::Right(process_id) = &arg.process else {
			panic!("expected to connect to the existing process");
		};
		assert_eq!(process_id, id);
		assert_eq!(arg.mode, Mode::Run);
		let output = tg::process::spawn::Output {
			cached: false,
			command: None,
			lease: None,
			location: None,
			outcome: None,
			process: tg::Either::Right(id.clone()),
			tokens: tg::authorization::Tokens::default(),
		};
		self.output
			.send(Ok(ServerMessage::Notification(
				ServerNotification::Progress(tg::progress::Event::Output(output)),
			)))
			.await
			.unwrap();
		arg
	}
}

#[tokio::test]
async fn header_does_not_wait_for_input_or_process_selection() {
	let (client, connections, _server) = mock_server(http::StatusCode::OK).await;
	let id = tg::process::Id::new();
	let lease = tg::process::Lease::new();
	let arg = Arg {
		lease: Some(lease),
		location: None,
		mode: Mode::Run,
		process: tg::Either::Right(id.clone()),
		push: false,
		tokens: tg::authorization::Tokens::default(),
	};
	let (_, mut output) = tokio::time::timeout(
		Duration::from_secs(5),
		client.get_process_connect_stream(arg, futures::stream::pending().boxed()),
	)
	.await
	.unwrap()
	.unwrap();
	let mut connection = connections.recv().await.unwrap();
	assert_eq!(connection.arg.lease, Some(lease));
	connection.connect(&id).await;
	assert!(matches!(
		output.try_next().await.unwrap().unwrap(),
		ServerMessage::Notification(ServerNotification::Progress(tg::progress::Event::Output(_)))
	));
}

#[tokio::test]
async fn handles_preserve_not_found() {
	let (client, _, _server) = mock_server(http::StatusCode::NOT_FOUND).await;
	let session = client.session(&client.context);
	let handles = [tg::Either::Left(client), tg::Either::Right(session)];
	let arg = || Arg {
		lease: None,
		location: None,
		mode: Mode::Run,
		process: tg::Either::Right(tg::process::Id::new()),
		push: false,
		tokens: tg::authorization::Tokens::default(),
	};
	for instance in handles {
		let instance = tg::instance::dynamic::Instance::new(instance);
		let output = tokio::time::timeout(
			Duration::from_secs(5),
			instance.try_get_process_connect_stream(arg(), futures::stream::pending().boxed()),
		)
		.await
		.unwrap()
		.unwrap();
		assert!(output.is_none());
		let result = tokio::time::timeout(
			Duration::from_secs(5),
			instance.get_process_connect_stream(arg(), futures::stream::pending().boxed()),
		)
		.await
		.unwrap();
		assert!(result.is_err());
	}
}

#[tokio::test]
async fn reconnect_preserves_the_process() {
	tokio::time::timeout(Duration::from_secs(10), async {
		let (client, connections, _server) = mock_server(http::StatusCode::OK).await;
		let id = tg::process::Id::new();
		let server_id = id.clone();
		let driver = tokio::spawn(async move {
			for attempt in 0..2 {
				let mut connection = connections.recv().await.unwrap();
				connection.connect(&server_id).await;
				if attempt == 1 {
					let outcome = tg::process::outcome::Data {
						checksum: None,
						error: None,
						exit: 0,
						output: None,
					};
					connection
						.output
						.send(Ok(ServerMessage::Notification(
							ServerNotification::Outcome(outcome),
						)))
						.await
						.unwrap();
				}
				// Ending the response body closes this physical connection while preserving buffered messages.
			}
		});
		let process =
			tg::Process::<tg::Value>::connect_with_instance(&client, id, Options::default())
				.await
				.unwrap();
		assert_eq!(
			process
				.wait_with_instance(&client, tg::process::wait::Options::default())
				.await
				.unwrap()
				.exit,
			0
		);
		driver.await.unwrap();
	})
	.await
	.unwrap();
}

#[test]
fn opening_metadata_roundtrips() {
	let id = tg::process::Id::new();
	let lease = tg::process::Lease::new();
	let arg = Arg {
		lease: Some(lease),
		location: Some("remote:test".parse().unwrap()),
		mode: Mode::Run,
		process: tg::Either::Right(id.clone()),
		push: false,
		tokens: tg::authorization::Tokens::default(),
	};
	let json = serde_json::to_value(&arg).unwrap();
	assert_eq!(json["process"], id.to_string());
	assert_eq!(json["mode"], "run");
	assert_eq!(json["lease"], lease.to_string());
	assert_eq!(json["location"], "remote:test");
	assert!(json.get("target").is_none());
	assert_roundtrip(&arg);
}

#[tokio::test]
async fn spawn_metadata_uses_native_types() {
	let (client, connections, _server) = mock_server(http::StatusCode::OK).await;
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
			"cpu": {"max": 2, "min": 0},
			"host": "aarch64-darwin",
			"hostname": "test",
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
			push: false,
			tokens: tg::authorization::Tokens::default(),
		};
		let json = serde_json::to_value(&request).unwrap();
		assert_eq!(json["process"], serde_json::to_value(&arg).unwrap());
		assert_eq!(json["mode"], serde_json::to_value(mode).unwrap());
		assert!(json.get("target").is_none());
		assert_roundtrip(&request);
		let (_, _stream) = tokio::time::timeout(
			Duration::from_secs(5),
			client.get_process_connect_stream(request, futures::stream::pending().boxed()),
		)
		.await
		.unwrap()
		.unwrap();
		let connection = connections.recv().await.unwrap();
		assert_eq!(serde_json::to_value(&connection.arg).unwrap(), json);
		arg.command.node = tg::Either::Right(tg::command::Id::new(b"command"));
		arg.sandbox = Some(tg::Either::Right(tg::Referent::with_node(
			tg::sandbox::Id::new(),
		)));
		arg.tty = Some(tg::Either::Left(true));
	}
}

#[test]
fn operation_payloads_are_native_structs() {
	let args = [
		serde_json::json!({"kind": "cancel", "value": {"lease": "06d0000001r010000000000000", "location": "local"}}),
		serde_json::json!({"kind": "signal", "value": {"signal": "TERM"}}),
		serde_json::json!({"kind": "tty", "value": {"size": {"cols": 80, "rows": 24}}}),
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
	];
	for event in events {
		let event: tg::progress::Event<tg::process::spawn::Output> =
			serde_json::from_value(event).unwrap();
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
			let outcome = tg::process::outcome::Data {
				checksum: None,
				error: error.clone(),
				exit: 1,
				output: value,
			};
			let decoded = assert_roundtrip(&outcome);
			assert_eq!(decoded.output.is_some(), outcome.output.is_some());
			assert_roundtrip(&ServerNotification::Outcome(outcome.clone()));
			let output = tg::process::spawn::Output {
				cached: true,
				command: None,
				lease: Some(tg::process::Lease::new()),
				location: Some("remote:test".parse().unwrap()),
				outcome: Some(outcome),
				process: tg::Either::Right(tg::process::Id::new()),
				tokens: tg::authorization::Tokens::default(),
			};
			assert_roundtrip(&ServerNotification::Progress(tg::progress::Event::Output(
				output,
			)));
		}
	}
	let output = tg::process::cancel::Output { released: true };
	assert_roundtrip(&ServerResponseOutput::Cancel(output));
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
			let service = hyper::service::service_fn(
				move |mut request: http::Request<hyper::body::Incoming>| {
					let sender = sender.clone();
					async move {
						assert_eq!(request.uri().path(), "/processes/connect");
						request
							.extensions_mut()
							.insert(tangram_http::body::arg::MaxLength::default());
						if !status.is_success() {
							let response = http::Response::builder()
								.status(status)
								.body(tangram_http::body::Boxed::empty())
								.unwrap();
							return Ok(response);
						}
						let (arg, request) =
							tangram_http::request::Ext::arg_with_tangram::<Arg>(request)
								.await
								.unwrap();
						let body = tangram_http::body::Boxed::new(request.into_body());
						let input = stdio::decode(body, 1024 * 1024).boxed();
						let (output, receiver) = async_channel::unbounded();
						let connection = MockConnection {
							_input: input,
							arg: arg.unwrap(),
							output,
						};
						sender.send(connection).await.unwrap();
						let body = stdio::encode(receiver.boxed(), 1024 * 1024);
						let body = tangram_http::body::header::set(
							body,
							&Header::default(),
							tangram_http::body::encoding::Encoding::Tangram,
						)
						.unwrap();
						let response = http::Response::builder()
							.header(http::header::CONTENT_TYPE, TANGRAM_CONTENT_TYPE)
							.body(body)
							.unwrap();
						Ok::<_, std::convert::Infallible>(response)
					}
				},
			);
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
