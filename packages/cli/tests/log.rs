use {
	futures::{
		SinkExt as _, StreamExt as _,
		channel::{mpsc, oneshot},
		stream::BoxStream,
	},
	std::{
		collections::BTreeMap,
		io::SeekFrom,
		ops::ControlFlow,
		sync::{Arc, Mutex},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_tests_util::{Config, Temp, cli::Server},
};

/// The lease that the tests' runner holds for the processes that it starts.
const LEASE: &str = "06d0000001r010000000000000";

/// A process's finish is acknowledged before the header is returned, and every header carries a sync.
#[tokio::test]
async fn control_early_finish() -> tg::Result<()> {
	for source in [
		r#"export default () => tg.file("control output");"#,
		r#"export default () => { throw new Error("control error"); }"#,
	] {
		// Start a server with checkpoints, and build a process that finishes.
		let mut config = Config::new();
		config.merge(serde_json::json!({ "advanced": { "checkpoints": true } }));
		let server = Server::start(env!("CARGO_BIN_EXE_tangram"), config).await?;
		let package = package(source)?;
		let submitted = watch(&server, "process.control.finish.submitted").await?;
		let parent = build(&server, &package).await?;
		server
			.tg(
				None,
				[
					"checkpoint",
					"wait",
					"--timeout",
					"30s",
					"process.control.finish.submitted",
					&submitted,
					"0",
				],
			)
			.await?;
		server
			.tg(
				None,
				[
					"checkpoint",
					"unwatch",
					"process.control.finish.submitted",
					&submitted,
				],
			)
			.await?;
		let finished = server
			.get::<tg::process::Data>(None, &parent.to_string())
			.await?;
		assert!(matches!(finished.status, tg::process::Status::Finished));

		// Start a process whose control stream sends its finish before the header is returned.
		let header_watch = watch(&server, "process.control.header").await?;
		let finish_watch = watch(&server, "process.control.finish").await?;
		let finished = finished_data(finished);
		let finish = tg::process::control::ClientRequestArg::Finish(
			tg::process::control::FinishClientRequestArg {
				data: finished.clone(),
			},
		);
		let request = tg::process::control::ClientRequest {
			arg: finish.clone(),
			id: "early-finish".to_owned(),
		};
		let message = tg::process::control::ClientMessage::Request(request);
		let start = start(&parent, &finished)?;
		let client = server.client()?;
		let id = tg::process::Id::new();
		let pending = tokio::spawn({
			let id = id.clone();
			let data = started_data(&finished);
			async move { Control::connect(client, None, id, start, data, vec![message]).await }
		});

		// Wait for the request body to reach the server before the header is returned.
		for (name, watch) in [
			("process.control.header", &header_watch),
			("process.control.finish", &finish_watch),
		] {
			server
				.tg(None, ["checkpoint", "wait", name, watch, "0"])
				.await?;
		}
		server
			.tg(
				None,
				[
					"checkpoint",
					"unwatch",
					"process.control.header",
					&header_watch,
				],
			)
			.await?;
		let (mut control, header) = tokio::time::timeout(Duration::from_secs(10), pending)
			.await
			.map_err(|_| tg::error!("the header should be returned"))?
			.map_err(|error| tg::error!(!error, "the connect task panicked"))??;
		let token = header
			.token
			.ok_or_else(|| tg::error!("expected the process's token"))?;
		control.receive_ack("early-finish").await?;
		control.close();
		server
			.tg(
				None,
				[
					"checkpoint",
					"unwatch",
					"process.control.finish",
					&finish_watch,
				],
			)
			.await?;

		// End the empty log.
		write(&server, &id, &token, end(0, 0, 0)).await?;

		// Reconnect after losing the acknowledged finish, which must not initialize the process again.
		let started_watch = watch(&server, "process.control.index.started").await?;
		let resume = tg::process::control::Mode::Resume {
			lease: LEASE.parse()?,
		};
		let connected = Control::connect(
			server.client()?,
			Some(&token),
			id.clone(),
			resume,
			finished.clone(),
			Vec::new(),
		)
		.await;
		server
			.tg(
				None,
				[
					"checkpoint",
					"unwatch",
					"process.control.index.started",
					&started_watch,
				],
			)
			.await?;
		let (mut control, _) = connected?;
		control.request("early-finish", finish).await?;
		control.close();

		// The finished process's output or error carries a token for its sync.
		let output = server.tg(None, ["get", &id.to_string()]).await?;
		let process = serde_json::from_str::<serde_json::Value>(&output)
			.map_err(|error| tg::error!(!error, "failed to deserialize the process"))?;
		assert_eq!(process["status"], "finished");
		let value = if process["output"].is_null() {
			&process["error"]
		} else {
			&process["output"]
		};
		let mut readable = false;
		for token in tokens(value) {
			let body = server
				.tg(None, ["authorization", "token", "decode", &token])
				.await?;
			let body = serde_json::from_str::<serde_json::Value>(&body)
				.map_err(|error| tg::error!(!error, "failed to deserialize the token"))?;
			let resource = body["resource"].as_str().unwrap_or_default();
			let permissions = body["permissions"].as_array().cloned().unwrap_or_default();
			if resource.starts_with("syn_")
				&& permissions
					.iter()
					.any(|permission| permission == "sync_read")
			{
				readable = true;
			}
		}
		assert!(
			readable,
			"the process's value should carry a sync read token: {value}"
		);

		server.stop().await?;
	}

	Ok(())
}

/// A process log reader can follow output as the log grows.
#[tokio::test]
async fn log_growing() -> tg::Result<()> {
	let (server, parent, finished) = finished_process().await?;
	for added in [46, 96] {
		let (control, id, token) = start_process(&server, &parent, &finished).await?;
		let mut control = control;
		write(
			&server,
			&id,
			&token,
			chunk(tg::process::stdio::Stream::Stdout, 0, b"abcd", 0),
		)
		.await?;
		let arg = read_arg(
			vec![tg::process::stdio::Stream::Stdout],
			Some(SeekFrom::End(96)),
			Some(-99),
			Some(1),
		);
		let mut reader = reader(&server, &id, arg).await?;

		// Wait for the seek to resolve before changing the log.
		let event = read(&mut reader, true).await?;
		assert_position(&event, -99, 100)?;
		let bytes = vec![b'x'; added];
		write(
			&server,
			&id,
			&token,
			chunk(tg::process::stdio::Stream::Stdout, 4, &bytes, 4),
		)
		.await?;
		control.request("finish", finish(&finished)).await?;
		let combined = 4 + u64::try_from(added).unwrap();
		write(&server, &id, &token, end(combined, 0, combined)).await?;
		if added < 96 {
			let event = read(&mut reader, true).await?;
			let length = -(3 + i64::try_from(added).unwrap());
			assert_position(&event, length, combined)?;
		}
		let mut output = Vec::new();
		loop {
			match read(&mut reader, false).await? {
				tg::process::stdio::read::ServerMessage::Response(_) => break,
				tg::process::stdio::read::ServerMessage::Notification(
					tg::process::stdio::read::Event::Chunk(chunk),
				) => output.extend_from_slice(&chunk.bytes),
				message @ tg::process::stdio::read::ServerMessage::Notification(
					tg::process::stdio::read::Event::Position { .. },
				) => return Err(tg::error!(?message, "unexpected message")),
			}
		}
		let mut expected = bytes;
		expected.extend_from_slice(b"dcb");
		assert_eq!(output, expected);
		control.close();
	}

	server.stop().await?;

	Ok(())
}

/// A process log stream resumes after its connection is interrupted.
#[tokio::test]
async fn log_reconnect() -> tg::Result<()> {
	let (server, parent, finished) = finished_process().await?;
	let (mut control, id, token) = start_process(&server, &parent, &finished).await?;
	let id_string = id.to_string();
	let stdout = tg::process::stdio::Stream::Stdout;
	let stderr = tg::process::stdio::Stream::Stderr;

	// Rewrite a chunk after losing its position.
	let first = chunk(stdout, 0, b"hello\n", 0);
	assert_written(write(&server, &id, &token, first.clone()).await?, false, 6);
	assert_written(write(&server, &id, &token, first).await?, false, 6);

	// An unfinished log must not clip a reverse cursor to the currently persisted prefix.
	let output = server
		.output(
			None,
			["log", "--position", "12", "--length=-12", &id_string],
		)
		.await?;
	assert!(
		output.status.success() && output.stdout.is_empty() && output.stderr.is_empty(),
		"{output:?}"
	);

	// Rewrite a persisted chunk without duplicating it.
	let second = chunk(stderr, 6, b"world\n", 0);
	assert_written(
		write(&server, &id, &token, second.clone()).await?,
		false,
		12,
	);
	assert_written(write(&server, &id, &token, second).await?, false, 12);

	// The writer can supply the end before the finish is processed, and repeat it after.
	assert_written(write(&server, &id, &token, end(12, 6, 6)).await?, true, 12);
	control.request("finish", finish(&finished)).await?;
	control.close();
	assert_written(write(&server, &id, &token, end(12, 6, 6)).await?, true, 12);
	let output = server
		.output(None, ["log", "--no-timeout", &id_string])
		.await?;
	assert!(output.status.success(), "{output:?}");
	assert_eq!(output.stdout, b"hello\n");
	assert_eq!(output.stderr, b"world\n");

	server.stop().await?;

	Ok(())
}

/// Process log chunks are returned in stream order.
#[tokio::test]
async fn log_reordered() -> tg::Result<()> {
	let (server, parent, finished) = finished_process().await?;
	let stdout = tg::process::stdio::Stream::Stdout;
	let stderr = tg::process::stdio::Stream::Stderr;
	for last in [stdout, stderr] {
		let (mut control, id, token) = start_process(&server, &parent, &finished).await?;
		let id_string = id.to_string();

		// Replay the last chunk before the missing initial and middle chunks.
		let last_position = if last == stdout { 2 } else { 0 };
		write(&server, &id, &token, chunk(last, 2, b"C", last_position)).await?;
		let output = server
			.output(None, ["log", "--timeout", "0", &id_string])
			.await?;
		assert!(
			output.status.success() && output.stdout.is_empty() && output.stderr.is_empty(),
			"{output:?}"
		);
		write(&server, &id, &token, chunk(stdout, 0, b"A", 0)).await?;

		// Snapshot reads stop at the gap, including when both entries have the same stream.
		for streams in [vec![], vec!["--stream", "stdout"]] {
			let mut args = vec!["log", "--timeout", "0"];
			args.extend(streams);
			args.push(&id_string);
			let output = server.output(None, args).await?;
			assert!(
				output.status.success() && output.stdout == b"A" && output.stderr.is_empty(),
				"{output:?}"
			);
		}

		// A reverse read must not return a prefix separated from its cursor by a gap.
		let output = server
			.output(
				None,
				[
					"log",
					"--timeout",
					"0",
					"--position",
					"3",
					"--length=-3",
					&id_string,
				],
			)
			.await?;
		assert!(
			output.status.success() && output.stdout.is_empty() && output.stderr.is_empty(),
			"{output:?}"
		);

		// A live reader retains its position and resumes when the middle chunk arrives.
		let arg = read_arg(vec![stdout, stderr], None, None, None);
		let mut reader = reader(&server, &id, arg).await?;
		let tg::process::stdio::read::ServerMessage::Notification(
			tg::process::stdio::read::Event::Chunk(first),
		) = read(&mut reader, false).await?
		else {
			return Err(tg::error!("expected a chunk"));
		};
		assert_eq!(first.bytes.as_ref(), b"A");
		write(&server, &id, &token, chunk(stdout, 1, b"B", 1)).await?;
		control.request("finish", finish(&finished)).await?;
		let (stderr_position, stdout_position) = if last == stdout { (0, 3) } else { (1, 2) };
		write(
			&server,
			&id,
			&token,
			end(3, stderr_position, stdout_position),
		)
		.await?;
		let mut output = BTreeMap::from([(stdout, b"A".to_vec()), (stderr, Vec::new())]);
		let mut position = 1;
		loop {
			let chunk = match read(&mut reader, false).await? {
				tg::process::stdio::read::ServerMessage::Response(_) => break,
				tg::process::stdio::read::ServerMessage::Notification(
					tg::process::stdio::read::Event::Chunk(chunk),
				) => chunk,
				message @ tg::process::stdio::read::ServerMessage::Notification(
					tg::process::stdio::read::Event::Position { .. },
				) => return Err(tg::error!(?message, "unexpected message")),
			};
			assert_eq!(chunk.combined_position, position);
			let bytes = output.get_mut(&chunk.stream).unwrap();
			assert_eq!(chunk.stream_position, u64::try_from(bytes.len()).unwrap());
			bytes.extend_from_slice(&chunk.bytes);
			position += u64::try_from(chunk.bytes.len()).unwrap();
		}
		let (expected_stdout, expected_stderr): (&[u8], &[u8]) = if last == stdout {
			(b"ABC", b"")
		} else {
			(b"AB", b"C")
		};
		assert_eq!(output[&stdout], expected_stdout);
		assert_eq!(output[&stderr], expected_stderr);
		let output = server
			.output(None, ["log", "--no-timeout", &id_string])
			.await?;
		assert!(output.status.success(), "{output:?}");
		assert_eq!(
			(output.stdout.as_slice(), output.stderr.as_slice()),
			(expected_stdout, expected_stderr)
		);
		let output = server
			.output(
				None,
				[
					"log",
					"--no-timeout",
					"--position",
					"3",
					"--length=-3",
					&id_string,
				],
			)
			.await?;
		assert!(output.status.success(), "{output:?}");
		let expected_reverse: &[u8] = if last == stdout { b"CBA" } else { b"BA" };
		assert_eq!(
			(output.stdout.as_slice(), output.stderr.as_slice()),
			(expected_reverse, expected_stderr)
		);
		control.close();
	}

	server.stop().await?;

	Ok(())
}

/// A process control stream that answers the server's get requests while the test waits on other work.
struct Control {
	data: Arc<Mutex<tg::process::Data>>,
	events: mpsc::UnboundedReceiver<tg::Result<tg::process::control::ServerMessage>>,
	sender: mpsc::UnboundedSender<tg::Result<tg::process::control::ClientMessage>>,
	task: tokio::task::JoinHandle<()>,
}

impl Control {
	/// Open a process control stream that sends the messages before the header is returned.
	async fn connect(
		client: tg::Client,
		token: Option<&str>,
		id: tg::process::Id,
		mode: tg::process::control::Mode,
		data: tg::process::Data,
		messages: Vec<tg::process::control::ClientMessage>,
	) -> tg::Result<(Self, tg::process::control::Header)> {
		// Open the stream.
		let (sender, receiver) = mpsc::unbounded();
		for message in messages {
			sender
				.unbounded_send(Ok(message))
				.map_err(|error| tg::error!(!error, "failed to send the message"))?;
		}
		let session = client.session(&tg::Context::new(token.map(ToOwned::to_owned)));
		let location = tg::Location::Local(tg::location::Local::default());
		let arg = tg::process::control::Arg {
			id,
			location: Some(location.into()),
			mode,
			proof: None,
		};
		let (header, stream) = match session
			.try_get_process_control_stream(arg, receiver.boxed())
			.await?
		{
			ControlFlow::Break(Some(output)) => output,
			ControlFlow::Break(None) => {
				return Err(tg::error!("the process control stream was not found"));
			},
			ControlFlow::Continue(error) => return Err(error),
		};

		// Answer the server's get requests, and pass the other messages to the test.
		let data = Arc::new(Mutex::new(data));
		let (events_sender, events) = mpsc::unbounded();
		let task = tokio::spawn({
			let data = data.clone();
			let sender = sender.clone();
			async move {
				let mut stream = std::pin::pin!(stream);
				while let Some(message) = stream.next().await {
					if let Ok(tg::process::control::ServerMessage::Request(request)) = &message
						&& let tg::process::control::ServerRequestArg::Get(_) = request.arg
					{
						let data = data.lock().unwrap().clone();
						let output =
							tg::process::control::GetClientResponseOutput { data, sync: None };
						let response = tg::process::control::ClientResponse {
							error: None,
							id: request.id.clone(),
							output: Some(tg::process::control::ClientResponseOutput::Get(output)),
						};
						sender
							.unbounded_send(Ok(tg::process::control::ClientMessage::Response(
								response,
							)))
							.ok();
						continue;
					}
					if events_sender.unbounded_send(message).is_err() {
						break;
					}
				}
			}
		});
		let control = Self {
			data,
			events,
			sender,
			task,
		};

		Ok((control, header))
	}

	/// Send a request, wait for its response, and acknowledge it.
	async fn request(
		&mut self,
		id: &str,
		arg: tg::process::control::ClientRequestArg,
	) -> tg::Result<()> {
		if let tg::process::control::ClientRequestArg::Finish(finish) = &arg {
			*self.data.lock().unwrap() = finish.data.clone();
		}
		let request = tg::process::control::ClientRequest {
			arg,
			id: id.to_owned(),
		};
		self.sender
			.send(Ok(tg::process::control::ClientMessage::Request(request)))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		let response = loop {
			if let tg::process::control::ServerMessage::Response(response) = self.receive().await?
				&& response.id == id
			{
				break response;
			}
		};
		if let Some(error) = response.error {
			return Err(tg::error!(?error, "the request failed"));
		}
		let ack = tg::process::control::ClientAck { id: id.to_owned() };
		self.sender
			.send(Ok(tg::process::control::ClientMessage::Ack(ack)))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the ack"))?;

		Ok(())
	}

	/// Wait for the server's acknowledgement of a request.
	async fn receive_ack(&mut self, id: &str) -> tg::Result<()> {
		loop {
			if let tg::process::control::ServerMessage::Ack(ack) = self.receive().await?
				&& ack.id == id
			{
				return Ok(());
			}
		}
	}

	async fn receive(&mut self) -> tg::Result<tg::process::control::ServerMessage> {
		let message = tokio::time::timeout(Duration::from_secs(10), self.events.next())
			.await
			.map_err(|_| tg::error!("timed out waiting for a control message"))?
			.ok_or_else(|| tg::error!("the control stream ended"))??;
		Ok(message)
	}

	/// Close the stream, as a runner that loses its connection would.
	fn close(self) {
		self.task.abort();
	}
}

/// Build a process that finishes, and return the server, the process, and its data.
async fn finished_process() -> tg::Result<(Server, tg::process::Id, tg::process::Data)> {
	let server = Server::start(env!("CARGO_BIN_EXE_tangram"), Config::new()).await?;
	let package = package("export default function () {}")?;
	let parent = build(&server, &package).await?;
	server.tg(None, ["wait", &parent.to_string()]).await?;
	let finished = server
		.get::<tg::process::Data>(None, &parent.to_string())
		.await?;
	let finished = finished_data(finished);

	Ok((server, parent, finished))
}

/// Start a shortcut child of the parent as the root, and return its control stream, ID, and token.
async fn start_process(
	server: &Server,
	parent: &tg::process::Id,
	finished: &tg::process::Data,
) -> tg::Result<(Control, tg::process::Id, String)> {
	let id = tg::process::Id::new();
	let start = start(parent, finished)?;
	let (control, header) = Control::connect(
		server.client()?,
		None,
		id.clone(),
		start,
		started_data(finished),
		Vec::new(),
	)
	.await?;
	let token = header
		.token
		.ok_or_else(|| tg::error!("expected the process's token"))?;

	Ok((control, id, token))
}

/// Create the mode that starts a shortcut child of the parent, naming the parent with tokens that the server does not check for the root.
fn start(
	parent: &tg::process::Id,
	finished: &tg::process::Data,
) -> tg::Result<tg::process::control::Mode> {
	let shortcut = tg::process::control::Shortcut {
		parent: tg::process::control::Parent {
			id: parent.clone(),
			token: Some(String::new()),
		},
		sandbox: tg::process::control::Sandbox {
			create: None,
			token: Some(String::new()),
		},
	};
	let start = tg::process::control::StartClientRequestArg {
		data: started_data(finished),
		lease: LEASE.parse()?,
		options: tg::referent::Options::default(),
		shortcut: Some(shortcut),
	};

	Ok(tg::process::control::Mode::Start(start))
}

/// Remove the log from a finished process's data.
fn finished_data(mut data: tg::process::Data) -> tg::process::Data {
	data.log = None;
	data
}

/// Create the data of a started process from a finished process's data.
fn started_data(finished: &tg::process::Data) -> tg::process::Data {
	let mut data = finished.clone();
	data.children = None;
	data.error = None;
	data.exit = None;
	data.finished_at = None;
	data.output = None;
	data.status = tg::process::Status::Started;
	data
}

fn finish(finished: &tg::process::Data) -> tg::process::control::ClientRequestArg {
	let finish = tg::process::control::FinishClientRequestArg {
		data: finished.clone(),
	};
	tg::process::control::ClientRequestArg::Finish(finish)
}

/// Write a package with the module's source, and return its directory.
fn package(source: &str) -> tg::Result<Temp> {
	let temp = Temp::new().map_err(|error| tg::error!(!error, "failed to create the package"))?;
	std::fs::write(temp.path().join("tangram.ts"), source)
		.map_err(|error| tg::error!(!error, "failed to write the module"))?;
	Ok(temp)
}

async fn build(server: &Server, package: &Temp) -> tg::Result<tg::process::Id> {
	let path = package
		.path()
		.to_str()
		.ok_or_else(|| tg::error!("invalid path"))?;
	let output = server
		.tg(None, ["build", "--no-tokens", "--detach", path])
		.await?;
	let process = output.split('?').next().unwrap_or(&output).parse()?;
	Ok(process)
}

async fn watch(server: &Server, name: &str) -> tg::Result<String> {
	let output = server.tg(None, ["checkpoint", "watch", name]).await?;
	let output = serde_json::from_str::<serde_json::Value>(&output)
		.map_err(|error| tg::error!(!error, "failed to deserialize the watch"))?;
	let watch = output["watch"]
		.as_u64()
		.ok_or_else(|| tg::error!("expected the watch"))?
		.to_string();
	Ok(watch)
}

/// Get the tokens in a process's output or error, which is a referent string with tokens in its query or an object with tokens in its options.
fn tokens(value: &serde_json::Value) -> Vec<String> {
	let mut tokens = Vec::new();
	match value {
		serde_json::Value::Object(object) => {
			if let Some(map) = object
				.get("options")
				.and_then(|options| options.get("tokens"))
				.and_then(serde_json::Value::as_object)
			{
				for entry in map.values() {
					for token in entry.as_array().into_iter().flatten() {
						tokens.extend(token.as_str().map(ToOwned::to_owned));
					}
				}
			}
		},
		serde_json::Value::String(string) => {
			let query = string
				.split_once('?')
				.map(|(_, query)| query)
				.unwrap_or_default();
			for parameter in query.split('&') {
				if let Some((name, token)) = parameter.split_once('=')
					&& name.starts_with("tokens[")
				{
					tokens.push(token.to_owned());
				}
			}
		},
		_ => (),
	}
	tokens
}

fn chunk(
	stream: tg::process::stdio::Stream,
	combined_position: u64,
	bytes: &[u8],
	stream_position: u64,
) -> tg::process::stdio::write::Data {
	let chunk = tg::process::stdio::Chunk {
		bytes: bytes.to_vec().into(),
		combined_position,
		stream,
		stream_position,
		timestamp: Some(0),
	};
	tg::process::stdio::write::Data::Chunk(chunk)
}

fn end(combined_position: u64, stderr: u64, stdout: u64) -> tg::process::stdio::write::Data {
	let stream_positions = BTreeMap::from([
		(tg::process::stdio::Stream::Stderr, stderr),
		(tg::process::stdio::Stream::Stdout, stdout),
	]);
	let end = tg::process::stdio::End {
		combined_position,
		stream_positions,
	};
	tg::process::stdio::write::Data::End(end)
}

/// Write to a process's stdout and stderr on a new connection with the process's token, and return the server's first output.
async fn write(
	server: &Server,
	id: &tg::process::Id,
	token: &str,
	data: tg::process::stdio::write::Data,
) -> tg::Result<tg::process::stdio::write::Output> {
	let session = server
		.client()?
		.session(&tg::Context::new(Some(token.to_owned())));
	let arg = tg::process::stdio::write::stream::Arg {
		id: None,
		location: None,
		streams: vec![
			tg::process::stdio::Stream::Stdout,
			tg::process::stdio::Stream::Stderr,
		],
		tokens: tg::authorization::Tokens::default(),
	};
	let (_close, closed) = oneshot::channel::<()>();
	let input = futures::stream::once(async move { Ok(data) })
		.chain(until(closed))
		.boxed();
	let process = tg::Either::Right(id.clone());
	let stream = match session
		.try_write_process_stdio(&process, arg, input)
		.await?
	{
		ControlFlow::Break(Some(stream)) => stream,
		ControlFlow::Break(None) => return Err(tg::error!("the process was not found")),
		ControlFlow::Continue(error) => return Err(error),
	};
	let mut stream = std::pin::pin!(stream);
	let output = tokio::time::timeout(Duration::from_secs(10), stream.next())
		.await
		.map_err(|_| tg::error!("timed out waiting for the write's output"))?
		.ok_or_else(|| tg::error!("the write stream ended"))??;
	Ok(output)
}

fn read_arg(
	streams: Vec<tg::process::stdio::Stream>,
	position: Option<SeekFrom>,
	length: Option<i64>,
	size: Option<u64>,
) -> tg::process::stdio::read::Arg {
	tg::process::stdio::read::Arg {
		id: None,
		length,
		location: None,
		position,
		size,
		streams,
		timeout: None,
		tokens: tg::authorization::Tokens::default(),
	}
}

/// A reader of a process's log, whose request ends when it is dropped.
struct Reader {
	_close: oneshot::Sender<()>,
	stream: BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>,
}

/// Open a reader of a process's log without a token.
async fn reader(
	server: &Server,
	id: &tg::process::Id,
	arg: tg::process::stdio::read::Arg,
) -> tg::Result<Reader> {
	let session = server.client()?.session(&tg::Context::new(None));
	let process = tg::Either::Right(id.clone());
	let (close, closed) = oneshot::channel();
	let input = until(closed).boxed();
	let stream = match session.try_read_process_stdio(&process, arg, input).await? {
		ControlFlow::Break(Some(stream)) => stream,
		ControlFlow::Break(None) => return Err(tg::error!("the process was not found")),
		ControlFlow::Continue(error) => return Err(error),
	};
	let reader = Reader {
		_close: close,
		stream: stream.boxed(),
	};
	Ok(reader)
}

/// Read the reader's next message, skipping position notifications unless they are requested.
async fn read(
	reader: &mut Reader,
	positions: bool,
) -> tg::Result<tg::process::stdio::read::ServerMessage> {
	loop {
		let message = tokio::time::timeout(Duration::from_secs(10), reader.stream.next())
			.await
			.map_err(|_| tg::error!("timed out waiting for the reader"))?
			.ok_or_else(|| tg::error!("the reader ended"))??;
		if !positions
			&& matches!(
				message,
				tg::process::stdio::read::ServerMessage::Notification(
					tg::process::stdio::read::Event::Position { .. }
				)
			) {
			continue;
		}
		return Ok(message);
	}
}

/// Create a request body that ends when the sender is dropped.
fn until<T>(closed: oneshot::Receiver<()>) -> impl futures::Stream<Item = T> + Send + 'static
where
	T: Send + 'static,
{
	futures::stream::once(closed).filter_map(|_| async { None })
}

fn assert_position(
	message: &tg::process::stdio::read::ServerMessage,
	expected_length: i64,
	expected_position: u64,
) -> tg::Result<()> {
	let tg::process::stdio::read::ServerMessage::Notification(
		tg::process::stdio::read::Event::Position { length, position },
	) = message
	else {
		return Err(tg::error!(?message, "expected a position"));
	};
	assert_eq!(
		(*length, *position),
		(Some(expected_length), expected_position)
	);
	Ok(())
}

fn assert_written(output: tg::process::stdio::write::Output, closed: bool, position: u64) {
	assert_eq!((output.closed, output.position), (closed, position));
}
