use {
	futures::{StreamExt as _, channel::mpsc, stream::BoxStream},
	std::{ops::ControlFlow, time::Duration},
	tangram_client::prelude::*,
	tangram_tests_util::{Config, Temp, cli::Server},
};

/// A process connection follows the process lifecycle, so detaching closes it.
#[tokio::test]
async fn attach_lifecycle() -> tg::Result<()> {
	let server = Server::start(env!("CARGO_BIN_EXE_tangram"), Config::new()).await?;
	let package = package("export default async () => { await tg.sleep(60); };")?;
	let (id, lease) = build(&server, &package, &[]).await?;
	let mut attach = Attach::open(&server, None, attach_arg(&id, Some(lease), "local")?).await?;
	attach.output().await?;
	let output = attach
		.request(tg::process::attach::ClientRequestArg::Detach)
		.await?;
	assert!(matches!(
		output,
		tg::process::attach::ServerResponseOutput::Detach
	));
	attach.closed().await?;
	server
		.tg(None, ["cancel", &id.to_string(), &lease.to_string()])
		.await?;

	server.stop().await?;

	Ok(())
}

/// A local spawn that selects a remote cached process forwards the connection, and the cached process's logs remain readable.
#[tokio::test]
async fn attach_cached() -> tg::Result<()> {
	// Build a process on the remote.
	let tangram = env!("CARGO_BIN_EXE_tangram");
	let remote = Server::start(tangram, Config::new()).await?;
	let package = package(
		r#"export default () => { console.error("cached log"); return "cached output"; };"#,
	)?;
	let path = package.path().to_str().unwrap();
	let process = remote
		.tg(None, ["build", "--no-tokens", "--detach", path])
		.await?;
	let process = node(&process);
	remote.tg(None, ["wait", process]).await?;
	let data = remote.get::<tg::process::Data>(None, process).await?;
	let command = match data.command.node {
		tg::Either::Left(command) => {
			let command = serde_json::to_value(command)
				.and_then(serde_json::from_value)
				.map_err(|error| tg::error!(!error, "failed to convert the command"))?;
			tg::Either::Left(command)
		},
		tg::Either::Right(id) => tg::Either::Right(id),
	};

	// Spawn the cached process through a local server that uses the remote.
	let mut config = Config::new();
	config.merge(
		serde_json::json!({ "remotes": { "default": { "url": remote.client()?.url().to_string() } } }),
	);
	let local = Server::start(tangram, config).await?;
	let mut spawn = spawn_arg(command);
	spawn.cached = Some(true);
	spawn.stderr = tg::process::Stdio::Log;
	spawn.stdout = tg::process::Stdio::Log;
	let arg = tg::process::attach::Arg {
		lease: None,
		location: None,
		mode: tg::process::attach::Mode::Run,
		nonce: None,
		open: Vec::new(),
		process: tg::Either::Left(Box::new(spawn)),
		push: false,
		tokens: tg::authorization::Tokens::default(),
	};
	let mut attach = Attach::open(&local, None, arg).await?;
	let output = attach.output().await?;
	assert!(
		output.cached,
		"the connection should select the cached process"
	);
	let selected = output
		.process
		.right()
		.ok_or_else(|| tg::error!("expected the process's ID"))?;
	attach.closed().await?;
	let outcome = attach
		.outcome
		.take()
		.ok_or_else(|| tg::error!("expected the outcome"))?;
	assert_eq!(outcome.exit, 0);
	assert_eq!(
		serde_json::to_value(&outcome.output).unwrap(),
		serde_json::json!("cached output")
	);

	// The cached process's log is readable through the local server.
	let log = local.output(None, ["log", &selected.to_string()]).await?;
	assert!(log.status.success(), "{log:?}");
	assert_eq!(log.stderr, b"cached log\n");

	local.stop().await?;
	remote.stop().await?;

	Ok(())
}

/// A process connection can reach a process on a remote server, and only losing the owning connection cancels the process.
#[tokio::test]
async fn attach_remote() -> tg::Result<()> {
	// Start a remote with checkpoints and a local server that uses it.
	let tangram = env!("CARGO_BIN_EXE_tangram");
	let mut config = Config::new();
	config.merge(serde_json::json!({ "advanced": { "checkpoints": true } }));
	let remote = Server::start(tangram, config).await?;
	let mut config = Config::new();
	config.merge(
		serde_json::json!({ "remotes": { "default": { "url": remote.client()?.url().to_string() } } }),
	);
	let local = Server::start(tangram, config).await?;
	let package = package("export default async () => { await tg.sleep(60); };")?;

	// A confirmed detach does not cancel the process when the connection closes.
	let (id, lease) = build(&remote, &package, &[]).await?;
	let params = serde_json::json!({ "process": id.to_string() });
	let release = remote
		.watch_checkpoint(None, "process.wait.release", params)
		.await?;
	let mut attach = Attach::open(&local, None, attach_arg(&id, Some(lease), "remote")?).await?;
	attach.output().await?;
	attach
		.request(tg::process::attach::ClientRequestArg::Detach)
		.await?;
	attach.closed().await?;
	remote
		.wait_checkpoint(None, "process.wait.release", release, 0)
		.await?;
	remote
		.continue_checkpoint(None, "process.wait.release", release, 0)
		.await?;
	remote
		.unwatch_checkpoint(None, "process.wait.release", release)
		.await?;
	assert_ne!(status(&remote, &id).await?, "finished");
	remote
		.tg(None, ["cancel", &id.to_string(), &lease.to_string()])
		.await?;

	// Losing an observing connection does not release another handle's lease.
	let (id, lease) = build(&remote, &package, &["--retry"]).await?;
	let mut attach = Attach::open(&local, None, attach_arg(&id, None, "remote")?).await?;
	attach.output().await?;
	drop(attach);
	assert_ne!(status(&remote, &id).await?, "finished");

	// Losing the owning connection cancels the process on the remote.
	let mut attach = Attach::open(&local, None, attach_arg(&id, Some(lease), "remote")?).await?;
	attach.output().await?;
	drop(attach);
	let id_string = id.to_string();
	tokio::time::timeout(
		Duration::from_secs(30),
		remote.output(None, ["process", "wait", &id_string]),
	)
	.await
	.ok();
	assert_eq!(status(&remote, &id).await?, "finished");

	local.stop().await?;
	remote.stop().await?;

	Ok(())
}

/// The header, carrying the command sync, arrives before the command push completes, and requests do not block the push.
#[tokio::test]
async fn attach_command_sync_window() -> tg::Result<()> {
	// Start a scheduler without a runner, a separate runner, and a local server that routes runs through the scheduler.
	let tangram = env!("CARGO_BIN_EXE_tangram");
	let root_token = "root";
	let store = serde_json::json!({ "object_max_batch": 1 });
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"advanced": { "checkpoints": true, "multi_server": true },
		"authentication": { "root": { "token": root_token }, "users": { "providers": { "insecure": true } } },
		"roles": ["api", "indexer", "scheduler"],
		"sync": { "get": { "store": { "lmdb": store, "memory": store, "scylla": store } } },
	}));
	let remote = Server::start(tangram, config).await?;
	let remote_url = remote.client()?.url().to_string();
	let created = remote.create_runner(root_token, None).await?;
	let runner_token = created
		.token
		.token
		.clone()
		.ok_or_else(|| tg::error!("expected the runner's token"))?;
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"advanced": { "checkpoints": true },
		"remotes": { "default": { "token": runner_token, "url": remote_url } },
		"roles": ["api", "indexer", "runner"],
		"runner": { "id": created.data.id.to_string(), "remote": "default", "token": runner_token },
	}));
	let runner = Server::start(tangram, config).await?;
	let alice = remote.login("alice").await?;
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"advanced": { "checkpoints": true },
		"remotes": { "default": { "token": alice.token, "url": remote_url } },
	}));
	let local = Server::start(tangram, config).await?;

	// Store an executable command only on the local server, and hold its push and its run.
	let (command, blob) = command(&local, None).await?;
	let run = runner
		.watch_checkpoint(None, "runner.process.run", serde_json::json!({}))
		.await?;
	let params = serde_json::json!({ "id": blob });
	let store = remote
		.watch_checkpoint(Some(root_token), "sync.get.store.object", params)
		.await?;

	// Run the command through a connection that sends its requests once the header arrives.
	let count = 32;
	let (ready_sender, ready) = tokio::sync::oneshot::channel();
	let client = local.client()?;
	let task = tokio::spawn(async move {
		let mut spawn = spawn_arg(tg::Either::Right(command));
		spawn.cached = Some(false);
		let arg = tg::process::attach::Arg {
			lease: None,
			location: Some("remote".parse()?),
			mode: tg::process::attach::Mode::Run,
			nonce: None,
			open: Vec::new(),
			process: tg::Either::Left(Box::new(spawn)),
			push: false,
			tokens: tg::authorization::Tokens::default(),
		};
		let mut attach = Attach::open_with_client(&client, None, arg).await?;
		assert!(
			attach.header.sync.is_some(),
			"the header should carry the command sync"
		);
		for _ in 0..count {
			let size = tg::process::tty::Size { cols: 80, rows: 24 };
			let arg = tg::process::tty::size::put::Arg {
				location: None,
				size,
				tokens: tg::authorization::Tokens::default(),
			};
			attach.send(tg::process::attach::ClientRequestArg::Tty(arg))?;
		}
		ready_sender.send(()).ok();
		let responses = attach.finish().await?;
		Ok::<_, tg::Error>((responses, attach.outcome))
	});

	// The client sends its requests while the command push is held.
	remote
		.wait_checkpoint(Some(root_token), "sync.get.store.object", store, 0)
		.await?;
	tokio::time::timeout(Duration::from_secs(30), ready)
		.await
		.map_err(|_| {
			tg::error!("the client should send its requests while the command push is held")
		})?
		.map_err(|_| tg::error!("the connection task ended"))?;
	remote
		.unwatch_checkpoint(Some(root_token), "sync.get.store.object", store)
		.await?;
	runner
		.unwatch_checkpoint(None, "runner.process.run", run)
		.await?;

	// The buffered requests do not block the command sync, and every request completes.
	let (responses, outcome) = tokio::time::timeout(Duration::from_secs(30), task)
		.await
		.map_err(|_| tg::error!("the run should complete"))?
		.map_err(|error| tg::error!(!error, "the connection task panicked"))??;
	assert_eq!(responses, count);
	assert_eq!(outcome.map(|outcome| outcome.exit), Some(0));

	local.stop().await?;
	runner.stop().await?;
	remote.stop().await?;

	Ok(())
}

/// A wait that locates a process by an attach's nonce before the attach exists receives the process's outcome, and a wait by a nonce that no attach owns is not found.
#[tokio::test]
async fn nonce() -> tg::Result<()> {
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"advanced": { "checkpoints": true },
		"process": { "attach": { "service": { "request_timeout": 5 } } },
	}));
	let server = Server::start(env!("CARGO_BIN_EXE_tangram"), config).await?;
	let (command, _) = command(&server, None).await?;

	// Wait by the nonce, and hold the attach until the wait looks for the nonce's owner.
	let nonce = tg::process::Nonce::new();
	let heartbeat = "process.attach.service.heartbeat.request";
	let watch = server
		.watch_checkpoint(None, heartbeat, serde_json::json!({}))
		.await?;
	let wait = tokio::spawn(wait_by_nonce(server.client()?, None, nonce));
	server.wait_checkpoint(None, heartbeat, watch, 0).await?;
	server.unwatch_checkpoint(None, heartbeat, watch).await?;

	// Spawn the process through an attach with the nonce.
	let outcome = attach_with_nonce(&server, None, command, nonce).await?;
	assert_eq!(outcome.exit, 0);

	// The wait receives the outcome.
	let outcome = wait
		.await
		.map_err(|error| tg::error!(!error, "the wait task panicked"))??
		.ok_or_else(|| tg::error!("the wait should find the process"))?;
	assert_eq!(outcome.exit, 0);

	// A wait by a nonce that no attach owns is not found.
	let unknown = wait_by_nonce(server.client()?, None, tg::process::Nonce::new()).await?;
	assert!(
		unknown.is_none(),
		"a wait by an unknown nonce should not be found"
	);

	server.stop().await?;

	Ok(())
}

/// A wait that locates a process by an attach's nonce is rejected unless it is authenticated as the principal that opened the attach.
#[tokio::test]
async fn nonce_principal() -> tg::Result<()> {
	let root_token = "root";
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"advanced": { "checkpoints": true },
		"authentication": { "root": { "token": root_token }, "users": { "providers": { "insecure": true } } },
		"process": { "attach": { "service": { "request_timeout": 5 } } },
	}));
	let server = Server::start(env!("CARGO_BIN_EXE_tangram"), config).await?;
	let alice = server.login("alice").await?;
	let bob = server.login("bob").await?;
	let (command, _) = command(&server, Some(&alice.token)).await?;

	// Wait as bob by alice's nonce, and hold alice's attach until the wait looks for the nonce's owner.
	let nonce = tg::process::Nonce::new();
	let heartbeat = "process.attach.service.heartbeat.request";
	let watch = server
		.watch_checkpoint(Some(root_token), heartbeat, serde_json::json!({}))
		.await?;
	let wait = tokio::spawn(wait_by_nonce(
		server.client()?,
		Some(bob.token.clone()),
		nonce,
	));
	server
		.wait_checkpoint(Some(root_token), heartbeat, watch, 0)
		.await?;
	server
		.unwatch_checkpoint(Some(root_token), heartbeat, watch)
		.await?;

	// Spawn the process as alice through an attach with the nonce.
	let outcome = attach_with_nonce(&server, Some(&alice.token), command, nonce).await?;
	assert_eq!(outcome.exit, 0);

	// Bob's wait is rejected.
	let error = wait
		.await
		.map_err(|error| tg::error!(!error, "the wait task panicked"))?
		.expect_err("bob's wait by alice's nonce should be rejected");
	let message = error.trace().to_string();
	assert!(message.contains("403"), "{message}");

	server.stop().await?;

	Ok(())
}

/// A local node capability must not hide parent authority on a process connection, whether the parent authority is a remote capability or a local grant.
#[tokio::test]
async fn attach_control_authorization() -> tg::Result<()> {
	// Start a scheduler and a runner with users.
	let tangram = env!("CARGO_BIN_EXE_tangram");
	let remote_root = "remote";
	let runner_root = "runner";
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"advanced": { "multi_server": true },
		"authentication": { "root": { "token": remote_root }, "users": { "providers": { "insecure": true } } },
		"roles": ["api", "indexer", "scheduler"],
	}));
	let remote = Server::start(tangram, config).await?;
	let remote_url = remote.client()?.url().to_string();
	let created = remote.create_runner(remote_root, None).await?;
	let runner_token = created
		.token
		.token
		.clone()
		.ok_or_else(|| tg::error!("expected the runner's token"))?;
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"advanced": { "checkpoints": true },
		"authentication": { "root": { "token": runner_root }, "users": { "providers": { "insecure": true } } },
		"remotes": { "default": { "token": runner_token, "url": remote_url } },
		"roles": ["api", "indexer", "runner"],
		"runner": { "id": created.data.id.to_string(), "remote": "default", "token": runner_token },
	}));
	let runner = Server::start(tangram, config).await?;
	let alice = runner.login("alice").await?;
	runner
		.tg(
			Some(&alice.token),
			["remote", "put", "default", &remote_url],
		)
		.await?;
	let alice_id = alice.user.id.to_string();

	// Store an executable that waits for input and ignores TERM.
	let script = "#!/bin/sh\ntrap \"\" TERM\nprintf \"ready\\n\"\nread line\nprintf \"%s\\n\" \"$line\"\nread line || true\n";
	let contents = serde_json::to_string(script).unwrap();
	let value = format!(r#"tg.file({{ "contents": tg.blob({contents}), "executable": true }})"#);
	let file = remote
		.tg(Some(remote_root), ["put", "--no-tokens", &value])
		.await?;
	let file = node(&file).to_owned();

	for authority in ["remote", "local"] {
		// Spawn the process on the scheduler, and wait for the runner to record it.
		let inserted = "runner.process.state.inserted";
		let watch = runner
			.watch_checkpoint(Some(runner_root), inserted, serde_json::json!({}))
			.await?;
		let arg = serde_json::from_value::<tg::process::spawn::Arg>(serde_json::json!({
			"command": { "node": { "executable": { "node": { "artifact": file } } } },
			"sandbox": {},
			"stderr": "pipe",
			"stdin": "pipe",
			"stdout": "pipe",
		}))
		.map_err(|error| tg::error!(!error, "failed to create the spawn arg"))?;
		let session = remote
			.client()?
			.session(&tg::Context::new(Some(remote_root.to_owned())));
		let spawn = tokio::spawn(async move {
			let stream = session.spawn_process(arg).await?;
			let mut stream = std::pin::pin!(stream);
			let mut output = None;
			while let Some(event) = stream.next().await {
				if let tg::progress::Event::Output(value) = event? {
					output = Some(value);
				}
			}
			output.ok_or_else(|| tg::error!("expected the spawn's output"))
		});
		runner
			.wait_checkpoint(Some(runner_root), inserted, watch, 0)
			.await?;
		runner
			.unwatch_checkpoint(Some(runner_root), inserted, watch)
			.await?;
		let spawned = tokio::time::timeout(Duration::from_secs(30), spawn)
			.await
			.map_err(|_| tg::error!("the spawn should complete"))?
			.map_err(|error| tg::error!(!error, "the spawn task panicked"))??;
		let process = spawned
			.process
			.right()
			.ok_or_else(|| tg::error!("expected the process's ID"))?;
		let process_string = process.to_string();
		let capability = spawned
			.tokens
			.local_authorization()
			.first()
			.cloned()
			.ok_or_else(|| tg::error!("expected the process's capability"))?;

		// Give alice node authority, and get her node token from the runner.
		runner
			.tg(
				Some(runner_root),
				["grant", &alice_id, "process_node", &process_string],
			)
			.await?;
		runner.tg(Some(runner_root), ["index"]).await?;
		let session = runner
			.client()?
			.session(&tg::Context::new(Some(alice.token.clone())));
		let arg = tg::process::get::Arg {
			location: Some("remote".parse()?),
			..Default::default()
		};
		let output = session
			.try_get_process(&process, arg)
			.await?
			.ok_or_else(|| tg::error!("expected the process"))?;
		let token = output
			.tokens
			.local_authorization()
			.first()
			.cloned()
			.ok_or_else(|| tg::error!("expected alice's node token"))?;
		let mut tokens = tg::authorization::Tokens::with_authorization([token]);
		remote.tg(Some(remote_root), ["index"]).await?;
		runner.tg(Some(runner_root), ["index"]).await?;

		// Node authority alone does not permit signaling the process over a connection.
		let mut attach =
			attach_with_tokens(&runner, &alice.token, &process, tokens.clone()).await?;
		let error = attach
			.request(tg::process::attach::ClientRequestArg::Signal(signal()))
			.await
			.expect_err("node authority should not permit a signal");
		let message = error.trace().to_string().to_lowercase();
		assert!(
			["unauthorized", "not found", "failed to find"]
				.iter()
				.any(|pattern| message.contains(pattern)),
			"{message}"
		);
		attach
			.request(tg::process::attach::ClientRequestArg::Detach)
			.await?;
		attach.closed().await?;

		// Parent authority, from the remote capability or a local grant, permits signaling the process.
		if authority == "remote" {
			tokens.insert_authorization("remote".parse()?, capability);
		} else {
			runner
				.tg(
					Some(runner_root),
					["grant", &alice_id, "process_parent", &process_string],
				)
				.await?;
			runner.tg(Some(runner_root), ["index"]).await?;
		}
		let mut attach = attach_with_tokens(&runner, &alice.token, &process, tokens).await?;
		attach
			.request(tg::process::attach::ClientRequestArg::Signal(signal()))
			.await?;
		attach
			.request(tg::process::attach::ClientRequestArg::Detach)
			.await?;
		attach.closed().await?;
		let lease = spawned
			.lease
			.ok_or_else(|| tg::error!("expected the process's lease"))?;
		remote
			.tg(
				Some(remote_root),
				["cancel", &process_string, &lease.to_string()],
			)
			.await?;
	}

	runner.stop().await?;
	remote.stop().await?;

	Ok(())
}

/// A process connection.
struct Attach {
	header: tg::process::attach::Header,
	next: u64,
	outcome: Option<tg::process::outcome::Data>,
	sender: mpsc::UnboundedSender<tg::Result<tg::process::attach::ClientMessage>>,
	stream: BoxStream<'static, tg::Result<tg::process::attach::ServerMessage>>,
}

impl Attach {
	async fn open(
		server: &Server,
		token: Option<&str>,
		arg: tg::process::attach::Arg,
	) -> tg::Result<Self> {
		Self::open_with_client(&server.client()?, token, arg).await
	}

	async fn open_with_client(
		client: &tg::Client,
		token: Option<&str>,
		arg: tg::process::attach::Arg,
	) -> tg::Result<Self> {
		let session = client.session(&tg::Context::new(token.map(ToOwned::to_owned)));
		let (sender, receiver) = mpsc::unbounded();
		let (header, stream) = match session
			.try_get_process_attach_stream(arg, receiver.boxed())
			.await?
		{
			ControlFlow::Break(Some(output)) => output,
			ControlFlow::Break(None) => return Err(tg::error!("the process was not found")),
			ControlFlow::Continue(error) => return Err(error),
		};
		let attach = Self {
			header,
			next: 1,
			outcome: None,
			sender,
			stream,
		};
		Ok(attach)
	}

	/// Wait for the spawn's output, and tell the server that the client is ready.
	async fn output(&mut self) -> tg::Result<tg::process::spawn::Output> {
		loop {
			let message = self
				.next()
				.await?
				.ok_or_else(|| tg::error!("the connection closed before the output"))?;
			if let tg::process::attach::ServerMessage::Notification(
				tg::process::attach::ServerNotification::Progress(tg::progress::Event::Output(
					output,
				)),
			) = message
			{
				self.notify_ready()?;
				return Ok(output);
			}
		}
	}

	/// Send a request, wait for its response, and acknowledge it.
	async fn request(
		&mut self,
		arg: tg::process::attach::ClientRequestArg,
	) -> tg::Result<tg::process::attach::ServerResponseOutput> {
		let id = self.send(arg)?;
		loop {
			let message = self
				.next()
				.await?
				.ok_or_else(|| tg::error!("the connection closed before the response"))?;
			if let tg::process::attach::ServerMessage::Response(response) = message
				&& response.id == id
			{
				self.acknowledge(id)?;
				if let Some(error) = response.error {
					return Err(tg::error!(?error, "the request failed"));
				}
				return response
					.output
					.ok_or_else(|| tg::error!("expected the response's output"));
			}
		}
	}

	/// Send a request without waiting for its response, and return its ID.
	fn send(&mut self, arg: tg::process::attach::ClientRequestArg) -> tg::Result<u64> {
		let id = self.next;
		self.next += 1;
		let request = tg::process::attach::ClientRequest { arg, id };
		self.sender
			.unbounded_send(Ok(tg::process::attach::ClientMessage::Request(request)))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		Ok(id)
	}

	/// Read until the connection closes, telling the server that the client is ready and acknowledging responses, and return the number of responses, including failed ones.
	async fn finish(&mut self) -> tg::Result<u64> {
		let mut responses = 0;
		while let Some(message) = self.next().await? {
			match message {
				tg::process::attach::ServerMessage::Notification(
					tg::process::attach::ServerNotification::Progress(tg::progress::Event::Output(
						_,
					)),
				) => self.notify_ready()?,
				tg::process::attach::ServerMessage::Response(response) => {
					self.acknowledge(response.id)?;
					responses += 1;
				},
				_ => (),
			}
		}
		Ok(responses)
	}

	/// Read until the connection closes.
	async fn closed(&mut self) -> tg::Result<()> {
		while self.next().await?.is_some() {}
		Ok(())
	}

	/// Read the next message, recording the outcome.
	async fn next(&mut self) -> tg::Result<Option<tg::process::attach::ServerMessage>> {
		let message = tokio::time::timeout(Duration::from_secs(20), self.stream.next())
			.await
			.map_err(|_| tg::error!("timed out waiting for the connection"))?
			.transpose()?;
		if let Some(tg::process::attach::ServerMessage::Notification(
			tg::process::attach::ServerNotification::Outcome(outcome),
		)) = &message
		{
			self.outcome = Some(outcome.clone());
		}
		Ok(message)
	}

	fn notify_ready(&self) -> tg::Result<()> {
		let notification = tg::process::attach::ClientNotification::Ready;
		self.sender
			.unbounded_send(Ok(tg::process::attach::ClientMessage::Notification(
				notification,
			)))
			.map_err(|error| tg::error!(!error, "failed to send the notification"))
	}

	fn acknowledge(&self, id: u64) -> tg::Result<()> {
		let ack = tg::process::attach::Ack { id };
		self.sender
			.unbounded_send(Ok(tg::process::attach::ClientMessage::Ack(ack)))
			.map_err(|error| tg::error!(!error, "failed to send the ack"))
	}
}

/// Open a connection to a process on the remote through the server as the user, with the tokens, and wait for its output.
async fn attach_with_tokens(
	server: &Server,
	token: &str,
	process: &tg::process::Id,
	tokens: tg::authorization::Tokens,
) -> tg::Result<Attach> {
	let mut arg = attach_arg(process, None, "remote")?;
	arg.tokens = tokens;
	let mut attach = Attach::open(server, Some(token), arg).await?;
	attach.output().await?;
	Ok(attach)
}

fn signal() -> tg::process::signal::post::Arg {
	tg::process::signal::post::Arg {
		location: None,
		signal: tg::process::Signal::SIGTERM,
		tokens: tg::authorization::Tokens::default(),
	}
}

/// Create the arg for a connection that runs an existing process at the location.
fn attach_arg(
	id: &tg::process::Id,
	lease: Option<tg::process::Lease>,
	location: &str,
) -> tg::Result<tg::process::attach::Arg> {
	let arg = tg::process::attach::Arg {
		lease,
		location: Some(location.parse()?),
		mode: tg::process::attach::Mode::Run,
		nonce: None,
		open: Vec::new(),
		process: tg::Either::Right(id.clone()),
		push: false,
		tokens: tg::authorization::Tokens::default(),
	};
	Ok(arg)
}

/// Create the arg that spawns the command in a new sandbox with null stdio.
fn spawn_arg(
	command: tg::Either<tg::process::spawn::CommandArg, tg::command::Id>,
) -> tg::process::spawn::Arg {
	tg::process::spawn::Arg {
		cache_location: None,
		cached: None,
		checksum: None,
		command: tg::Referent::with_node(command),
		debug: None,
		location: None,
		parent: None,
		public: false,
		retry: false,
		sandbox: Some(tg::Either::Left(tg::sandbox::create::Arg::default())),
		scheduler: None,
		stderr: tg::process::Stdio::Null,
		stdin: tg::process::Stdio::Null,
		stdout: tg::process::Stdio::Null,
		tty: None,
	}
}

/// Spawn the command through a connection with the nonce, and return the process's outcome.
async fn attach_with_nonce(
	server: &Server,
	token: Option<&str>,
	command: tg::command::Id,
	nonce: tg::process::Nonce,
) -> tg::Result<tg::process::outcome::Data> {
	let mut spawn = spawn_arg(tg::Either::Right(command));
	spawn.cached = Some(false);
	let arg = tg::process::attach::Arg {
		lease: None,
		location: None,
		mode: tg::process::attach::Mode::Run,
		nonce: Some(nonce),
		open: Vec::new(),
		process: tg::Either::Left(Box::new(spawn)),
		push: false,
		tokens: tg::authorization::Tokens::default(),
	};
	let mut attach = Attach::open(server, token, arg).await?;
	attach.finish().await?;
	attach
		.outcome
		.take()
		.ok_or_else(|| tg::error!("the connection closed before the outcome"))
}

/// Wait for a process by an attach's nonce, and return its outcome, or `None` if no attach owns the nonce.
async fn wait_by_nonce(
	client: tg::Client,
	token: Option<String>,
	nonce: tg::process::Nonce,
) -> tg::Result<Option<tg::process::outcome::Data>> {
	let session = client.session(&tg::Context::new(token));
	let process = tg::Either::Left(nonce);
	let Some(future) = session
		.try_wait_process_future(&process, tg::process::wait::Arg::default())
		.await?
	else {
		return Ok(None);
	};
	let outcome = tokio::time::timeout(Duration::from_secs(30), future)
		.await
		.map_err(|_| tg::error!("timed out waiting for the process"))??;
	Ok(outcome)
}

/// Store an executable command that exits successfully, and return it and its blob.
async fn command(server: &Server, token: Option<&str>) -> tg::Result<(tg::command::Id, String)> {
	let contents = serde_json::to_string("#!/bin/sh\nexit 0\n").unwrap();
	let blob = server
		.tg(
			token,
			["put", "--no-tokens", &format!("tg.blob({contents})")],
		)
		.await?;
	let blob = node(&blob).to_owned();
	let file = server
		.tg(
			token,
			[
				"put",
				"--no-tokens",
				&format!(r#"tg.file({{"contents":{blob},"executable":true}})"#),
			],
		)
		.await?;
	let file = node(&file).to_owned();
	let os = if std::env::consts::OS == "macos" {
		"darwin"
	} else {
		std::env::consts::OS
	};
	let host = format!("{}-{os}", std::env::consts::ARCH);
	let value = format!(r#"tg.command({{"executable":{{"artifact":{file}}},"host":"{host}"}})"#);
	let command = server.tg(token, ["put", "--no-tokens", &value]).await?;
	let command = node(&command).parse()?;
	Ok((command, blob))
}

/// Build the package without waiting, and return the process and its lease.
async fn build(
	server: &Server,
	package: &Temp,
	args: &[&str],
) -> tg::Result<(tg::process::Id, tg::process::Lease)> {
	let path = package
		.path()
		.to_str()
		.ok_or_else(|| tg::error!("invalid path"))?;
	let mut command = vec!["build", "--detach", "--no-tokens", "--verbose"];
	command.extend(args);
	command.push(path);
	let output = server.tg(None, command).await?;
	let output = serde_json::from_str::<tg::process::spawn::Output>(&output)
		.map_err(|error| tg::error!(!error, %output, "failed to deserialize the build output"))?;
	let process = output
		.process
		.right()
		.ok_or_else(|| tg::error!("expected the process's ID"))?;
	let lease = output
		.lease
		.ok_or_else(|| tg::error!("expected the process's lease"))?;
	Ok((process, lease))
}

async fn status(server: &Server, id: &tg::process::Id) -> tg::Result<String> {
	let output = server
		.tg(None, ["process", "get", "--no-tokens", &id.to_string()])
		.await?;
	let output = serde_json::from_str::<serde_json::Value>(&output)
		.map_err(|error| tg::error!(!error, "failed to deserialize the process"))?;
	let status = output["status"].as_str().unwrap_or_default().to_owned();
	Ok(status)
}

/// Write a package with the module's source, and return its directory.
fn package(source: &str) -> tg::Result<Temp> {
	let temp = Temp::new().map_err(|error| tg::error!(!error, "failed to create the package"))?;
	std::fs::write(temp.path().join("tangram.ts"), source)
		.map_err(|error| tg::error!(!error, "failed to write the module"))?;
	Ok(temp)
}

/// Get the node of a referent.
fn node(referent: &str) -> &str {
	referent.split('?').next().unwrap_or(referent)
}
