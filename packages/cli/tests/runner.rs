use {
	futures::{SinkExt as _, StreamExt as _},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_tests_util::{Config, cli::Server},
};

const ROOT_TOKEN: &str = "root";

/// Destroy sandbox requests on a runner control connection enforce runner ownership and are idempotent.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn destroy_sandbox_requires_ownership() -> tg::Result<()> {
	let tangram = env!("CARGO_BIN_EXE_tangram");

	// Create a file system with project quotas for sandbox storage accounting, or skip the test if passwordless sudo is not available.
	let Some(quota) = tangram_tests_util::quota::Mount::new().map_err(|error| {
		tg::error!(
			!error,
			"failed to create the file system with project quotas"
		)
	})?
	else {
		eprintln!(
			"skipping the test because passwordless sudo is required to create a file system with project quotas"
		);
		return Ok(());
	};

	// Start a scheduler and a runner, whose directory supports sandbox storage accounting.
	let root_token = "root";
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"advanced": { "multi_server": true },
		"authentication": { "root": { "token": root_token } },
		"roles": ["api", "indexer", "scheduler"],
		"scheduler": { "runner_time_to_live": 300 },
	}));
	let remote = Server::start(tangram, config).await?;
	let created = remote.create_runner(root_token, None).await?;
	let created_token = created
		.token
		.token
		.clone()
		.ok_or_else(|| tg::error!("expected the runner's token"))?;
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"remotes": { "default": { "token": created_token, "url": remote.client()?.url().to_string() } },
		"runner": { "id": created.data.id.to_string(), "remote": "default", "token": created_token },
	}));
	let directory = tangram_tests_util::Temp::with_parent(&quota.mount_path())
		.map_err(|error| tg::error!(!error, "failed to create the runner's directory"))?;
	let mut runner =
		Server::start_with_path(tangram, config, directory.path().join("server")).await?;
	let sandbox = remote
		.tg(Some(root_token), ["sandbox", "create", "--no-tokens"])
		.await?;
	let sandbox = sandbox
		.split('?')
		.next()
		.unwrap_or(&sandbox)
		.parse::<tg::sandbox::Id>()?;

	// Another runner cannot destroy the sandbox.
	let other = remote.create_runner(root_token, None).await?;
	let destroyed = destroy_sandbox(&remote, &other, &sandbox, false).await?;
	assert!(
		!destroyed.0,
		"another runner should not destroy the sandbox"
	);
	assert!(
		!destroyed.1,
		"a repeated destroy should not destroy the sandbox"
	);
	let status = sandbox_status(&remote, root_token, &sandbox).await?;
	assert_eq!(status, "started");

	// Stop the sandbox, crash the runner, and destroy the stopped sandbox, which records it as destroying until its runner removes the directory.
	let id = sandbox.to_string();
	remote
		.tg(Some(root_token), ["sandbox", "stop", "--wait", &id])
		.await?;
	runner.kill().await?;
	remote
		.tg(Some(root_token), ["sandbox", "destroy", &id])
		.await?;
	let status = sandbox_status(&remote, root_token, &sandbox).await?;
	assert_eq!(status, "destroying");

	// The runner that hosts the sandbox reports its destruction once, after the scheduler asks it to remove the directory.
	let destroyed = destroy_sandbox(&remote, &created, &sandbox, true).await?;
	assert!(
		destroyed.0,
		"the sandbox's runner should destroy the sandbox"
	);
	assert!(
		!destroyed.1,
		"a repeated destroy should not destroy the sandbox again"
	);
	let status = sandbox_status(&remote, root_token, &sandbox).await?;
	assert_eq!(status, "destroyed");

	remote.stop().await?;

	Ok(())
}

/// A runner cannot finish a process of a sandbox that another runner holds, with a report of the process's finish or of the end of the sandbox's boot.
#[tokio::test]
async fn reports_require_the_boot() -> tg::Result<()> {
	let tangram = env!("CARGO_BIN_EXE_tangram");

	// Start a scheduler and a runner.
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"authentication": { "root": { "token": ROOT_TOKEN } },
		"roles": ["api", "indexer", "scheduler"],
	}));
	let remote = Server::start(tangram, config).await?;
	let created = remote.create_runner(ROOT_TOKEN, None).await?;
	let created_token = created
		.token
		.token
		.clone()
		.ok_or_else(|| tg::error!("expected the runner's token"))?;
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"remotes": { "default": { "token": created_token, "url": remote.client()?.url().to_string() } },
		"roles": ["api", "indexer", "runner"],
		"runner": { "id": created.data.id.to_string(), "remote": "default", "token": created_token },
	}));
	let runner = Server::start(tangram, config).await?;

	// Spawn a process that keeps running in a new sandbox on the runner.
	let contents = serde_json::to_string("#!/bin/sh\nwhile :; do :; done\n").unwrap();
	let command = executable_command(&remote, &contents).await?;
	let output = remote
		.spawn(ROOT_TOKEN, ["--cached=false", "--sandbox", &command])
		.await?;
	let process = output
		.process
		.right()
		.ok_or_else(|| tg::error!("expected the process's ID"))?;
	let process_string = process.to_string();
	remote
		.tg(
			Some(ROOT_TOKEN),
			["process", "status", "--until", "started", &process_string],
		)
		.await?;
	let session = remote
		.client()?
		.session(&tg::Context::new(Some(ROOT_TOKEN.to_owned())));
	let data = session
		.try_get_process(&process, tg::process::get::Arg::default())
		.await?
		.ok_or_else(|| tg::error!("expected the process"))?
		.data;
	let sandbox = data
		.sandbox
		.clone()
		.ok_or_else(|| tg::error!("expected the process's sandbox"))?;
	let sandbox_string = sandbox.to_string();
	let sandbox_output = remote
		.tg(Some(ROOT_TOKEN), ["sandbox", "get", &sandbox_string])
		.await?;
	let sandbox_output = serde_json::from_str::<tg::sandbox::get::Output>(&sandbox_output)
		.map_err(|error| tg::error!(!error, "failed to deserialize the sandbox"))?;
	let boot = sandbox_output.data.boot;

	// Another runner reports that the process cannot run, and that the sandbox's boot ended.
	let other = remote.create_runner(ROOT_TOKEN, None).await?;
	let reported = tg::runner::control::Process {
		data,
		id: Some(process.clone()),
		lease: tg::process::Lease::new(),
		leases: Vec::new(),
		open: Vec::new(),
		options: tg::referent::Options::default(),
		parent: None,
		token: None,
	};
	let error = tg::Either::Left(tg::error::Data {
		message: Some("the process cannot run".to_owned()),
		..Default::default()
	});
	let finish = tg::runner::control::ClientRequestArg::FinishProcesses(
		tg::runner::control::FinishProcessesClientRequestArg {
			boot,
			error: error.clone(),
			processes: vec![reported],
			sandbox: sandbox.clone(),
			sequence: u64::MAX,
		},
	);
	let mut ended = sandbox_output;
	ended.data.status = tg::sandbox::Status::Destroyed;
	ended.data.error = Some(error);
	let end = tg::runner::control::ClientRequestArg::EndSandbox(
		tg::runner::control::EndSandboxClientRequestArg {
			boot,
			created_at: 0,
			data: ended,
			processes: vec![process.clone()],
			proof: None,
			recorded: false,
			sandbox: sandbox.clone(),
			sequence: u64::MAX,
		},
	);
	let responses = send_runner_requests(&remote, &other, vec![finish, end]).await?;

	// The process and its sandbox keep running.
	remote.tg(Some(ROOT_TOKEN), ["index"]).await?;
	let data = session
		.try_get_process(&process, tg::process::get::Arg::default())
		.await?
		.ok_or_else(|| tg::error!("expected the process"))?
		.data;
	assert_eq!(data.status, tg::process::Status::Started);
	let data = sandbox_data(&remote, &sandbox_string).await?;
	assert_eq!(data["status"], "started");

	// The scheduler refuses both reports.
	for response in responses {
		assert!(
			response.error.is_some(),
			"the scheduler should refuse the other runner's report"
		);
	}

	runner.stop().await?;
	remote.stop().await?;

	Ok(())
}

/// Only a runner that proves that it derived a sandbox's ID may open a sandbox control connection for a new sandbox and receive its token.
#[tokio::test]
async fn sandbox_control_requires_proof() -> tg::Result<()> {
	// Start a server with users and a root token.
	let root_token = "root";
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"authentication": {
			"root": { "token": root_token },
			"users": { "providers": { "insecure": true } },
		},
	}));
	let server = Server::start(env!("CARGO_BIN_EXE_tangram"), config).await?;
	let mallory = server.login("mallory").await?;
	let owner = tg::Principal::User(mallory.user.id.clone()).into();
	let runner = server.create_runner(&mallory.token, Some(&owner)).await?;
	let runner_token = runner
		.token
		.token
		.clone()
		.ok_or_else(|| tg::error!("expected the runner's token"))?;
	let other = server.create_runner(&mallory.token, Some(&owner)).await?;

	// A runner without a proof, and the root, are rejected.
	for token in [runner_token.as_str(), root_token] {
		let arg = sandbox_control_arg(&runner.data.id, None);
		let error = connect_sandbox(&server, token, arg)
			.await
			.expect_err("a connection for a new sandbox should require a proof");
		let message = error.trace().to_string();
		assert!(message.contains("403"), "{message}");
	}

	// A runner must not connect a sandbox whose ID another runner derived, or name another runner.
	let arg = sandbox_control_arg(&runner.data.id, Some(&other.data.id));
	let error = connect_sandbox(&server, &runner_token, arg)
		.await
		.expect_err("a runner should not connect a sandbox whose ID another runner derived");
	let message = error.trace().to_string();
	assert!(message.contains("did not derive"), "{message}");
	let arg = sandbox_control_arg(&other.data.id, Some(&other.data.id));
	let error = connect_sandbox(&server, &runner_token, arg)
		.await
		.expect_err("a runner should not connect a sandbox for another runner");
	let message = error.trace().to_string();
	assert!(message.contains("must be its principal"), "{message}");

	// A runner that derived the sandbox's ID receives the sandbox's token.
	let arg = sandbox_control_arg(&runner.data.id, Some(&runner.data.id));
	let header = connect_sandbox(&server, &runner_token, arg).await?;
	assert!(
		header.token.is_some_and(|token| !token.is_empty()),
		"the header should carry the sandbox's token"
	);

	server.stop().await?;

	Ok(())
}

/// A runner whose control stream ends with an end message is removed at once, and its sandbox stops.
#[tokio::test]
async fn a_runner_control_stream_that_ends_with_an_end_message_removes_the_runner() -> tg::Result<()>
{
	// Start a scheduler, connect a runner, and connect a sandbox on the runner.
	let (server, runner, sandbox, sender) = start_runner_with_sandbox().await?;
	let token = Some(ROOT_TOKEN);
	let watch = watch_checkpoint(&server, "runner.control.ended").await?;

	// End the control stream with an end message, and wait for the server to handle its end.
	let mut sender = sender;
	sender
		.send(Ok(tg::runner::control::ClientMessage::Notification(
			tg::runner::control::ClientNotification::End,
		)))
		.await
		.map_err(|error| tg::error!(!error, "failed to send the end message"))?;
	drop(sender);
	server
		.tg(
			token,
			[
				"checkpoint",
				"wait",
				"--timeout",
				"30s",
				"runner.control.ended",
				&watch,
				"0",
			],
		)
		.await?;
	server
		.tg(
			token,
			["checkpoint", "unwatch", "runner.control.ended", &watch],
		)
		.await?;

	// The runner is removed at once, which stops its sandbox.
	let sandbox = sandbox.to_string();
	server
		.tg(token, ["sandbox", "status", "--until", "stopped", &sandbox])
		.await?;
	let data = sandbox_data(&server, &sandbox).await?;
	assert_eq!(data["status"], "stopped");
	assert_eq!(data["error"]["message"], "heartbeat expired");

	drop(runner);
	server.stop().await?;

	Ok(())
}

/// A runner whose control stream ends without an end message is not removed, so that its sandbox keeps running until the runner's ttl expires.
#[tokio::test]
async fn a_runner_control_stream_that_ends_without_an_end_message_keeps_the_runner()
-> tg::Result<()> {
	// Start a scheduler, connect a runner, and connect a sandbox on the runner.
	let (server, runner, sandbox, sender) = start_runner_with_sandbox().await?;
	let token = Some(ROOT_TOKEN);
	let watch = watch_checkpoint(&server, "runner.control.ended").await?;

	// End the control stream cleanly without an end message, and wait for the server to handle its end.
	drop(sender);
	server
		.tg(
			token,
			[
				"checkpoint",
				"wait",
				"--timeout",
				"30s",
				"runner.control.ended",
				&watch,
				"0",
			],
		)
		.await?;
	server
		.tg(
			token,
			["checkpoint", "unwatch", "runner.control.ended", &watch],
		)
		.await?;

	// The sandbox keeps running.
	let sandbox = sandbox.to_string();
	server.tg(token, ["index"]).await?;
	let data = sandbox_data(&server, &sandbox).await?;
	assert_eq!(data["status"], "started");

	// The sandbox stops once the runner's ttl expires.
	server.tg(token, ["clock", "advance", "4000ms"]).await?;
	server
		.tg(token, ["sandbox", "status", "--until", "stopped", &sandbox])
		.await?;
	let data = sandbox_data(&server, &sandbox).await?;
	assert_eq!(data["status"], "stopped");
	assert_eq!(data["error"]["message"], "heartbeat expired");

	drop(runner);
	server.stop().await?;

	Ok(())
}

/// Open a runner control connection as the runner, wait for the scheduler to ask the runner to remove the sandbox's directory if `asked` is set, and report the sandbox's destruction twice. Return whether each report destroyed it.
#[cfg(target_os = "linux")]
async fn destroy_sandbox(
	server: &Server,
	runner: &tg::runner::create::Output,
	sandbox: &tg::sandbox::Id,
	asked: bool,
) -> tg::Result<(bool, bool)> {
	// Open the connection.
	let token = runner
		.token
		.token
		.clone()
		.ok_or_else(|| tg::error!("expected the runner's token"))?;
	let session = server.client()?.session(&tg::Context::new(Some(token)));
	let (mut sender, receiver) =
		futures::channel::mpsc::unbounded::<tg::Result<tg::runner::control::ClientMessage>>();
	let arg = tg::runner::control::Arg {
		first: false,
		heartbeat: tg::runner::control::HeartbeatClientNotification::default(),
		host: "test".to_owned(),
		id: runner.data.id.clone(),
		location: None,
		sandboxes: Vec::new(),
		scheduler_time_to_live: std::time::Duration::from_secs(300),
	};
	let (_, stream) = match session
		.get_runner_control_stream(arg, receiver.boxed())
		.await?
	{
		ControlFlow::Break(output) => output,
		ControlFlow::Continue(error) => return Err(error),
	};
	let mut stream = std::pin::pin!(stream);

	// Send a heartbeat, so that the scheduler considers the runner ready, and wait for the scheduler to ask the runner to remove the sandbox's directory.
	if asked {
		let mut capacity = tg::runner::control::Capacity::default();
		capacity.total.sandboxes = 1;
		let heartbeat = tg::runner::control::HeartbeatClientNotification {
			capacity,
			index: 1,
			sandboxes: std::collections::BTreeSet::new(),
		};
		sender
			.send(Ok(tg::runner::control::ClientMessage::Notification(
				tg::runner::control::ClientNotification::Heartbeat(heartbeat),
			)))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the heartbeat"))?;
		loop {
			let message = stream
				.next()
				.await
				.ok_or_else(|| tg::error!("the control stream ended"))??;
			if let tg::runner::control::ServerMessage::Request(request) = message
				&& let tg::runner::control::ServerRequestArg::DestroySandbox(arg) = request.arg
				&& arg.sandbox == *sandbox
			{
				break;
			}
		}
	}

	// Report the sandbox's destruction twice.
	let mut destroyed = Vec::new();
	for id in ["destroy", "repeat"] {
		let request = tg::runner::control::ClientRequest {
			arg: tg::runner::control::ClientRequestArg::DestroySandbox(
				tg::runner::control::DestroySandboxClientRequestArg {
					error: None,
					sandbox: sandbox.clone(),
				},
			),
			id: id.to_owned(),
		};
		sender
			.send(Ok(tg::runner::control::ClientMessage::Request(request)))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		let response = loop {
			let message = stream
				.next()
				.await
				.ok_or_else(|| tg::error!("the control stream ended"))??;
			if let tg::runner::control::ServerMessage::Response(response) = message
				&& response.id == id
			{
				break response;
			}
		};
		if let Some(error) = response.error {
			return Err(tg::error!(?error, "the destroy request failed"));
		}
		let ack = tg::runner::control::ClientAck { id: id.to_owned() };
		sender
			.send(Ok(tg::runner::control::ClientMessage::Ack(ack)))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the ack"))?;
		let Some(tg::runner::control::ServerResponseOutput::DestroySandbox(output)) =
			response.output
		else {
			return Err(tg::error!("expected a destroy sandbox response"));
		};
		destroyed.push(output.destroyed);
	}

	Ok((destroyed[0], destroyed[1]))
}

#[cfg(target_os = "linux")]
async fn sandbox_status(
	server: &Server,
	token: &str,
	sandbox: &tg::sandbox::Id,
) -> tg::Result<String> {
	let output = server
		.tg(
			Some(token),
			["sandbox", "get", sandbox.to_string().as_str()],
		)
		.await?;
	let output = serde_json::from_str::<serde_json::Value>(&output)
		.map_err(|error| tg::error!(!error, "failed to deserialize the sandbox"))?;
	let status = output["data"]["status"]
		.as_str()
		.ok_or_else(|| tg::error!("expected the sandbox's status"))?
		.to_owned();

	Ok(status)
}

/// Open a runner control connection as the runner, send the requests in order, and return the responses.
async fn send_runner_requests(
	server: &Server,
	runner: &tg::runner::create::Output,
	requests: Vec<tg::runner::control::ClientRequestArg>,
) -> tg::Result<Vec<tg::runner::control::ServerResponse>> {
	// Open the connection.
	let token = runner
		.token
		.token
		.clone()
		.ok_or_else(|| tg::error!("expected the runner's token"))?;
	let session = server.client()?.session(&tg::Context::new(Some(token)));
	let (mut sender, receiver) =
		futures::channel::mpsc::unbounded::<tg::Result<tg::runner::control::ClientMessage>>();
	let arg = tg::runner::control::Arg {
		first: false,
		heartbeat: tg::runner::control::HeartbeatClientNotification::default(),
		host: "test".to_owned(),
		id: runner.data.id.clone(),
		location: None,
		sandboxes: Vec::new(),
		scheduler_time_to_live: std::time::Duration::from_secs(300),
	};
	let (_, stream) = match session
		.get_runner_control_stream(arg, receiver.boxed())
		.await?
	{
		ControlFlow::Break(output) => output,
		ControlFlow::Continue(error) => return Err(error),
	};
	let mut stream = std::pin::pin!(stream);

	// Send each request, and wait for its response.
	let mut responses = Vec::new();
	for (index, arg) in requests.into_iter().enumerate() {
		let id = index.to_string();
		let request = tg::runner::control::ClientRequest {
			arg,
			id: id.clone(),
		};
		sender
			.send(Ok(tg::runner::control::ClientMessage::Request(request)))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		let response = loop {
			let message = stream
				.next()
				.await
				.ok_or_else(|| tg::error!("the control stream ended"))??;
			if let tg::runner::control::ServerMessage::Response(response) = message
				&& response.id == id
			{
				break response;
			}
		};
		let ack = tg::runner::control::ClientAck { id };
		sender
			.send(Ok(tg::runner::control::ClientMessage::Ack(ack)))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the ack"))?;
		responses.push(response);
	}

	Ok(responses)
}

/// Create a command whose executable is a file with the contents, for the host.
async fn executable_command(server: &Server, contents: &str) -> tg::Result<String> {
	let token = Some(ROOT_TOKEN);
	let blob = server
		.tg(
			token,
			["put", "--no-tokens", &format!("tg.blob({contents})")],
		)
		.await?;
	let blob = blob.split('?').next().unwrap_or(&blob).to_owned();
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
	let file = file.split('?').next().unwrap_or(&file).to_owned();
	let os = if std::env::consts::OS == "macos" {
		"darwin"
	} else {
		std::env::consts::OS
	};
	let host = format!("{}-{os}", std::env::consts::ARCH);
	let value = format!(r#"tg.command({{"executable":{{"artifact":{file}}},"host":"{host}"}})"#);
	let command = server.tg(token, ["put", "--no-tokens", &value]).await?;
	let command = command.split('?').next().unwrap_or(&command).to_owned();

	Ok(command)
}

/// Start a scheduler with a virtual clock and checkpoints, connect a runner to it, and connect a sandbox on the runner. Return the server, the runner's control stream, the sandbox, and the sender of the runner's control messages.
async fn start_runner_with_sandbox() -> tg::Result<(
	Server,
	futures::stream::BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>,
	tg::sandbox::Id,
	futures::channel::mpsc::UnboundedSender<tg::Result<tg::runner::control::ClientMessage>>,
)> {
	// Start the scheduler with a virtual clock at the current time, so that the runner's proofs of the sandbox IDs that it derives are recent.
	let output = std::process::Command::new("date")
		.args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
		.output()
		.map_err(|error| tg::error!(!error, "failed to get the current time"))?;
	let now = String::from_utf8(output.stdout)
		.map_err(|error| tg::error!(!error, "failed to read the current time"))?
		.trim()
		.to_owned();
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"advanced": {
			"checkpoints": true,
			"clock": { "start": now },
			"multi_server": true,
		},
		"authentication": { "root": { "token": ROOT_TOKEN } },
		"roles": ["api", "indexer", "scheduler"],
		"scheduler": { "runner_time_to_live": 3 },
	}));
	let server = Server::start(env!("CARGO_BIN_EXE_tangram"), config).await?;
	let runner = server.create_runner(ROOT_TOKEN, None).await?;
	let token = runner
		.token
		.token
		.clone()
		.ok_or_else(|| tg::error!("expected the runner's token"))?;

	// Open the runner's control connection.
	let session = server
		.client()?
		.session(&tg::Context::new(Some(token.clone())));
	let (sender, receiver) =
		futures::channel::mpsc::unbounded::<tg::Result<tg::runner::control::ClientMessage>>();
	let arg = tg::runner::control::Arg {
		first: false,
		heartbeat: tg::runner::control::HeartbeatClientNotification::default(),
		host: "test".to_owned(),
		id: runner.data.id.clone(),
		location: None,
		sandboxes: Vec::new(),
		scheduler_time_to_live: std::time::Duration::from_secs(300),
	};
	let (_, stream) = match session
		.get_runner_control_stream(arg, receiver.boxed())
		.await?
	{
		ControlFlow::Break(output) => output,
		ControlFlow::Continue(error) => return Err(error),
	};

	// Connect a sandbox on the runner, and wait for the index to record it.
	let arg = sandbox_control_arg(&runner.data.id, Some(&runner.data.id));
	let sandbox = arg
		.id
		.clone()
		.ok_or_else(|| tg::error!("expected the sandbox's id"))?;
	connect_sandbox(&server, &token, arg).await?;
	server.tg(Some(ROOT_TOKEN), ["index"]).await?;
	let data = sandbox_data(&server, &sandbox.to_string()).await?;
	assert_eq!(data["status"], "started");

	Ok((server, stream.boxed(), sandbox, sender))
}

/// Watch a checkpoint, and return the watch.
async fn watch_checkpoint(server: &Server, checkpoint: &str) -> tg::Result<String> {
	let output = server
		.tg(Some(ROOT_TOKEN), ["checkpoint", "watch", checkpoint])
		.await?;
	let output = serde_json::from_str::<serde_json::Value>(&output)
		.map_err(|error| tg::error!(!error, "failed to deserialize the watch"))?;
	let watch = match &output["watch"] {
		serde_json::Value::String(watch) => watch.clone(),
		watch => watch.to_string(),
	};

	Ok(watch)
}

/// Get a sandbox's data.
async fn sandbox_data(server: &Server, sandbox: &str) -> tg::Result<serde_json::Value> {
	let output = server
		.tg(Some(ROOT_TOKEN), ["sandbox", "get", sandbox])
		.await?;
	let mut output = serde_json::from_str::<serde_json::Value>(&output)
		.map_err(|error| tg::error!(!error, "failed to deserialize the sandbox"))?;
	let data = output["data"].take();

	Ok(data)
}

/// Create the arg for a new local sandbox on the runner, with an ID that the deriving runner derives if one is given, and a new ID otherwise.
fn sandbox_control_arg(
	runner: &tg::runner::Id,
	deriver: Option<&tg::runner::Id>,
) -> tg::sandbox::control::Arg {
	let (id, proof) = match deriver {
		Some(deriver) => {
			let (id, proof) = tg::sandbox::Id::with_runner(deriver);
			(id, Some(proof))
		},
		None => (tg::sandbox::Id::new(), None),
	};
	let location = tg::Location::Local(tg::location::Local::default());
	tg::sandbox::control::Arg {
		boot: 0,
		create: Some(tg::sandbox::create::Arg::default()),
		created_at: None,
		creator: tg::Principal::Root,
		id: Some(id),
		location: Some(location.into()),
		owner: tg::Principal::Root,
		proof,
		runner: Some(runner.clone()),
		sequence: 1,
	}
}

/// Open a sandbox control connection, and return its header.
async fn connect_sandbox(
	server: &Server,
	token: &str,
	arg: tg::sandbox::control::Arg,
) -> tg::Result<tg::sandbox::control::Header> {
	let session = server
		.client()?
		.session(&tg::Context::new(Some(token.to_owned())));
	let stream = futures::stream::pending().boxed();
	match session.get_sandbox_control_stream(arg, stream).await? {
		ControlFlow::Break((header, _)) => Ok(header),
		ControlFlow::Continue(error) => Err(error),
	}
}
