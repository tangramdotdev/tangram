use {
	futures::{SinkExt as _, StreamExt as _, channel::mpsc},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_tests_util::{Config, Temp, cli::Server},
};

/// Destroy sandbox requests on a runner control connection enforce runner ownership and are idempotent.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn destroy_sandbox_requires_ownership() -> tg::Result<()> {
	let tangram = env!("CARGO_BIN_EXE_tangram");
	let Some(quota) =
		std::env::var_os("TANGRAM_TEST_PROJECT_QUOTA_PATH").filter(|path| !path.is_empty())
	else {
		eprintln!(
			"skipping the test because TANGRAM_TEST_PROJECT_QUOTA_PATH is required for sandbox storage accounting"
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
		"scheduler": { "runner_ttl": 300 },
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
	let directory = Temp::with_parent(std::path::Path::new(&quota))
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

	// Another runner neither hosts the sandbox nor can destroy it.
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

	// The runner that hosts the sandbox destroys it once, after it crashes.
	runner.kill().await?;
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

/// Open a runner control connection as the runner, check whether the server says that the runner hosts the sandbox, and request that the sandbox be destroyed twice. Return whether each request destroyed it.
async fn destroy_sandbox(
	server: &Server,
	runner: &tg::runner::create::Output,
	sandbox: &tg::sandbox::Id,
	hosts: bool,
) -> tg::Result<(bool, bool)> {
	// Open the connection.
	let token = runner
		.token
		.token
		.clone()
		.ok_or_else(|| tg::error!("expected the runner's token"))?;
	let session = server.client()?.session(&tg::Context::new(Some(token)));
	let (mut sender, receiver) =
		mpsc::unbounded::<tg::Result<tg::runner::control::ClientMessage>>();
	let arg = tg::runner::control::Arg {
		heartbeat: tg::runner::control::HeartbeatClientNotification::default(),
		host: "test".to_owned(),
		id: runner.data.id.clone(),
		location: None,
		scheduler_ttl: std::time::Duration::from_secs(300),
	};
	let (header, stream) = match session
		.get_runner_control_stream(arg, receiver.boxed())
		.await?
	{
		ControlFlow::Break(output) => output,
		ControlFlow::Continue(error) => return Err(error),
	};
	assert_eq!(
		header.sandboxes.contains(sandbox),
		hosts,
		"the header should list the sandbox only for its runner"
	);
	let mut stream = std::pin::pin!(stream);

	// Request that the sandbox be destroyed twice.
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
		create: Some(tg::sandbox::create::Arg::default()),
		created_at: None,
		creator: None,
		id: Some(id),
		location: Some(location.into()),
		proof,
		runner: Some(runner.clone()),
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
