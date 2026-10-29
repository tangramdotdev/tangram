use {
	futures::StreamExt as _,
	std::{ops::ControlFlow, time::Duration},
	tangram_client::prelude::*,
	tangram_tests_util::{Config, cli::Server},
};

/// A runner must not start a shortcut child of a process that it does not run, since the child would receive the parent's authority over the command's objects, or in a sandbox that it does not host, since the sandbox's owner is billed for the child.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn shortcut_start_requires_proof() -> tg::Result<()> {
	// Start a server with users.
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"authentication": { "users": { "providers": { "insecure": true } } },
	}));
	let server = Server::start(env!("CARGO_BIN_EXE_tangram"), config).await?;
	let alice = server.login("alice").await?;
	let mallory = server.login("mallory").await?;
	let mallory_principal: tg::principal::Selector =
		tg::Principal::User(mallory.user.id.clone()).into();

	// Alice runs a sandboxed process whose command references her private file.
	let secret = server
		.tg(
			Some(&alice.token),
			["put", "--no-tokens", r#"tg.file("shortcutsecret")"#],
		)
		.await?;
	let parent = spawn(&server, &alice.token, ["--executable", &secret]).await?;
	let parent_string = parent.to_string();
	server
		.output(Some(&alice.token), ["wait", &parent_string])
		.await?;
	server.tg(None, ["index"]).await?;
	let data = server
		.get::<tg::process::Data>(&alice.token, &parent_string)
		.await?;
	let readable = server
		.output(Some(&mallory.token), ["get", &secret])
		.await?;
	assert!(
		!readable.status.success(),
		"Mallory should not read Alice's private file"
	);

	// Mallory's runner tries to start a shortcut child that names Alice's process as its parent.
	let runner = server
		.create_runner(&mallory.token, Some(&mallory_principal))
		.await?;
	let lease = lease(&parent)?;
	let shortcut = tg::process::control::Shortcut {
		parent: tg::process::control::Parent {
			id: parent.clone(),
			token: Some(mallory.token.clone()),
		},
		sandbox: tg::process::control::Sandbox {
			create: None,
			token: Some(mallory.token.clone()),
		},
	};

	// The runner must not start a child whose ID it did not derive, or whose ID another runner derived.
	let runner_token = runner
		.token
		.token
		.ok_or_else(|| tg::error!("expected the runner's token"))?;
	let start = start_arg(&data, &lease, Some(shortcut.clone()));
	let (_, result) = start_process(&server, &runner_token, start, None).await?;
	assert!(
		result.is_err(),
		"a runner should not start a process whose ID it did not derive"
	);
	let other = server
		.create_runner(&mallory.token, Some(&mallory_principal))
		.await?;
	let start = start_arg(&data, &lease, Some(shortcut.clone()));
	let (_, result) = start_process(&server, &runner_token, start, Some(&other.data.id)).await?;
	let error =
		result.expect_err("a runner should not start a process whose ID another runner derived");
	let message = error.trace().to_string();
	assert!(message.contains("did not derive"), "{message}");

	// The runner must not start the child without a proof, or with a token that is not the parent's.
	for (shortcut, expected) in [
		(None, "must prove its parent and sandbox".to_owned()),
		(Some(shortcut.clone()), parent.to_string()),
	] {
		let start = start_arg(&data, &lease, shortcut);
		let (child, result) =
			start_process(&server, &runner_token, start, Some(&runner.data.id)).await?;
		let error = result.expect_err(
			"a runner should not start a shortcut child of a process that it does not run",
		);
		let message = error.trace().to_string();
		assert!(message.contains(&expected), "{message}");

		// A request that claims the rejected child fails with the rejection instead of waiting for the child's control service.
		let claim = tokio::time::timeout(
			Duration::from_secs(5),
			claim_process_status(&server, &runner_token, &child),
		)
		.await
		.map_err(|_| tg::error!("the claim should fail quickly"))?;
		let error = claim.expect_err("the claim of a rejected child should fail");
		let message = error.trace().to_string();
		assert!(message.contains("rejected"), "{message}");
	}

	// Mallory runs her own process, whose token proves that her runner runs it.
	let own = spawn(
		&server,
		&mallory.token,
		[
			"--executable",
			"/bin/sh",
			"--",
			"-c",
			r#"echo "$TANGRAM_TOKEN"; sleep 60"#,
		],
	)
	.await?;
	let own_string = own.to_string();
	tokio::time::timeout(
		Duration::from_secs(30),
		server.tg(
			Some(&mallory.token),
			["log", "--no-timeout", "--length", "1", &own_string],
		),
	)
	.await
	.map_err(|_| tg::error!("the process should log its token"))??;
	let own_token = server
		.tg(Some(&mallory.token), ["log", &own_string])
		.await?;

	// The runner must not start a child of its own process in Alice's sandbox with a token that is not the sandbox's.
	let shortcut = tg::process::control::Shortcut {
		parent: tg::process::control::Parent {
			id: own.clone(),
			token: Some(own_token.clone()),
		},
		sandbox: tg::process::control::Sandbox {
			create: None,
			token: Some(own_token),
		},
	};
	let start = start_arg(&data, &lease, Some(shortcut));
	let (_, result) = start_process(&server, &runner_token, start, Some(&runner.data.id)).await?;
	let error = result.expect_err(
		"a runner should not start a shortcut child in a sandbox that it does not host",
	);
	let sandbox = data
		.sandbox
		.clone()
		.ok_or_else(|| tg::error!("expected a sandbox"))?;
	let message = error.trace().to_string();
	assert!(message.contains(&sandbox.to_string()), "{message}");
	server
		.output(Some(&mallory.token), ["cancel", &own_string])
		.await?;

	server.stop().await?;

	Ok(())
}

/// A process that starts after its parent finished does not receive the parent's authority over the command's objects.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn finished_parent_child() -> tg::Result<()> {
	// Start a server with users and a root token.
	let root_token = "root".to_owned();
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"authentication": {
			"root": { "token": root_token },
			"users": { "providers": { "insecure": true } },
		},
		"sync": { "service": { "request_timeout": 5 } },
	}));
	let server = Server::start(env!("CARGO_BIN_EXE_tangram"), config).await?;
	let alice = server.login("alice").await?;

	// Alice runs a sandboxed process whose command references her private file, and it finishes without children.
	let secret = server
		.tg(
			Some(&alice.token),
			["put", "--no-tokens", r#"tg.file("finishedparentsecret")"#],
		)
		.await?;
	let parent = spawn(&server, &alice.token, ["--executable", &secret]).await?;
	let parent_string = parent.to_string();
	server
		.output(Some(&alice.token), ["wait", &parent_string])
		.await?;
	server.tg(None, ["index"]).await?;
	let mut data = server
		.get::<tg::process::Data>(&alice.token, &parent_string)
		.await?;
	assert!(matches!(data.status, tg::process::Status::Finished));

	// Start a shortcut child of the finished parent as the root, which is not asked to prove the parent.
	data.children = None;
	data.error = None;
	data.exit = None;
	data.finished_at = None;
	data.log = None;
	data.output = None;
	data.status = tg::process::Status::Started;
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
	let start = start_arg(&data, &lease(&parent)?, Some(shortcut));
	let (_, result) = start_process(&server, &root_token, start, None).await?;
	let header = result?;
	let child_token = header
		.token
		.ok_or_else(|| tg::error!("expected the child's token"))?;
	server.tg(None, ["index"]).await?;

	// The child cannot read the private file that its finished parent could read, once its unserved command sync cannot prove the file.
	let readable = server.output(Some(&child_token), ["get", &secret]).await?;
	assert!(
		!readable.status.success(),
		"the child of a finished parent should not read the parent's objects"
	);

	server.stop().await?;

	Ok(())
}

/// Spawn a sandboxed process and return its ID.
async fn spawn<'a>(
	server: &Server,
	token: &str,
	args: impl IntoIterator<Item = &'a str>,
) -> tg::Result<tg::process::Id> {
	let args = ["--sandbox"].into_iter().chain(args);
	let output = server.spawn(token, args).await?;
	let process = output
		.process
		.right()
		.ok_or_else(|| tg::error!("expected a process ID"))?;

	Ok(process)
}

/// Get the lease that a runner holds for the process, which is the process ID's body.
fn lease(process: &tg::process::Id) -> tg::Result<tg::process::Lease> {
	let id = tg::Id::from(process.clone());
	let lease = tg::id::ENCODING.encode(id.body().as_slice()).parse()?;

	Ok(lease)
}

fn start_arg(
	data: &tg::process::Data,
	lease: &tg::process::Lease,
	shortcut: Option<tg::process::control::Shortcut>,
) -> tg::process::control::StartClientRequestArg {
	tg::process::control::StartClientRequestArg {
		data: data.clone(),
		lease: *lease,
		options: tg::referent::Options::default(),
		shortcut,
	}
}

/// Open a process control stream that starts a process as a runner, and return the process's ID with the response header or the rejection. The process gets an ID that the runner derives when a runner is given, and a new ID otherwise.
async fn start_process(
	server: &Server,
	token: &str,
	start: tg::process::control::StartClientRequestArg,
	runner: Option<&tg::runner::Id>,
) -> tg::Result<(tg::process::Id, tg::Result<tg::process::control::Header>)> {
	let (id, proof) = match runner {
		Some(runner) => {
			let (id, proof) = tg::process::Id::with_runner(runner);
			(id, Some(proof))
		},
		None => (tg::process::Id::new(), None),
	};
	let location = tg::Location::Local(tg::location::Local::default());
	let arg = tg::process::control::Arg {
		id: id.clone(),
		location: Some(location.into()),
		mode: tg::process::control::Mode::Start(start),
		proof,
	};
	let client = server.client()?;
	let context = tg::Context::new(Some(token.to_owned()));
	let session = client.session(&context);
	let stream = futures::stream::pending().boxed();
	let result = match session.try_get_process_control_stream(arg, stream).await {
		Ok(ControlFlow::Break(Some((header, _)))) => Ok(header),
		Ok(ControlFlow::Break(None)) => Err(tg::error!("the process control stream was not found")),
		Ok(ControlFlow::Continue(error)) | Err(error) => Err(error),
	};

	Ok((id, result))
}

/// Get a process's status with a request that claims the process with the runner's token.
async fn claim_process_status(
	server: &Server,
	token: &str,
	process: &tg::process::Id,
) -> tg::Result<()> {
	let client = server.client()?;
	let mut context = tg::Context::new(Some(token.to_owned()));
	context.set_process(Some(process.clone()));
	let session = client.session(&context);
	let process = tg::Either::Right(process.clone());
	let stream = session
		.try_get_process_status_stream(&process, tg::process::status::Arg::default())
		.await?
		.ok_or_else(|| tg::error!("expected the process status"))?;
	let mut stream = std::pin::pin!(stream);
	if let Some(event) = stream.next().await {
		event?;
	}

	Ok(())
}
