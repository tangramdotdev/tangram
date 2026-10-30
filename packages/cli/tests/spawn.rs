use {
	std::{
		sync::{
			Arc, Mutex,
			atomic::{AtomicBool, Ordering},
		},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_tests_util::{Config, Instance, cli::Server, instance},
};

/// A spawn succeeds when the server loses the process's connected message, as happens when its messenger connection drops while the message is published.
#[tokio::test]
async fn lost_connected_message() -> tg::Result<()> {
	// Create a cloud instance whose servers share the NATS messenger.
	let tangram = env!("CARGO_BIN_EXE_tangram");
	let root_token = "root";
	let config = serde_json::json!({
		"advanced": { "checkpoints": true },
		"authentication": { "root": { "token": root_token }, "users": { "providers": { "insecure": true } } },
	});
	let arg = instance::Arg {
		config: config.as_object().unwrap().clone(),
		..Default::default()
	};
	let Some(instance) = Instance::cloud(arg).await? else {
		return Ok(());
	};

	// Start the scheduler server on the messenger directly.
	let mut config = instance.config(None)?;
	config.merge(serde_json::json!({ "roles": ["api", "indexer", "scheduler"] }));
	let remote = Server::start_with_instance(tangram, &instance, config).await?;

	// Start the API server with its messenger connection routed through a proxy that the test can sever.
	let proxy = Proxy::start("127.0.0.1:4222").await?;
	let mut config = instance.config(None)?;
	config.merge(serde_json::json!({
		"messenger": { "kind": "nats", "url": proxy.url },
		"roles": ["api", "indexer"],
	}));
	let api = Server::start_with_instance(tangram, &instance, config).await?;

	// Start a runner against the scheduler server.
	let created = remote.create_runner(root_token, None).await?;
	let runner_token = created
		.token
		.token
		.clone()
		.ok_or_else(|| tg::error!("expected the runner's token"))?;
	let remote_url = remote.client()?.url().to_string();
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"remotes": { "default": { "token": runner_token, "url": remote_url } },
		"roles": ["api", "indexer", "runner"],
		"runner": { "id": created.data.id.to_string(), "remote": "default", "token": runner_token },
	}));
	let runner = Server::start(tangram, config).await?;
	let alice = remote.login("alice").await?;

	// Run a command through the API server without interference to prove the topology.
	let command = command(&api, Some(&alice.token)).await?;
	let output = api
		.spawn(&alice.token, ["--cached=false", "--sandbox", &command.to_string()])
		.await?;
	let process = output
		.process
		.right()
		.ok_or_else(|| tg::error!("expected the process's ID"))?;
	api.tg(Some(&alice.token), ["wait", &process.to_string()])
		.await?;

	// Watch the checkpoints around the spawn's wait for the process attachment and the connected message's publish.
	let wait = api
		.watch_checkpoint(
			Some(root_token),
			"process.spawn.connection.wait",
			serde_json::json!({}),
		)
		.await?;
	let submitted = remote
		.watch_checkpoint(
			Some(root_token),
			"process.control.index.submitted",
			serde_json::json!({}),
		)
		.await?;
	let header = remote
		.watch_checkpoint(
			Some(root_token),
			"process.control.header",
			serde_json::json!({}),
		)
		.await?;

	// Start a spawn through the API server.
	let mut spawn = api.command()?;
	spawn
		.arg("--token")
		.arg(&alice.token)
		.args(["spawn", "--no-tokens", "--verbose", "--cached=false"])
		.arg("--sandbox")
		.arg(command.to_string());
	let spawn = tokio::spawn(async move { spawn.output().await });

	// Wait for the spawn to observe the sandbox connection, and for the process control connection to be about to publish the connected message.
	api.wait_checkpoint(Some(root_token), "process.spawn.connection.wait", wait, 0)
		.await?;
	remote
		.wait_checkpoint(
			Some(root_token),
			"process.control.index.submitted",
			submitted,
			0,
		)
		.await?;

	// Sever the API server's messenger connection while the connected message is published, so that the message is lost.
	proxy.sever();
	remote
		.continue_checkpoint(
			Some(root_token),
			"process.control.index.submitted",
			submitted,
			0,
		)
		.await?;
	remote
		.wait_checkpoint(Some(root_token), "process.control.header", header, 0)
		.await?;
	proxy.restore();
	remote
		.continue_checkpoint(Some(root_token), "process.control.header", header, 0)
		.await?;
	api.continue_checkpoint(Some(root_token), "process.spawn.connection.wait", wait, 0)
		.await?;
	remote
		.unwatch_checkpoint(Some(root_token), "process.control.index.submitted", submitted)
		.await?;
	remote
		.unwatch_checkpoint(Some(root_token), "process.control.header", header)
		.await?;
	api.unwatch_checkpoint(Some(root_token), "process.spawn.connection.wait", wait)
		.await?;

	// The spawn succeeds even though the connected message was lost, and the process completes.
	let output = tokio::time::timeout(Duration::from_secs(120), spawn)
		.await
		.map_err(|_| tg::error!("the spawn should complete"))?
		.map_err(|error| tg::error!(!error, "the spawn task panicked"))?
		.map_err(|error| tg::error!(!error, "failed to run the spawn"))?;
	assert!(
		output.status.success(),
		"the spawn should succeed when the connected message is lost: {}",
		String::from_utf8_lossy(&output.stderr),
	);
	let output = serde_json::from_slice::<tg::process::spawn::Output>(&output.stdout)
		.map_err(|error| tg::error!(!error, "failed to deserialize the spawn output"))?;
	let process = output
		.process
		.right()
		.ok_or_else(|| tg::error!("expected the process's ID"))?;
	api.tg(Some(&alice.token), ["wait", &process.to_string()])
		.await?;

	runner.stop().await?;
	api.stop().await?;
	remote.stop().await?;

	Ok(())
}

/// A TCP proxy in front of the NATS server that the test severs and restores to simulate a messenger disconnection.
struct Proxy {
	connections: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
	enabled: Arc<AtomicBool>,
	task: tokio::task::JoinHandle<()>,
	url: String,
}

impl Proxy {
	async fn start(target: &str) -> tg::Result<Self> {
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
			.await
			.map_err(|error| tg::error!(!error, "failed to bind the proxy listener"))?;
		let port = listener
			.local_addr()
			.map_err(|error| tg::error!(!error, "failed to get the proxy address"))?
			.port();
		let url = format!("nats://127.0.0.1:{port}");
		let connections = Arc::new(Mutex::new(Vec::new()));
		let enabled = Arc::new(AtomicBool::new(true));
		let task = tokio::spawn({
			let connections = connections.clone();
			let enabled = enabled.clone();
			let target = target.to_owned();
			async move {
				while let Ok((mut inbound, _)) = listener.accept().await {
					if !enabled.load(Ordering::SeqCst) {
						continue;
					}
					let Ok(mut outbound) = tokio::net::TcpStream::connect(&target).await else {
						continue;
					};
					let connection = tokio::spawn(async move {
						tokio::io::copy_bidirectional(&mut inbound, &mut outbound)
							.await
							.ok();
					});
					connections.lock().unwrap().push(connection);
				}
			}
		});
		let proxy = Self {
			connections,
			enabled,
			task,
			url,
		};

		Ok(proxy)
	}

	/// Drop the proxied connections and refuse new ones.
	fn sever(&self) {
		self.enabled.store(false, Ordering::SeqCst);
		for connection in self.connections.lock().unwrap().drain(..) {
			connection.abort();
		}
	}

	/// Accept connections again.
	fn restore(&self) {
		self.enabled.store(true, Ordering::SeqCst);
	}
}

impl Drop for Proxy {
	fn drop(&mut self) {
		self.task.abort();
		for connection in self.connections.lock().unwrap().drain(..) {
			connection.abort();
		}
	}
}

/// Put a command that runs a shell script that exits successfully, and return its ID.
async fn command(server: &Server, token: Option<&str>) -> tg::Result<tg::command::Id> {
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
	Ok(command)
}

/// Get the node of a referent.
fn node(referent: &str) -> &str {
	referent.split('?').next().unwrap_or(referent)
}
