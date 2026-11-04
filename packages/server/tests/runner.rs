use {
	futures::{StreamExt as _, channel::mpsc},
	std::{collections::BTreeMap, ops::ControlFlow, time::Duration},
	tangram_client::prelude::*,
	tangram_tests_util::{Config, server::Server},
};

#[tokio::test]
async fn an_older_registration_cannot_replace_a_newer_registration_index() -> tg::Result<()> {
	// Start a server and create a runner whose connections the test controls.
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"advanced": { "checkpoints": true },
		"authentication": { "root": { "token": "root" } },
	}));
	let server = Server::start(config).await?;
	let session = server
		.client()?
		.session(&tg::Context::new(Some("root".to_owned())));
	let runner = session
		.create_runner(tg::runner::create::Arg::default())
		.await?;

	// Hold the first response immediately before it records ownership.
	let checkpoint = "runner.registration.accept";
	let arg = tg::checkpoint::watch::Arg {
		params: BTreeMap::from([
			("registration_index".to_owned(), 1.into()),
			("runner".to_owned(), runner.data.id.to_string().into()),
		]),
	};
	let watch = session
		.try_watch_checkpoint(checkpoint, arg)
		.await?
		.ok_or_else(|| tg::error!("missing the checkpoint"))?
		.watch;
	let (first_sender, first_receiver) = mpsc::unbounded();
	let first = tokio::spawn({
		let session = session.clone();
		let runner = runner.data.id.clone();
		async move {
			session
				.get_runner_control_stream(control_arg(runner, 1), first_receiver.boxed())
				.await
		}
	});
	session
		.try_wait_checkpoint_hit(checkpoint, watch, 0)
		.await?
		.ok_or_else(|| tg::error!("missing the checkpoint hit"))?;

	// Accept the newer scheduler index on the same scheduler before releasing the first.
	let (second_sender, second_receiver) = mpsc::unbounded();
	let second = session
		.get_runner_control_stream(
			control_arg(runner.data.id.clone(), 2),
			second_receiver.boxed(),
		)
		.await?;
	let ControlFlow::Break((_, second_stream)) = second else {
		return Err(tg::error!("the newer registration was not accepted"));
	};
	session.try_unwatch_checkpoint(checkpoint, watch).await?;
	let result = first
		.await
		.map_err(|error| tg::error!(!error, "the first registration task panicked"))?;
	let Err(error) = result else {
		return Err(tg::error!("the older registration should be rejected"));
	};
	assert!(
		error
			.trace()
			.to_string()
			.contains("the runner registration was superseded"),
		"{error}"
	);

	// Reject a stale request before it can replace the scheduler's current in-memory connection.
	let (_sender, receiver) = mpsc::unbounded();
	let result = session
		.get_runner_control_stream(control_arg(runner.data.id, 1), receiver.boxed())
		.await;
	assert!(
		result.is_err(),
		"an older scheduler index should not reconnect"
	);

	drop((first_sender, second_sender, second_stream));
	server.stop().await?;

	Ok(())
}

fn control_arg(id: tg::runner::Id, registration_index: u64) -> tg::runner::control::Arg {
	tg::runner::control::Arg {
		heartbeat: tg::runner::control::HeartbeatClientNotification::default(),
		host: "test".to_owned(),
		id,
		location: None,
		sandboxes: Vec::new(),
		registration_index,
		scheduler_ttl: Duration::from_secs(300),
	}
}
