use {
	std::{
		io,
		os::{
			fd::AsRawFd as _,
			unix::{net::UnixStream, process::CommandExt as _},
		},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_tests_util::{Config, Temp, server::Server},
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
};

#[tokio::test]
async fn external_checkout_waits_for_inherited_writers() -> tg::Result<()> {
	checkout(true).await?;
	Ok(())
}

#[tokio::test]
async fn internal_checkout_waits_for_inherited_writers() -> tg::Result<()> {
	checkout(false).await?;
	Ok(())
}

async fn checkout(external: bool) -> tg::Result<()> {
	// Start the server and store an executable that has not been checked out.
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"advanced": { "checkpoints": true },
		"authentication": { "root": { "token": "root" } },
		"runner": { "sandbox_pool_size": 0 },
		"vfs": false,
	}));
	let server = Server::start(config).await?;
	let session = server
		.client()?
		.session(&tg::Context::new(Some("root".to_owned())));
	let file = tg::File::builder()
		.contents("#!/bin/sh\nprintf 'checkout ready\\n'\n")
		.executable(true)
		.build()?;
	let id = file.store_with_instance(&session).await?;
	let temp = Temp::new().unwrap();

	// Hold the checkout while its file is open, and again before it publishes the result.
	let mut watches = Vec::new();
	for checkpoint in ["checkout.file.write", "checkout.publish"] {
		let arg = tg::checkpoint::watch::Arg {
			params: [("id".to_owned(), id.to_string().into())].into(),
		};
		let watch = session
			.try_watch_checkpoint(checkpoint, arg)
			.await?
			.unwrap()
			.watch;
		watches.push((checkpoint, watch));
	}
	let mut checkout = tokio::spawn({
		let session = session.clone();
		let path = external.then(|| temp.path().join("script"));
		async move {
			let arg = tg::checkout::Arg {
				dependencies: true,
				extension: None,
				force: false,
				lock: None,
				nodes: vec![tg::Referent::with_node(id.into())],
				path,
			};
			let path = tg::checkout::checkout_one_with_instance(&session, arg).await?;
			let output = tangram_util::spawn::output_async(&mut tokio::process::Command::new(path))
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the checked out file"))?;
			Ok::<_, tg::Error>(output)
		}
	});
	let (writing, writing_watch) = watches[0];
	session
		.try_wait_checkpoint_hit(writing, writing_watch, 0)
		.await?
		.unwrap();

	// Fork with the checkout descriptor open and hold the child before exec using only async-signal-safe operations.
	let (host, guest) = UnixStream::pair().unwrap();
	host.set_nonblocking(true).unwrap();
	let mut host = tokio::net::UnixStream::from_std(host).unwrap();
	let mut command = std::process::Command::new("true");
	// SAFETY: The closure only reads and writes a live socket and constructs errors without allocating.
	unsafe {
		command.pre_exec(move || {
			let fd = guest.as_raw_fd();
			let mut byte = 0u8;
			if libc::write(fd, (&raw const byte).cast(), 1) != 1 {
				return Err(io::Error::last_os_error());
			}
			if libc::read(fd, (&raw mut byte).cast(), 1) != 1 {
				return Err(io::Error::from_raw_os_error(libc::EPIPE));
			}
			Ok(())
		});
	}
	let spawner = std::thread::spawn(move || tangram_util::spawn::spawn(&mut command));
	host.read_u8().await.unwrap();
	session
		.try_unwatch_checkpoint(writing, writing_watch)
		.await?;
	let (publishing, publishing_watch) = watches[1];
	session
		.try_wait_checkpoint_hit(publishing, publishing_watch, 0)
		.await?
		.unwrap();
	session
		.try_unwatch_checkpoint(publishing, publishing_watch)
		.await?;

	// Keep the inherited descriptor open until the checkout has had an opportunity to return incorrectly.
	let early = tokio::time::timeout(Duration::from_millis(100), &mut checkout).await;
	let returned_early = early.is_ok();
	host.write_u8(0).await.unwrap();
	let mut child = spawner.join().unwrap().unwrap();
	child.wait().unwrap();
	let output = match early {
		Ok(output) => output,
		Err(_) => checkout.await,
	}
	.unwrap()?;

	// Verify the completed checkout can be executed immediately.
	assert!(
		!returned_early,
		"the checkout returned while a child held its file open for writing"
	);
	assert!(output.status.success(), "{output:?}");
	assert_eq!(output.stdout, b"checkout ready\n");
	server.stop().await?;

	Ok(())
}
