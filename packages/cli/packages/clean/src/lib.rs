use {
	num::ToPrimitive as _, std::os::unix::fs::PermissionsExt as _, tangram_cli_core::Cli,
	tangram_client::prelude::*, tangram_futures::stream::Ext as _,
};

/// Remove unused sandboxes, processes, and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long, short)]
	force: bool,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	if args.force {
		run_force(cli).await?;
		return Ok(());
	}

	let client = cli.client().await?;

	// Index.
	let arg = tg::index::Arg::default();
	let stream = client
		.index(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to index"))?;
	cli.render_progress_stream(stream).await?;

	// Clean.
	let stream = client
		.clean()
		.await
		.map_err(|error| tg::error!(!error, "failed to clean"))?;
	let output = cli.render_progress_stream(stream).await?;
	let bytes =
		byte_unit::Byte::from_u64(output.bytes).get_appropriate_unit(byte_unit::UnitType::Decimal);
	let message = format!(
		"cleaned {} objects ({bytes:#.1}), {} processes, {} sandboxes, {} tags",
		output.objects, output.processes, output.sandboxes, output.tags,
	);
	cli.print_info_message(&message);

	Ok(())
}

pub async fn run_force(cli: &mut Cli) -> tg::Result<()> {
	let path = cli.directory_path();
	let store_path = path.join("store");

	// Stop the server if it is running.
	cli.stop_server().await.ok();

	// Force unmount the VFS store path if it is mounted.
	force_unmount_vfs(&store_path).await;

	// Resolve the progress configuration.
	let progress_config = cli
		.config
		.as_ref()
		.and_then(|config| config.progress)
		.unwrap_or_default();
	let channel_capacity = progress_config
		.channel_capacity
		.unwrap_or(tg::progress::DEFAULT_CHANNEL_CAPACITY);
	let interval = progress_config
		.interval
		.unwrap_or_else(|| tg::progress::Config::default().interval);

	// Create the progress handle.
	let progress = tg::progress::Handle::<()>::new(channel_capacity);

	// Spawn the blocking task to clean the directory.
	let progress_ = progress.clone();
	let task = tokio::task::spawn_blocking(move || {
		if !path.exists() {
			return Ok(());
		}

		progress_.start(
			"cleaning".to_owned(),
			"cleaning".to_owned(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
		let mut paths = Vec::new();
		let mut stack = vec![path];
		while let Some(path) = stack.pop() {
			let metadata = std::fs::symlink_metadata(&path)
				.map_err(|error| tg::error!(!error, "failed to get the metadata"))?;
			if !metadata.is_symlink() {
				let mut perms = metadata.permissions();
				#[cfg(unix)]
				{
					if metadata.is_dir() {
						let mode = perms.mode();
						perms.set_mode(mode | 0o700);
						std::fs::set_permissions(&path, perms)
							.map_err(|error| tg::error!(!error, "failed to set the permissions"))?;
					}
				}
				#[cfg(not(unix))]
				{
					perms.set_readonly(false);
					std::fs::set_permissions(&path, perms)
						.map_err(|error| tg::error!(!error, "failed to set the permissions"))?;
				}
			}
			if metadata.is_dir() {
				let entries = std::fs::read_dir(&path)
					.map_err(|error| tg::error!(!error, "failed to read the directory"))?;
				for entry in entries {
					let entry = entry
						.map_err(|error| tg::error!(!error, "failed to read the directory"))?;
					stack.push(entry.path());
				}
			}
			paths.push((path, metadata));
			progress_.increment("cleaning", 1);
		}
		progress_.finish("cleaning");

		progress_.start(
			"cleaning".to_owned(),
			"cleaning".to_owned(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			Some(paths.len().to_u64().unwrap()),
		);
		for (path, metadata) in paths.into_iter().rev() {
			if metadata.is_dir() {
				std::fs::remove_dir(&path)
					.map_err(|error| tg::error!(!error, "failed to remove the directory"))?;
			} else {
				std::fs::remove_file(&path)
					.map_err(|error| tg::error!(!error, "failed to remove the file"))?;
			}
			progress_.increment("cleaning", 1);
		}
		progress_.finish("cleaning");

		Ok::<_, tg::Error>(())
	});
	let progress_ = progress.clone();
	let task = tokio::spawn(async move {
		match task.await {
			Ok(Ok(())) => {
				progress_.output(());
			},
			Ok(Err(error)) => {
				progress_.error(error);
			},
			Err(error) => {
				progress_.error(tg::error!(!error, "the clean task panicked"));
			},
		}
	});

	// Render the progress stream.
	cli.render_progress_stream(progress.stream(interval).attach(task))
		.await?;

	Ok(())
}

async fn force_unmount_vfs(path: &std::path::Path) {
	#[cfg(target_os = "linux")]
	{
		tokio::process::Command::new("fusermount3")
			.args(["-u", "-z"])
			.arg(path)
			.stdin(std::process::Stdio::null())
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::null())
			.status()
			.await
			.ok();
	}
	#[cfg(target_os = "macos")]
	{
		tokio::process::Command::new("umount")
			.arg("-f")
			.arg(path)
			.stdin(std::process::Stdio::null())
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::null())
			.status()
			.await
			.ok();
	}
}
