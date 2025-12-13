use {
	crate::Cli, num::ToPrimitive, std::os::unix::fs::PermissionsExt as _,
	tangram_client::prelude::*,
};

/// Remove unused processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long, short)]
	force: bool,
}

impl Cli {
	pub async fn command_clean(&mut self, args: Args) -> tg::Result<()> {
		if args.force {
			self.command_clean_force().await?;
			return Ok(());
		}

		let handle = self.handle().await?;

		// Index.
		let stream = handle.index().await?;
		self.render_progress_stream(stream).await?;

		// Clean.
		let stream = handle.clean().await?;
		let output = self.render_progress_stream(stream).await?;
		let bytes = byte_unit::Byte::from_u64(output.bytes)
			.get_appropriate_unit(byte_unit::UnitType::Decimal);
		let message = format!(
			"cleaned {} processes, {} objects, {bytes:#.1}",
			output.processes, output.objects,
		);
		Self::print_info_message(&message);

		Ok(())
	}

	pub async fn command_clean_force(&mut self) -> tg::Result<()> {
		let path = self.directory_path();

		// Stop the server if it is running.
		self.stop_server().await.ok();

		// Create the progress handle.
		let progress = tangram_server::progress::Handle::<()>::new();

		// Spawn the blocking task to clean the directory.
		let progress_ = progress.clone();
		tokio::task::spawn_blocking(move || {
			if !path.exists() {
				progress_.output(());
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
					.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
				if !metadata.is_symlink() {
					let mut perms = metadata.permissions();
					#[cfg(unix)]
					{
						let mode = perms.mode();
						if metadata.is_dir() {
							perms.set_mode(mode | 0o700);
						} else {
							perms.set_mode(mode | 0o600);
						}
					}
					#[cfg(not(unix))]
					{
						perms.set_readonly(false);
					}
					std::fs::set_permissions(&path, perms)
						.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
				}
				if metadata.is_dir() {
					let entries = std::fs::read_dir(&path)
						.map_err(|source| tg::error!(!source, "failed to read the directory"))?;
					for entry in entries {
						let entry = entry.map_err(|source| {
							tg::error!(!source, "failed to read the directory")
						})?;
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
						.map_err(|source| tg::error!(!source, "failed to remove the directory"))?;
				} else {
					std::fs::remove_file(&path)
						.map_err(|source| tg::error!(!source, "failed to remove the file"))?;
				}
				progress_.increment("cleaning", 1);
			}
			progress_.finish("cleaning");

			progress_.output(());

			Ok::<_, tg::Error>(())
		});

		// Render the progress stream.
		self.render_progress_stream(progress.stream()).await?;

		Ok(())
	}
}
