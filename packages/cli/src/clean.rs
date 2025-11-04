use {
	crate::Cli,
	std::os::unix::fs::PermissionsExt as _,
	tangram_client::{self as tg, prelude::*},
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
		let message = format!(
			"cleaned {} processes, {} objects",
			output.processes, output.objects,
		);
		Self::print_info_message(&message);

		Ok(())
	}

	pub async fn command_clean_force(&mut self) -> tg::Result<()> {
		let path = self.directory_path();
		tokio::task::spawn_blocking(move || {
			if !path.exists() {
				return Ok(());
			}
			let mut stack = vec![path.clone()];
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
			}
			std::fs::remove_dir_all(&path)
				.map_err(|source| tg::error!(!source, "failed to remove the directory"))?;
			Ok::<_, tg::Error>(())
		})
		.await
		.unwrap()?;
		Ok(())
	}
}
