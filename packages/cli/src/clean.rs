use crate::Cli;
use crossterm::style::Stylize as _;
use std::os::unix::fs::PermissionsExt as _;
use tangram_client::{self as tg, prelude::*};

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
		eprintln!(
			"{} cleaned {} processes, {} objects",
			"info".blue().bold(),
			output.processes,
			output.objects,
		);

		Ok(())
	}

	pub async fn command_clean_force(&mut self) -> tg::Result<()> {
		if !self.directory_path().exists() {
			return Ok(());
		}
		let mut stack = vec![self.directory_path()];
		while let Some(path) = stack.pop() {
			let metadata = tokio::fs::metadata(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
			let mut perms = metadata.permissions();
			#[cfg(unix)]
			{
				let mode = perms.mode();
				if metadata.is_dir() {
					perms.set_mode(mode | 0o300);
				} else {
					perms.set_mode(mode | 0o200);
				}
			}
			#[cfg(not(unix))]
			{
				perms.set_readonly(false);
			}
			tokio::fs::set_permissions(&path, perms)
				.await
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
			if metadata.is_dir() {
				let mut entries = tokio::fs::read_dir(&path)
					.await
					.map_err(|source| tg::error!(!source, "failed to read the directory"))?;
				while let Some(entry) = entries
					.next_entry()
					.await
					.map_err(|source| tg::error!(!source, "failed to read the directory"))?
				{
					stack.push(entry.path());
				}
			}
		}
		tokio::fs::remove_dir_all(self.directory_path())
			.await
			.map_err(|source| tg::error!(!source, "failed to remove the directory"))?;
		Ok(())
	}
}
