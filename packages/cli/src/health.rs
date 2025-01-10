use crate::Cli;
use crossterm::style::Stylize as _;
use tangram_client::{self as tg, Handle as _};

/// Get the server's health.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	// Format as JSON.
	#[arg(long)]
	pub json: bool,
}

impl Cli {
	pub async fn command_health(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let health = handle.health().await?;
		if args.json {
			let health = serde_json::to_string_pretty(&health)
				.map_err(|source| tg::error!(!source, "failed to serialize"))?;
			println!("{health}");
			return Ok(());
		}

		if let Some(version) = &health.version {
			println!("{}: {version}", "version".blue());
		}
		if let Some(builds) = &health.builds {
			println!(
				"{}: created:{} dequeued:{} started:{}",
				"builds".blue(),
				builds.created,
				builds.dequeued,
				builds.started
			);
		}
		if let Some(database) = &health.database {
			println!(
				"{}: available_connections:{}",
				"database".blue(),
				database.available_connections
			);
		}
		if let Some(file_descriptor_semaphore) = &health.file_descriptor_semaphore {
			println!(
				"{}: available_permits:{}",
				"files".blue(),
				file_descriptor_semaphore.available_permits
			);
		}
		for diagnostic in &health.diagnostics {
			Self::print_diagnostic(diagnostic);
		}
		Ok(())
	}
}
