use {
	crate::Cli,
	std::path::PathBuf,
	tangram_client::{self as tg, prelude::*},
};

/// Update a lock.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(default_value = ".", index = 1)]
	pub path: PathBuf,

	#[arg(
		action = clap::ArgAction::Append,
		num_args = 1..,
	)]
	pub updates: Option<Vec<tg::tag::Pattern>>,
}

impl Cli {
	pub async fn command_update(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the absolute path.
		let path = std::path::absolute(&args.path)
			.map_err(|source| tg::error!(!source, "failed to get the absolute path"))?;

		// Get the updates.
		let updates = args
			.updates
			.unwrap_or_else(|| vec![tg::tag::Pattern::wildcard()]);

		// Check in.
		let arg = tg::checkin::Arg {
			options: tg::checkin::Options::default(),
			path,
			updates,
		};
		let stream = handle.checkin(arg).await?;
		self.render_progress_stream(stream).await?;

		Ok(())
	}
}
