use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

/// Update a lock.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub checkin: crate::checkin::Options,

	#[arg(default_value = ".", index = 1)]
	pub path: PathBuf,

	#[arg(
		action = clap::ArgAction::Append,
		index = 2,
		num_args = 1..,
	)]
	pub updates: Option<Vec<tg::tag::Pattern>>,
}

impl Cli {
	pub async fn command_update(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Canonicalize the path's parent.
		let path = tangram_util::fs::canonicalize_parent(&args.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;

		// Get the updates.
		let updates = args.updates.unwrap_or_else(|| vec!["*".parse().unwrap()]);

		// Check in.
		let arg = tg::checkin::Arg {
			options: args.checkin.to_options(),
			path: path.clone(),
			updates,
		};
		let stream = handle
			.checkin(arg)
			.await
			.map_err(|source| tg::error!(!source, path = %path.display(), "failed to check in"))?;
		self.render_progress_stream(stream).await?;

		Ok(())
	}
}
