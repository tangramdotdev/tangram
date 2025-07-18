use crate::Cli;
use std::path::PathBuf;
use tangram_client::{self as tg, prelude::*};

/// Check in an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Check in the artifact more quickly by allowing it to be destroyed.
	#[arg(long)]
	pub destructive: bool,

	/// Check in the artifact determnistically.
	#[arg(long)]
	pub deterministic: bool,

	/// If false, don't parse ignore files.
	#[arg(default_value = "true", long, action = clap::ArgAction::Set)]
	pub ignore: bool,

	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// Whether to write the lock.
	#[arg(default_value = "true", long, action = clap::ArgAction::Set)]
	pub lock: bool,

	/// The path to check in.
	#[arg(default_value = ".", index = 1)]
	pub path: Option<PathBuf>,

	#[arg(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub patterns: Option<Vec<tg::tag::Pattern>>,
}

impl Cli {
	pub async fn command_checkin(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Update nothing by default.
		let updates = args.patterns.unwrap_or_default();

		// Get the path.
		let path = std::path::absolute(args.path.unwrap_or_default())
			.map_err(|source| tg::error!(!source, "failed to get the path"))?;

		// Check in the artifact.
		let arg = tg::checkin::Arg {
			deterministic: args.deterministic,
			destructive: args.destructive,
			ignore: args.ignore,
			lock: args.lock,
			locked: args.locked,
			path,
			updates,
		};
		let stream = handle
			.checkin(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the artifact"))?;
		let output = self.render_progress_stream(stream).await?;

		// Print the artifact.
		println!("{}", output.referent.item);

		Ok(())
	}
}
