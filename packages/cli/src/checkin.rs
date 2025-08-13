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

	#[command(flatten)]
	pub ignore: Ignore,

	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[command(flatten)]
	pub lock: Lock,

	/// The path to check in.
	#[arg(default_value = ".", index = 1)]
	pub path: Option<PathBuf>,

	#[arg(
		action = clap::ArgAction::Append,
		long,
		num_args = 1..,
		short,
	)]
	pub patterns: Option<Vec<tg::tag::Pattern>>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Ignore {
	/// Whether to use ignore files.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_ignore",
		require_equals = true,
	)]
	ignore: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "ignore",
		require_equals = true,
	)]
	no_ignore: Option<bool>,
}

impl Ignore {
	pub fn get(&self) -> bool {
		self.ignore.or(self.no_ignore.map(|v| !v)).unwrap_or(true)
	}
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Lock {
	/// Whether to write the lock.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_lock",
		require_equals = true,
	)]
	lock: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "lock",
		require_equals = true,
	)]
	no_lock: Option<bool>,
}

impl Lock {
	pub fn get(&self) -> bool {
		self.lock.or(self.no_lock.map(|v| !v)).unwrap_or(true)
	}
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
			ignore: args.ignore.get(),
			lock: args.lock.get(),
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
