use {
	crate::Cli,
	std::path::PathBuf,
	tangram_client::{self as tg, prelude::*},
};

/// Check in an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	/// The path to check in.
	#[arg(default_value = ".", index = 1)]
	pub path: Option<PathBuf>,

	#[arg(
		action = clap::ArgAction::Append,
		long = "update",
		num_args = 1..,
		short,
	)]
	pub updates: Option<Vec<tg::tag::Pattern>>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Options {
	/// Check in the artifact more quickly by allowing it to be destroyed.
	#[arg(long)]
	pub destructive: bool,

	/// Check in the artifact determnistically.
	#[arg(long)]
	pub deterministic: bool,

	#[command(flatten)]
	pub ignore: Ignore,

	#[command(flatten)]
	pub local_dependencies: LocalDependencies,

	#[command(flatten)]
	pub lock: Lock,

	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,
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
pub struct LocalDependencies {
	/// Whether to use local dependencies.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_local_dependencies",
		require_equals = true,
	)]
	local_dependencies: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "local_dependencies",
		require_equals = true,
	)]
	no_local_dependencies: Option<bool>,
}

impl LocalDependencies {
	pub fn get(&self) -> bool {
		self.local_dependencies
			.or(self.no_local_dependencies.map(|v| !v))
			.unwrap_or(true)
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

		// Get the updates.
		let updates = args.updates.unwrap_or_default();

		// Get the path.
		let path = std::path::absolute(args.path.unwrap_or_default())
			.map_err(|source| tg::error!(!source, "failed to get the path"))?;

		// Check in the artifact.
		let arg = tg::checkin::Arg {
			options: args.options.to_options(),
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

impl Options {
	pub fn to_options(&self) -> tg::checkin::Options {
		tg::checkin::Options {
			destructive: self.destructive,
			deterministic: self.deterministic,
			ignore: self.ignore.get(),
			local_dependencies: self.local_dependencies.get(),
			lock: self.lock.get(),
			locked: self.locked,
		}
	}
}
