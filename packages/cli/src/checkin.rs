use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

/// Check in an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	/// The path to check in.
	#[arg(default_value = ".", index = 1)]
	pub path: PathBuf,

	#[command(flatten)]
	pub print: crate::print::Options,

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
	#[command(flatten)]
	pub cache_references: CacheReferences,

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

	#[command(flatten)]
	pub solve: Solve,

	#[command(flatten)]
	pub unsolved_dependencies: UnsolvedDependencies,

	#[arg(long)]
	pub watch: bool,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Solve {
	/// Whether to solve dependencies
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_solve",
		require_equals = true,
	)]
	solve: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "solve",
		require_equals = true,
	)]
	no_solve: Option<bool>,
}

impl Solve {
	pub fn get(&self) -> bool {
		self.solve.or(self.no_solve.map(|v| !v)).unwrap_or(true)
	}
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
	/// Whether to write the lock. Use `--lock=file` to write a lockfile, "attr" to write a lockattr. "file" is the default if not specified.
	#[arg(
		default_missing_value = "file",
		long,
		num_args = 0..=1,
		overrides_with = "no_lock",
		require_equals = true,
	)]
	lock: Option<tg::checkin::Lock>,

	/// Disable writing the lock.
	#[arg(long, overrides_with = "lock")]
	no_lock: bool,
}

impl Lock {
	pub fn get(&self) -> Option<tg::checkin::Lock> {
		if self.no_lock {
			None
		} else {
			self.lock.or(Some(tg::checkin::Lock::default()))
		}
	}
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct UnsolvedDependencies {
	/// Whether to allow unsolved dependencies.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_unsolved_dependencies",
		require_equals = true,
	)]
	unsolved_dependencies: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "unsolved_dependencies",
		require_equals = true,
	)]
	no_unsolved_dependencies: Option<bool>,
}

impl UnsolvedDependencies {
	pub fn get(&self) -> bool {
		self.unsolved_dependencies
			.or(self.no_unsolved_dependencies.map(|v| !v))
			.unwrap_or(false)
	}
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct CacheReferences {
	/// Whether to create cache references.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_cache_references",
		require_equals = true,
	)]
	cache_references: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "cache_references",
		require_equals = true,
	)]
	no_cache_references: Option<bool>,
}

impl CacheReferences {
	pub fn get(&self) -> bool {
		self.cache_references
			.or(self.no_cache_references.map(|v| !v))
			.unwrap_or(true)
	}
}

impl Cli {
	pub async fn command_checkin(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the updates.
		let updates = args.updates.unwrap_or_default();

		// Canonicalize the path's parent.
		let path = tangram_util::fs::canonicalize_parent(&args.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;

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

		// Print.
		Self::print_id(&output.artifact.item);

		Ok(())
	}
}

impl Options {
	pub fn to_options(&self) -> tg::checkin::Options {
		tg::checkin::Options {
			cache_references: self.cache_references.get(),
			destructive: self.destructive,
			deterministic: self.deterministic,
			ignore: self.ignore.get(),
			local_dependencies: self.local_dependencies.get(),
			lock: self.lock.get(),
			locked: self.locked,
			solve: self.solve.get(),
			unsolved_dependencies: self.unsolved_dependencies.get(),
			watch: self.watch,
		}
	}
}
