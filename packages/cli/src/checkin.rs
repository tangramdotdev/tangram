use {
	crate::Cli,
	std::{path::PathBuf, time::Duration},
	tangram_client::prelude::*,
};

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
	pub updates: Option<Vec<tg::specifier::Pattern>>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Options {
	#[command(flatten)]
	pub cache_pointers: CachePointers,

	/// Check in the artifact more quickly by allowing it to be destroyed.
	#[arg(id = "checkin.destructive", long = "destructive")]
	pub destructive: bool,

	/// Check in the artifact determnistically.
	#[arg(id = "checkin.deterministic", long = "deterministic")]
	pub deterministic: bool,

	#[command(flatten)]
	pub ignore: Ignore,

	#[command(flatten)]
	pub lock: Lock,

	/// If this flag is set, the lock will not be updated.
	#[arg(id = "checkin.locked", long = "locked")]
	pub locked: bool,

	/// Treat the provided path as the root path.
	#[arg(id = "checkin.root", long = "root")]
	pub root: bool,

	#[command(flatten)]
	pub solve: Solve,

	#[command(flatten)]
	pub source_dependencies: SourceDependencies,

	#[command(flatten)]
	pub tag_ttl: TagTtl,

	#[command(flatten)]
	pub unsolved_dependencies: UnsolvedDependencies,

	#[arg(id = "checkin.watch", long = "watch")]
	pub watch: bool,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct TagTtl {
	#[arg(id = "checkin.tag_ttl.tag_ttl", long = "tag-ttl", overrides_with = "checkin.tag_ttl.no_tag_ttl", value_parser = humantime::parse_duration)]
	pub tag_ttl: Option<Duration>,

	#[arg(
		id = "checkin.tag_ttl.no_tag_ttl",
		long = "no-tag-ttl",
		overrides_with = "checkin.tag_ttl.tag_ttl"
	)]
	pub no_tag_ttl: bool,
}

impl TagTtl {
	fn get(&self) -> Option<Duration> {
		if self.no_tag_ttl { None } else { self.tag_ttl }
	}
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Solve {
	/// Whether to solve dependencies
	#[arg(
		default_missing_value = "true",
		id = "checkin.solve.solve",
		long = "solve",
		num_args = 0..=1,
		overrides_with = "checkin.solve.no_solve",
		require_equals = true,
	)]
	solve: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "checkin.solve.no_solve",
		long = "no-solve",
		num_args = 0..=1,
		overrides_with = "checkin.solve.solve",
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
		id = "checkin.ignore.ignore",
		long = "ignore",
		num_args = 0..=1,
		overrides_with = "checkin.ignore.no_ignore",
		require_equals = true,
	)]
	ignore: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "checkin.ignore.no_ignore",
		long = "no-ignore",
		num_args = 0..=1,
		overrides_with = "checkin.ignore.ignore",
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
pub struct SourceDependencies {
	/// Whether to use source dependencies.
	#[arg(
		default_missing_value = "true",
		id = "checkin.source_dependencies.source_dependencies",
		long = "source-dependencies",
		num_args = 0..=1,
		overrides_with = "checkin.source_dependencies.no_source_dependencies",
		require_equals = true,
	)]
	source_dependencies: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "checkin.source_dependencies.no_source_dependencies",
		long = "no-source-dependencies",
		num_args = 0..=1,
		overrides_with = "checkin.source_dependencies.source_dependencies",
		require_equals = true,
	)]
	no_source_dependencies: Option<bool>,
}

impl SourceDependencies {
	pub fn get(&self) -> bool {
		self.source_dependencies
			.or(self.no_source_dependencies.map(|v| !v))
			.unwrap_or(true)
	}
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Lock {
	/// Whether to write the lock. Use `--lock=auto` to reuse an existing lock kind or prefer a lockattr for files. Use `--lock=file` to write a lockfile, and `--lock=attr` to write a lockattr. `auto` is the default if not specified.
	#[arg(
		default_missing_value = "auto",
		id = "checkin.lock.lock",
		long = "lock",
		num_args = 0..=1,
		overrides_with = "checkin.lock.no_lock",
		require_equals = true,
	)]
	lock: Option<tg::checkin::Lock>,

	/// Disable writing the lock.
	#[arg(
		id = "checkin.lock.no_lock",
		long = "no-lock",
		overrides_with = "checkin.lock.lock"
	)]
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
		id = "checkin.unsolved_dependencies.unsolved_dependencies",
		long = "unsolved-dependencies",
		num_args = 0..=1,
		overrides_with = "checkin.unsolved_dependencies.no_unsolved_dependencies",
		require_equals = true,
	)]
	unsolved_dependencies: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "checkin.unsolved_dependencies.no_unsolved_dependencies",
		long = "no-unsolved-dependencies",
		num_args = 0..=1,
		overrides_with = "checkin.unsolved_dependencies.unsolved_dependencies",
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
pub struct CachePointers {
	/// Whether to create cache pointers.
	#[arg(
		default_missing_value = "true",
		id = "checkin.cache_pointers.cache_pointers",
		long = "cache-pointers",
		num_args = 0..=1,
		overrides_with = "checkin.cache_pointers.no_cache_pointers",
		require_equals = true,
	)]
	cache_pointers: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "checkin.cache_pointers.no_cache_pointers",
		long = "no-cache-pointers",
		num_args = 0..=1,
		overrides_with = "checkin.cache_pointers.cache_pointers",
		require_equals = true,
	)]
	no_cache_pointers: Option<bool>,
}

impl CachePointers {
	pub fn get(&self) -> bool {
		self.cache_pointers
			.or(self.no_cache_pointers.map(|v| !v))
			.unwrap_or(true)
	}

	pub fn get_option(&self) -> Option<bool> {
		self.cache_pointers.or(self.no_cache_pointers.map(|v| !v))
	}
}

impl Cli {
	pub async fn command_checkin(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		// Get the updates.
		let updates = args.updates.unwrap_or_default();

		// Canonicalize the path's parent.
		let path = tangram_util::fs::canonicalize_parent(&args.path)
			.await
			.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;

		// Check in the artifact.
		let arg = tg::checkin::Arg {
			options: args.options.to_options(),
			path,
			updates,
		};
		let stream = client
			.checkin(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to check in the artifact"))?;
		let output = self.render_progress_stream(stream).await?;

		// Print.
		Self::print_id(&output.artifact.item);

		Ok(())
	}
}

impl Options {
	pub fn to_options(&self) -> tg::checkin::Options {
		tg::checkin::Options {
			cache_pointers: self.cache_pointers.get(),
			destructive: self.destructive,
			deterministic: self.deterministic,
			ignore: self.ignore.get(),
			lock: self.lock.get(),
			locked: self.locked,
			root: self.root,
			solve: self.solve.get(),
			source_dependencies: self.source_dependencies.get(),
			tag_ttl: self.tag_ttl.get(),
			unsolved_dependencies: self.unsolved_dependencies.get(),
			watch: self.watch,
		}
	}
}
