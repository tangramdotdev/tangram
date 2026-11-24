use {std::path::PathBuf, tangram_cli_core::Cli, tangram_client::prelude::*};

/// Check in an artifact.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub options: Options,

	/// The path to check in.
	#[usage(default = ".")]
	pub path: PathBuf,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	/// Grant public read access.
	#[usage(long)]
	pub public: bool,

	#[usage(long = "update", num_args = 1.., short)]
	pub updates: Option<Vec<tg::specifier::Pattern>>,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Options {
	#[usage(flatten)]
	pub checkout_pointers: CheckoutPointers,

	/// Check in the artifact more quickly by allowing it to be destroyed.
	#[usage(long = "destructive")]
	pub destructive: bool,

	/// Check in the artifact determnistically.
	#[usage(long = "deterministic")]
	pub deterministic: bool,

	#[usage(flatten)]
	pub ignore: Ignore,

	#[usage(flatten)]
	pub lock: Lock,

	/// If this flag is set, the lock will not be updated.
	#[usage(long = "locked")]
	pub locked: bool,

	/// Treat the provided path as the root path.
	#[usage(long = "root")]
	pub root: bool,

	#[usage(flatten)]
	pub solve: Solve,

	#[usage(flatten)]
	pub source_dependencies: SourceDependencies,

	#[usage(flatten)]
	pub tag_time_to_live: TagTimeToLive,

	#[usage(flatten)]
	pub unsolved_dependencies: UnsolvedDependencies,

	#[usage(flatten)]
	pub watch: Watch,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct TagTimeToLive {
	#[usage(
		alias = "no-tag-ttl",
		long = "no-tag-time-to-live",
		overrides("--tag-time-to-live")
	)]
	pub no_tag_time_to_live: bool,
	#[usage(
		alias = "tag-ttl",
		long = "tag-time-to-live",
		overrides("--no-tag-time-to-live")
	)]
	pub tag_time_to_live: Option<humantime::Duration>,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Solve {
	/// Whether to solve dependencies
	#[usage(
		default_missing = "true",
		long = "solve",
		overrides("--no-solve"),
		require_equals = true
	)]
	solve: Option<bool>,

	#[usage(
		default_missing = "true",
		long = "no-solve",
		overrides("--solve"),
		require_equals = true
	)]
	no_solve: Option<bool>,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Ignore {
	/// Whether to use ignore files.
	#[usage(
		default_missing = "true",
		long = "ignore",
		overrides("--no-ignore"),
		require_equals = true
	)]
	ignore: Option<bool>,

	#[usage(
		default_missing = "true",
		long = "no-ignore",
		overrides("--ignore"),
		require_equals = true
	)]
	no_ignore: Option<bool>,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct SourceDependencies {
	/// Whether to use source dependencies.
	#[usage(
		default_missing = "true",
		long = "source-dependencies",
		overrides("--no-source-dependencies"),
		require_equals = true
	)]
	source_dependencies: Option<bool>,

	#[usage(
		default_missing = "true",
		long = "no-source-dependencies",
		overrides("--source-dependencies"),
		require_equals = true
	)]
	no_source_dependencies: Option<bool>,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Lock {
	/// Whether to write the lock. Use `--lock=auto` to reuse an existing lock kind or prefer a lockattr for files. Use `--lock=file` to write a lockfile, and `--lock=attr` to write a lockattr. `auto` is the default if not specified.
	#[usage(
		default_missing = "auto",
		long = "lock",
		overrides("--no-lock"),
		require_equals = true
	)]
	lock: Option<tg::checkin::Lock>,

	/// Disable writing the lock.
	#[usage(long = "no-lock", overrides("--lock"))]
	no_lock: bool,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct UnsolvedDependencies {
	/// Whether to allow unsolved dependencies.
	#[usage(
		default_missing = "true",
		long = "unsolved-dependencies",
		overrides("--no-unsolved-dependencies"),
		require_equals = true
	)]
	unsolved_dependencies: Option<bool>,

	#[usage(
		default_missing = "true",
		long = "no-unsolved-dependencies",
		overrides("--unsolved-dependencies"),
		require_equals = true
	)]
	no_unsolved_dependencies: Option<bool>,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct CheckoutPointers {
	/// Whether to create checkout pointers.
	#[usage(
		default_missing = "true",
		long = "checkout-pointers",
		overrides("--no-checkout-pointers"),
		require_equals = true
	)]
	checkout_pointers: Option<bool>,

	#[usage(
		default_missing = "true",
		long = "no-checkout-pointers",
		overrides("--checkout-pointers"),
		require_equals = true
	)]
	no_checkout_pointers: Option<bool>,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Watch {
	#[usage(
		default_missing = "true",
		long = "no-watch",
		overrides("--watch"),
		require_equals = true
	)]
	no_watch: Option<bool>,

	/// Whether to watch the source. Defaults to true for non-destructive checkins.
	#[usage(
		default_missing = "true",
		long = "watch",
		overrides("--no-watch"),
		require_equals = true
	)]
	watch: Option<bool>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;

	// Get the updates.
	let updates = args.updates.unwrap_or_default();

	// Canonicalize the path's parent in the caller's filesystem.
	let path = tangram_util::fs::canonicalize_parent(&args.path)
		.await
		.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;

	// Check in the artifact.
	let arg = tg::checkin::Arg {
		options: tg::checkin::Options {
			public: args.public,
			..args.options.to_options()
		},
		path,
		updates,
	};
	let stream = client
		.checkin(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to check in the artifact"))?;
	let output = cli.render_progress_stream(stream).await?;

	// Print.
	Cli::print_referent(&output.artifact, &args.print);

	Ok(())
}

impl TagTimeToLive {
	fn get(&self) -> tg::remote::cache::TimeToLive {
		if self.no_tag_time_to_live {
			tg::remote::cache::TimeToLive::Infinite
		} else {
			self.tag_time_to_live
				.map_or_default(|duration| tg::remote::cache::TimeToLive::Duration(duration.into()))
		}
	}
}

impl Solve {
	#[must_use]
	pub fn get(&self) -> bool {
		self.solve.or(self.no_solve.map(|v| !v)).unwrap_or(true)
	}
}

impl Ignore {
	#[must_use]
	pub fn get(&self) -> bool {
		self.ignore.or(self.no_ignore.map(|v| !v)).unwrap_or(true)
	}
}

impl SourceDependencies {
	#[must_use]
	pub fn get(&self) -> bool {
		self.source_dependencies
			.or(self.no_source_dependencies.map(|v| !v))
			.unwrap_or(true)
	}
}

impl Lock {
	#[must_use]
	pub fn get(&self) -> Option<tg::checkin::Lock> {
		if self.no_lock {
			None
		} else {
			self.lock.or(Some(tg::checkin::Lock::default()))
		}
	}
}

impl UnsolvedDependencies {
	#[must_use]
	pub fn get(&self) -> bool {
		self.unsolved_dependencies
			.or(self.no_unsolved_dependencies.map(|v| !v))
			.unwrap_or(false)
	}
}

impl CheckoutPointers {
	#[must_use]
	pub fn get(&self) -> bool {
		self.checkout_pointers
			.or(self.no_checkout_pointers.map(|v| !v))
			.unwrap_or(true)
	}

	#[must_use]
	pub fn get_option(&self) -> Option<bool> {
		self.checkout_pointers
			.or(self.no_checkout_pointers.map(|v| !v))
	}
}

impl Watch {
	#[must_use]
	pub fn get_option(&self) -> Option<bool> {
		self.watch.or(self.no_watch.map(|v| !v))
	}
}

impl Options {
	#[must_use]
	pub fn to_options(&self) -> tg::checkin::Options {
		tg::checkin::Options {
			checkout_pointers: self.checkout_pointers.get(),
			destructive: self.destructive,
			deterministic: self.deterministic,
			ignore: self.ignore.get(),
			lock: self.lock.get(),
			locked: self.locked,
			public: false,
			root: self.root,
			solve: self.solve.get(),
			source_dependencies: self.source_dependencies.get(),
			tag_time_to_live: self.tag_time_to_live.get(),
			unsolved_dependencies: self.unsolved_dependencies.get(),
			watch: self.watch.get_option(),
		}
	}
}
