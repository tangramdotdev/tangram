use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

/// Check out an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Whether to check out the artifact's dependencies.
	#[command(flatten)]
	pub dependencies: Dependencies,

	/// Whether to overwrite an existing filesystem object at the path.
	#[arg(long, requires = "path", short)]
	pub force: bool,

	/// Whether to write a lock.
	#[command(flatten)]
	pub lock: Lock,

	/// The path to check out the artifact to.
	#[arg(index = 2)]
	pub path: Option<PathBuf>,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// The artifact to check out.
	#[arg(index = 1)]
	pub reference: tg::Reference,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Dependencies {
	/// Whether to check out the artifact's dependencies.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_dependencies",
		require_equals = true,
	)]
	dependencies: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "dependencies",
		require_equals = true,
	)]
	no_dependencies: Option<bool>,
}

impl Dependencies {
	pub fn get(&self) -> bool {
		self.dependencies
			.or(self.no_dependencies.map(|v| !v))
			.unwrap_or(true)
	}
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Lock {
	/// Whether to write the lock. Use `--lock=auto` to reuse an existing lock kind or prefer a lockattr for files. Use `--lock=file` to write a lockfile, and `--lock=attr` to write a lockattr. `auto` is the default if not specified.
	#[arg(
		default_missing_value = "auto",
		long,
		num_args = 0..=1,
		overrides_with = "no_lock",
		require_equals = true,
	)]
	lock: Option<tg::checkout::Lock>,

	/// Disable writing the lock.
	#[arg(long, overrides_with = "lock")]
	no_lock: bool,
}

impl Lock {
	pub fn get(&self) -> Option<tg::checkout::Lock> {
		if self.no_lock {
			None
		} else {
			self.lock.or(Some(tg::checkout::Lock::default()))
		}
	}
}

impl Cli {
	pub async fn command_checkout(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the absolute path.
		let path = if let Some(path) = args.path {
			let path = tangram_util::fs::canonicalize_parent(path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
			Some(path)
		} else {
			None
		};

		// Get the artifact.
		let referent = self.get_reference(&args.reference).await?;
		let tg::Either::Left(object) = referent.item else {
			return Err(tg::error!("expected an object"));
		};
		let artifact = tg::Artifact::try_from(object)?;
		let artifact = artifact.id();

		// Check out the artifact.
		let dependencies = args.dependencies.get();
		let force = args.force;
		let lock = args.lock.get();
		let arg = tg::checkout::Arg {
			artifact: artifact.clone(),
			dependencies,
			extension: None,
			force,
			lock,
			path,
		};
		let stream = handle.checkout(arg).await.map_err(
			|source| tg::error!(!source, %artifact, "failed to create the checkout stream"),
		)?;
		let output = self
			.render_progress_stream(stream)
			.await
			.map_err(|source| tg::error!(!source, %artifact, "failed to check out the artifact"))?;

		// Print the output.
		Self::print_display(output.path.display());

		Ok(())
	}
}
