use crate::Cli;
use std::path::PathBuf;
use tangram_client::{self as tg, prelude::*};
use tangram_either::Either;

/// Check out an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub dependencies: Dependencies,

	/// Whether to overwrite an existing file system object at the path.
	#[arg(long, requires = "path", short)]
	pub force: bool,

	#[command(flatten)]
	pub lock: Lock,

	/// The path to check out the artifact to.
	#[arg(index = 2)]
	pub path: Option<PathBuf>,

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
	pub async fn command_checkout(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the absolute path.
		let path = if let Some(path) = args.path {
			let path = std::path::absolute(path)
				.map_err(|source| tg::error!(!source, "failed to get the absolute path"))?;
			Some(path)
		} else {
			None
		};

		// Get the artifact.
		let referent = self.get_reference(&args.reference).await?;
		let Either::Right(object) = referent.item else {
			return Err(tg::error!("expected an object"));
		};
		let artifact = tg::Artifact::try_from(object)?;
		let artifact = artifact.id();

		// Check out the artifact.
		let dependencies = args.dependencies.get();
		let force = args.force;
		let lock = args.lock.get();
		let arg = tg::checkout::Arg {
			artifact,
			dependencies,
			force,
			lock,
			path,
		};
		let stream = handle
			.checkout(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the checkout stream"))?;
		let output = self.render_progress_stream(stream).await?;

		// Print the path.
		println!("{}", output.path.display());

		Ok(())
	}
}
