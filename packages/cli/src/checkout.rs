use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

/// Check out nodes.
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
	#[arg(long)]
	pub path: Option<PathBuf>,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// The nodes to check out.
	#[arg(required = true)]
	pub references: Vec<tg::Reference>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Dependencies {
	/// Whether to check out the artifact's dependencies.
	#[arg(
		default_missing_value = "true",
		id = "checkout.dependencies.dependencies",
		long = "dependencies",
		num_args = 0..=1,
		overrides_with = "checkout.dependencies.no_dependencies",
		require_equals = true,
	)]
	dependencies: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "checkout.dependencies.no_dependencies",
		long = "no-dependencies",
		num_args = 0..=1,
		overrides_with = "checkout.dependencies.dependencies",
		require_equals = true,
	)]
	no_dependencies: Option<bool>,
}

impl Dependencies {
	#[must_use]
	pub fn get(&self) -> bool {
		self.dependencies
			.or(self.no_dependencies.map(|v| !v))
			.unwrap_or(true)
	}

	#[must_use]
	pub fn is_set(&self) -> bool {
		self.dependencies.is_some() || self.no_dependencies.is_some()
	}
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Lock {
	/// Whether to write the lock. Use `--lock=auto` to reuse an existing lock kind or prefer a lockattr for files. Use `--lock=file` to write a lockfile, and `--lock=attr` to write a lockattr. `auto` is the default if not specified.
	#[arg(
		default_missing_value = "auto",
		id = "checkout.lock.lock",
		long = "lock",
		num_args = 0..=1,
		overrides_with = "checkout.lock.no_lock",
		require_equals = true,
	)]
	lock: Option<tg::checkout::Lock>,

	/// Disable writing the lock.
	#[arg(
		id = "checkout.lock.no_lock",
		long = "no-lock",
		overrides_with = "checkout.lock.lock"
	)]
	no_lock: bool,
}

impl Lock {
	#[must_use]
	pub fn get(&self) -> Option<tg::checkout::Lock> {
		if self.no_lock {
			None
		} else {
			self.lock.or(Some(tg::checkout::Lock::default()))
		}
	}

	#[must_use]
	pub fn is_set(&self) -> bool {
		self.lock.is_some() || self.no_lock
	}
}

impl Cli {
	pub async fn command_checkout(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		// Get the absolute path.
		let path = if let Some(path) = args.path {
			let path = tangram_util::fs::canonicalize_parent(path)
				.await
				.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;
			Some(path)
		} else {
			None
		};
		if path.is_none() && args.dependencies.is_set() {
			return Err(tg::error!(
				"the dependencies option cannot be set for an internal checkout"
			));
		}
		if path.is_none() && args.lock.is_set() {
			return Err(tg::error!(
				"the lock option cannot be set for an internal checkout"
			));
		}

		// Get the nodes.
		let referents = self.get_references(&args.references).await?;
		let mut nodes = Vec::with_capacity(referents.len());
		for referent in referents {
			let node = referent.try_map(|node| match node {
				tg::get::Node::Id(id)
					if tg::artifact::Id::try_from(id.clone()).is_ok()
						|| matches!(
							id.kind(),
							tg::id::Kind::Group
								| tg::id::Kind::Organization
								| tg::id::Kind::Tag | tg::id::Kind::User
						) =>
				{
					Ok(id)
				},
				tg::get::Node::Id(id) if id.kind().is_object() => {
					Err(tg::error!("expected an artifact"))
				},
				tg::get::Node::Id(id) => {
					Err(tg::error!(kind = %id.kind(), "expected an object ID"))
				},
				tg::get::Node::Pointer(_) => Err(tg::error!("cannot check out a graph pointer")),
			})?;
			nodes.push(node);
		}
		let external_artifact = path.as_ref().and_then(|_| {
			nodes
				.first()
				.filter(|_| nodes.len() == 1)
				.and_then(|node| tg::artifact::Id::try_from(node.node.clone()).ok())
		});

		// Check out the artifact.
		let dependencies = args.dependencies.get();
		let force = args.force;
		let lock = path.as_ref().and_then(|_| args.lock.get());
		let arg = tg::checkout::Arg {
			dependencies,
			extension: None,
			force,
			lock,
			nodes,
			path,
		};
		let stream = client.checkout(arg).await.map_err(|error| {
			if let Some(artifact) = &external_artifact {
				tg::error!(!error, %artifact, "failed to create the checkout stream")
			} else {
				tg::error!(!error, "failed to create the checkout stream")
			}
		})?;
		let output = self.render_progress_stream(stream).await.map_err(|error| {
			if let Some(artifact) = &external_artifact {
				tg::error!(!error, %artifact, "failed to check out the artifact")
			} else {
				error
			}
		})?;

		// Print the outputs.
		for path in output.paths {
			Self::print_display(path.display());
		}

		Ok(())
	}
}
