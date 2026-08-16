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
	pub fn get(&self) -> bool {
		self.dependencies
			.or(self.no_dependencies.map(|v| !v))
			.unwrap_or(true)
	}

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
	pub fn get(&self) -> Option<tg::checkout::Lock> {
		if self.no_lock {
			None
		} else {
			self.lock.or(Some(tg::checkout::Lock::default()))
		}
	}

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
		let referents = self.resolve_references(&args.references).await?;
		let mut artifacts = Vec::with_capacity(referents.len());
		let mut nodes = Vec::with_capacity(referents.len());
		for referent in referents {
			if let tg::get::Node::Id(id) = &referent.node
				&& matches!(
					id.kind(),
					tg::id::Kind::Group
						| tg::id::Kind::Organization
						| tg::id::Kind::Tag
						| tg::id::Kind::User
				) && referent.options.tag.is_none()
			{
				artifacts.push(None);
				nodes.push(tg::Referent::new(id.clone(), referent.options));
				continue;
			}
			let artifact = referent.into_graph_edge()?.try_map(|edge| {
				let object = edge
					.try_unwrap_object()
					.map_err(|_| tg::error!("expected an object"))?;
				let artifact = tg::Artifact::try_from(object)?;
				Ok::<_, tg::Error>(artifact.id())
			})?;
			artifacts.push(Some(artifact.node.clone()));
			let Some(tag) = artifact.options.tag.clone() else {
				nodes.push(artifact.map(Into::into));
				continue;
			};
			let output = client
				.try_get_tag(
					&tg::tag::Selector::Specifier(tag.clone()),
					tg::tag::get::Arg {
						location: artifact.options.location.clone().map(Into::into),
						..Default::default()
					},
				)
				.await?
				.ok_or_else(|| tg::error!(%tag, "the tag was not found"))?;
			let options = tg::referent::Options {
				artifact: Some(artifact.node),
				id: artifact.options.id,
				location: output.location,
				name: artifact.options.name,
				path: artifact.options.path,
				tokens: output.tokens,
				..Default::default()
			};
			nodes.push(tg::Referent::new(output.data.id.into(), options));
		}
		let external_artifact = path.as_ref().and_then(|_| {
			(nodes.len() == 1)
				.then(|| artifacts.pop().unwrap())
				.flatten()
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
