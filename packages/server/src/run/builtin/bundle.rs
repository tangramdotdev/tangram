use {
	crate::{Server, database::Transaction},
	futures::{TryStreamExt as _, stream::FuturesOrdered},
	std::path::{Path, PathBuf},
	tangram_client as tg,
};

static TANGRAM_ARTIFACTS_PATH: &str = ".tangram/artifacts";

impl Server {
	pub async fn run_builtin_bundle(
		&self,
		process: &tg::Process,
	) -> tg::Result<crate::run::Output> {
		let command = process.command(self).await?;

		// Get the args.
		let args = command.args(self).await?;

		// Get the artifact.
		let artifact: tg::Artifact = args
			.first()
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.clone()
			.try_into()
			.ok()
			.ok_or_else(|| tg::error!("expected an artifact"))?;

		// Collect the artifact's recursive artifact dependencies.
		let dependencies = Box::pin(artifact.recursive_dependencies(self)).await?;

		// If there are no dependencies, then return the artifact.
		if dependencies.is_empty() {
			let output = artifact.into();
			let output = crate::run::Output {
				checksum: None,
				error: None,
				exit: 0,
				output: Some(output),
			};
			return Ok(output);
		}

		// Create the artifacts directory by removing all dependencies.
		let entries = dependencies
			.into_iter()
			.map(|id| async move {
				let artifact = tg::Artifact::with_id(id.clone());
				let artifact = self.remove_dependencies(&artifact, 3, None).await?;
				Ok::<_, tg::Error>((id.to_string(), artifact))
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		let artifacts_directory = tg::Directory::with_entries(entries);

		// Create the bundle directory.
		let output: tg::Artifact = match artifact {
			// If the artifact is a directory, use it as is.
			tg::Artifact::Directory(directory) => directory.clone().into(),

			// Otherwise, return an error.
			artifact => {
				return Err(tg::error!(?artifact, "the artifact must be a directory"));
			},
		};

		// Remove dependencies from the bundle directory.
		let output = self
			.remove_dependencies(&output, 0, None)
			.await?
			.try_unwrap_directory()
			.ok()
			.ok_or_else(|| tg::error!("the artifact must be a directory"))?;

		// Add the artifacts directory to the bundled artifact at `.tangram/artifacts`.
		let output = output
			.builder(self)
			.await?
			.add(
				self,
				TANGRAM_ARTIFACTS_PATH.as_ref(),
				artifacts_directory.into(),
			)
			.await?
			.build();
		let output = output.into();

		let output = crate::run::Output {
			checksum: None,
			error: None,
			exit: 0,
			output: Some(output),
		};

		Ok(output)
	}

	/// Remove all dependencies from an artifact and its children recursively.
	async fn remove_dependencies(
		&self,
		artifact: &tg::Artifact,
		depth: usize,
		transaction: Option<&Transaction<'_>>,
	) -> tg::Result<tg::Artifact> {
		match artifact {
			// If the artifact is a directory, then recurse to remove dependencies from its entries.
			tg::Artifact::Directory(directory) => {
				let entries = Box::pin(async move {
					directory
						.entries(self)
						.await?
						.iter()
						.map(|(name, artifact)| async move {
							let artifact = self
								.remove_dependencies(artifact, depth + 1, transaction)
								.await?;
							Ok::<_, tg::Error>((name.clone(), artifact))
						})
						.collect::<FuturesOrdered<_>>()
						.try_collect()
						.await
				})
				.await?;

				let directory = tg::Directory::with_entries(entries);
				Ok(directory.into())
			},

			// If the artifact is a file, then return the file without any dependencies.
			tg::Artifact::File(file) => {
				let contents = file.contents(self).await?.clone();
				let executable = file.executable(self).await?;
				let file = tg::File::builder(contents).executable(executable).build();
				Ok(file.into())
			},

			// If the artifact is a symlink with an artifact, then replace it with a symlink pointing to `.tangram/artifacts/<id>`.
			tg::Artifact::Symlink(symlink) => {
				let artifact = symlink.artifact(self).await?;
				let path = symlink.path(self).await?;
				let mut target = PathBuf::new();
				if let Some(artifact) = artifact {
					for _ in 0..depth - 1 {
						target.push("..");
					}
					target.push(TANGRAM_ARTIFACTS_PATH);
					target.push(artifact.id().to_string());
				}
				if let Some(path) = path.as_ref() {
					target.push(path);
				}
				if target == Path::new("") {
					return Err(tg::error!("invalid symlink"));
				}
				let symlink = tg::Symlink::with_path(target);
				Ok(symlink.into())
			},
		}
	}
}
