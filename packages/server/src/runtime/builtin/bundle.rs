use super::Runtime;
use crate::database::Transaction;
use futures::{TryStreamExt as _, stream::FuturesOrdered};
use std::path::PathBuf;
use tangram_client as tg;

#[cfg(test)]
mod tests;

static TANGRAM_ARTIFACTS_PATH: &str = ".tangram/artifacts";

static TANGRAM_RUN_PATH: &str = ".tangram/run";

impl Runtime {
	pub async fn bundle(
		&self,
		build: &tg::Build,
		_remote: Option<String>,
	) -> tg::Result<tg::Value> {
		let server = &self.server;

		// Get the target.
		let target = build.target(server).await?;

		// Get the args.
		let args = target.args(server).await?;

		// Get the artifact.
		let artifact: tg::Artifact = args
			.get(1)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.clone()
			.try_into()
			.ok()
			.ok_or_else(|| tg::error!("expected an artifact"))?;

		// Collect the artifact's recursive artifact dependencies.
		let dependencies = Box::pin(artifact.recursive_dependencies(server)).await?;

		// If there are no dependencies, then return the artifact.
		if dependencies.is_empty() {
			return Ok(artifact.into());
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

			// If the artifact is an executable file, then create a directory and place the executable at `.tangram/run`.
			tg::Artifact::File(file) if file.executable(server).await? => {
				tg::directory::Builder::default()
					.add(server, TANGRAM_RUN_PATH.as_ref(), file.clone().into())
					.await?
					.build()
					.into()
			},

			// Otherwise, return an error.
			artifact => {
				return Err(tg::error!(
					?artifact,
					"the artifact must be a directory or an executable file"
				));
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
			.builder(server)
			.await?
			.add(
				server,
				TANGRAM_ARTIFACTS_PATH.as_ref(),
				artifacts_directory.into(),
			)
			.await?
			.build();

		Ok(output.into())
	}

	/// Remove all dependencies from an artifact and its children recursively.
	async fn remove_dependencies(
		&self,
		artifact: &tg::Artifact,
		depth: usize,
		transaction: Option<&Transaction<'_>>,
	) -> tg::Result<tg::Artifact> {
		let server = &self.server;
		match artifact {
			// If the artifact is a directory, then recurse to remove dependencies from its entries.
			tg::Artifact::Directory(directory) => {
				let entries = Box::pin(async move {
					directory
						.entries(server)
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
				let contents = file.contents(server).await?.clone();
				let executable = file.executable(server).await?;
				let file = tg::File::builder(contents).executable(executable).build();
				Ok(file.into())
			},

			// If the artifact is an artifact/path symlink, then replace it with a symlink pointing to `.tangram/artifacts/<id>`. If it's a target symlink, keep the existing path.
			tg::Artifact::Symlink(symlink) => {
				// Render the target.
				let target = if let Some(target_path) = symlink.target(server).await? {
					target_path
				} else if let Some(artifact) = symlink.artifact(server).await? {
					let mut target = PathBuf::new();
					let path = symlink.subpath(server).await?;
					for _ in 0..depth - 1 {
						target.push("..");
					}
					target.push(TANGRAM_ARTIFACTS_PATH);
					target.push(artifact.id(server).await?.to_string());
					if let Some(path) = path.as_ref() {
						target.push(path);
					}
					target
				} else {
					let symlink_id = symlink.id(server).await?;
					return Err(
						tg::error!(%symlink_id, "could not determine target or artifact for symlink"),
					);
				};

				let symlink = tg::Symlink::with_target(target);
				Ok(symlink.into())
			},
		}
	}
}
