use super::Runtime;
use crate::database::Transaction;
use futures::{stream::FuturesOrdered, TryStreamExt as _};
use once_cell::sync::Lazy;
use tangram_client as tg;

static TANGRAM_ARTIFACTS_PATH: Lazy<tg::Path> = Lazy::new(|| ".tangram/artifacts".parse().unwrap());

static TANGRAM_RUN_PATH: Lazy<tg::Path> = Lazy::new(|| ".tangram/run".parse().unwrap());

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

		// Collect the artifact's recursive references.
		let references = Box::pin(artifact.recursive_references(server)).await?;

		// If there are no references, then return the artifact.
		if references.is_empty() {
			return Ok(artifact.into());
		}

		// Create the artifacts directory by removing all references from the referenced artifacts.
		let entries = references
			.into_iter()
			.map(|id| async move {
				let artifact = tg::Artifact::with_id(id.clone());
				let artifact = self.remove_references(&artifact, 3, None).await?;
				Ok::<_, tg::Error>((id.to_string(), artifact))
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		let artifacts_directory = tg::Directory::new(entries);

		// Create the bundle directory.
		let output: tg::Artifact = match artifact {
			// If the artifact is a directory, use it as is.
			tg::Artifact::Directory(directory) => directory.clone().into(),

			// If the artifact is an executable file, then create a directory and place the executable at `.tangram/run`.
			tg::Artifact::File(file) if file.executable(server).await? => {
				tg::directory::Builder::default()
					.add(server, &TANGRAM_RUN_PATH, file.clone().into())
					.await?
					.build()
					.into()
			},

			// Otherwise, return an error.
			artifact => {
				return Err(tg::error!(
					%artifact,
					"the artifact must be a directory or an executable file"
				))
			},
		};

		// Remove references from the bundle directory.
		let output = self
			.remove_references(&output, 0, None)
			.await?
			.try_unwrap_directory()
			.ok()
			.ok_or_else(|| tg::error!("the artifact must be a directory"))?;

		// Add the artifacts directory to the bundled artifact at `.tangram/artifacts`.
		let output = output
			.builder(server)
			.await?
			.add(server, &TANGRAM_ARTIFACTS_PATH, artifacts_directory.into())
			.await?
			.build();

		Ok(output.into())
	}

	/// Remove all references from an artifact and its children recursively.
	async fn remove_references(
		&self,
		artifact: &tg::Artifact,
		depth: usize,
		transaction: Option<&Transaction<'_>>,
	) -> tg::Result<tg::Artifact> {
		let server = &self.server;
		match artifact {
			// If the artifact is a directory, then recurse to remove references from its entries.
			tg::Artifact::Directory(directory) => {
				let entries = Box::pin(async move {
					directory
						.entries(server)
						.await?
						.iter()
						.map(|(name, artifact)| async move {
							let artifact = self
								.remove_references(artifact, depth + 1, transaction)
								.await?;
							Ok::<_, tg::Error>((name.clone(), artifact))
						})
						.collect::<FuturesOrdered<_>>()
						.try_collect()
						.await
				})
				.await?;

				let directory = tg::Directory::new(entries);
				Ok(directory.into())
			},

			// If the artifact is a file, then return the file without any references.
			tg::Artifact::File(file) => {
				let contents = file.contents(server).await?.clone();
				let executable = file.executable(server).await?;
				let references = vec![];
				let file = tg::File::new(contents, executable, references);
				Ok(file.into())
			},

			// If the artifact is a symlink, then replace it with a symlink pointing to `.tangram/artifacts/<id>`.
			tg::Artifact::Symlink(symlink) => {
				// Render the target.
				let mut target = tg::Path::new();
				let artifact = symlink.artifact(server).await?;
				let path = symlink.path(server).await?;
				if let Some(artifact) = artifact.as_ref() {
					for _ in 0..depth - 1 {
						target.push(tg::path::Component::Parent);
					}
					target = target.join(
						TANGRAM_ARTIFACTS_PATH
							.clone()
							.join(artifact.id(server).await?.to_string()),
					);
				}
				if let Some(path) = path.as_ref() {
					target = target.join(path.clone());
				}
				let symlink = tg::Symlink::new(None, Some(target));
				Ok(symlink.into())
			},
		}
	}
}
