use crate::{database::Transaction, Server};
use futures::{stream::FuturesOrdered, TryStreamExt as _};
use once_cell::sync::Lazy;
use tangram_client as tg;
use tangram_http::{Incoming, Outgoing};

static TANGRAM_ARTIFACTS_PATH: Lazy<tg::Path> = Lazy::new(|| ".tangram/artifacts".parse().unwrap());

static TANGRAM_RUN_PATH: Lazy<tg::Path> = Lazy::new(|| ".tangram/run".parse().unwrap());

impl Server {
	pub async fn bundle_artifact(
		&self,
		id: &tg::artifact::Id,
	) -> tg::Result<tg::artifact::bundle::Output> {
		let artifact = tg::Artifact::with_id(id.clone());

		// Collect the artifact's recursive references.
		let references = Box::pin(artifact.recursive_references(self)).await?;

		// If there are no references, then return the artifact.
		if references.is_empty() {
			let output = tg::artifact::bundle::Output { id: id.clone() };
			return Ok(output);
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
			tg::Artifact::File(file) if file.executable(self).await? => {
				tg::directory::Builder::default()
					.add(self, &TANGRAM_RUN_PATH, file.clone().into())
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
		let output: tg::Artifact = output
			.builder(self)
			.await?
			.add(self, &TANGRAM_ARTIFACTS_PATH, artifacts_directory.into())
			.await?
			.build()
			.into();

		// Create the output.
		let id = output.id(self, None).await?;
		let output = tg::artifact::bundle::Output { id };

		Ok(output)
	}

	/// Remove all references from an artifact and its children recursively.
	async fn remove_references(
		&self,
		artifact: &tg::Artifact,
		depth: usize,
		transaction: Option<&Transaction<'_>>,
	) -> tg::Result<tg::Artifact> {
		match artifact {
			// If the artifact is a directory, then recurse to remove references from its entries.
			tg::Artifact::Directory(directory) => {
				let entries = Box::pin(async move {
					directory
						.entries(self)
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
				let contents = file.contents(self).await?.clone();
				let executable = file.executable(self).await?;
				let references = vec![];
				let file = tg::File::new(contents, executable, references);
				Ok(file.into())
			},

			// If the artifact is a symlink, then replace it with a symlink pointing to `.tangram/artifacts/<id>`.
			tg::Artifact::Symlink(symlink) => {
				// Render the target.
				let mut target = tg::Path::new();
				let artifact = symlink.artifact(self).await?;
				let path = symlink.path(self).await?;
				if let Some(artifact) = artifact.as_ref() {
					for _ in 0..depth - 1 {
						target.push(tg::path::Component::Parent);
					}
					target = target.join(
						TANGRAM_ARTIFACTS_PATH
							.clone()
							.join(artifact.id(self, None).await?.to_string()),
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

impl Server {
	pub(crate) async fn handle_bundle_artifact_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let output = handle.bundle_artifact(&id).await?;
		let response = http::Response::builder()
			.body(Outgoing::json(output))
			.unwrap();
		Ok(response)
	}
}
