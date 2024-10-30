use crate::{util::path::Ext as _, Server};
use futures::{future::BoxFuture, Stream, StreamExt as _};
use std::{
	path::PathBuf,
	pin::pin,
	sync::{Arc, RwLock},
};
use tangram_client as tg;
use tangram_futures::stream::TryStreamExt as _;
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};

mod input;
mod lockfile;
mod object;
mod output;
#[cfg(test)]
mod tests;
mod unify;

impl Server {
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkin::Output>>>,
	> {
		let progress = crate::progress::Handle::new();
		tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				let result = server.check_in_artifact_task(arg, Some(&progress)).await;
				match result {
					Ok(output) => progress.output(output),
					Err(error) => progress.error(error),
				};
			}
		});
		let stream = progress.stream();
		Ok(stream)
	}

	/// Attempt to store an artifact in the database.
	async fn check_in_artifact_task(
		&self,
		arg: tg::artifact::checkin::Arg,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<tg::artifact::checkin::Output> {
		// If this is a checkin of a path in the checkouts directory, then retrieve the corresponding artifact.
		if let Some(path) = arg
			.path
			.diff(self.cache_path())
			.filter(|path| matches!(path.components().next(), Some(std::path::Component::CurDir)))
		{
			let components = path.components().collect::<Vec<_>>();
			let id = components
				.get(1)
				.map(|component| {
					let std::path::Component::Normal(name) = component else {
						return Err(tg::error!("invalid path"));
					};
					name.to_str().ok_or_else(|| tg::error!("non-utf8 path"))
				})
				.ok_or_else(|| tg::error!("cannot check in the checkouts directory"))??
				.parse()?;
			if components.len() < 2 {
				let output = tg::artifact::checkin::Output { artifact: id };
				return Ok(output);
			}
			let mut path = PathBuf::new();
			for component in &components[2..] {
				path.push(component);
			}
			let artifact = tg::Artifact::with_id(id);
			let directory = artifact
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?;
			let artifact = directory.get(self, path).await?;
			let id = artifact.id(self).await?;
			let output = tg::artifact::checkin::Output { artifact: id };
			return Ok(output);
		}

		// Check in the artifact.
		self.check_in_or_store_artifact_inner(arg.clone(), None, progress)
			.await
	}

	// Check in the artifact.
	async fn check_in_or_store_artifact_inner(
		&self,
		mut arg: tg::artifact::checkin::Arg,
		store_as: Option<&tg::artifact::Id>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<tg::artifact::checkin::Output> {
		// Verify the path is absolute.
		if !arg.path.is_absolute() {
			return Err(tg::error!(%path = arg.path.display(), "expected an absolute path"));
		}

		// Canonicalize the path.
		let path = tokio::fs::canonicalize(
			arg.path
				.parent()
				.ok_or_else(|| tg::error!("expected a parent path"))?,
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
		.join(
			arg.path
				.file_name()
				.ok_or_else(|| tg::error!("expected a non-empty path"))?,
		);
		arg.path = path;

		// Collect the input.
		let input = self
			.create_input_graph(arg.clone(), progress)
			.await
			.map_err(
				|source| tg::error!(!source, %path = arg.path.display(), "failed to collect the input"),
			)?;
		self.select_lockfiles(input.clone()).await?;

		// Construct the graph for unification.
		let (mut unification_graph, root) = self
			.create_unification_graph(input.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to construct the object graph"))?;

		// Unify.
		if !arg.deterministic {
			unification_graph = self
				.unify_dependencies(unification_graph, &root)
				.await
				.map_err(|source| tg::error!(!source, "failed to unify the object graph"))?;

			// Validate.
			unification_graph.validate().await?;
		}

		// Create the object graph.
		let object_graph = self
			.create_object_graph(&root, unification_graph)
			.await
			.map_err(|source| tg::error!(!source, "failed to create object graph"))?;

		// Create the output graph.
		let output = self
			.create_output_graph(&object_graph)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the output graph"))?;

		// Get the artifact ID.
		let artifact = self
			.find_output_from_input(&arg.path, input.clone(), output.clone())
			.await?
			.ok_or_else(|| tg::error!(%path = arg.path.display(), "missing path in output"))?;

		if let Some(store_as) = store_as {
			// Store if requested.
			if store_as != &artifact {
				return Err(tg::error!("the checkouts directory is corrupted"));
			}
			self.write_output_to_database(output.clone()).await?;
		} else {
			// Copy or move files.
			self.copy_or_move_to_checkouts_directory(output.clone(), progress)
				.await?;

			// Write lockfiles.
			let lockfile = self.create_lockfile(&object_graph).await?;
			self.write_lockfiles(input.clone(), &lockfile, &object_graph.paths)
				.await?;
		}

		// Create the output.
		let output = tg::artifact::checkin::Output { artifact };

		Ok(output)
	}

	#[allow(clippy::only_used_in_recursion, clippy::needless_pass_by_value)]
	async fn find_output_from_input(
		&self,
		path: &PathBuf,
		input: Arc<tokio::sync::RwLock<input::Graph>>,
		output: Arc<RwLock<output::Graph>>,
	) -> tg::Result<Option<tg::artifact::Id>> {
		if &input.read().await.arg.path == path {
			return Ok(Some(output.read().unwrap().id.clone()));
		}
		// Recurse over path dependencies.
		let dependencies = input
			.read()
			.await
			.edges
			.iter()
			.filter_map(|edge| {
				let child = edge.node()?;
				edge.reference
					.item()
					.try_unwrap_path_ref()
					.ok()
					.or_else(|| edge.reference.options()?.path.as_ref())
					.map(|_| (edge.reference.clone(), child))
			})
			.collect::<Vec<_>>();
		for (reference, child_input) in dependencies {
			let child_output = output
				.read()
				.unwrap()
				.edges
				.iter()
				.find(|edge| edge.reference == reference)
				.ok_or_else(
					|| tg::error!(%referrer = path.display(), %reference, "missing output reference"),
				)?
				.node();
			if let Some(id) =
				Box::pin(self.find_output_from_input(path, child_input, child_output)).await?
			{
				return Ok(Some(id));
			}
		}
		Ok(None)
	}

	pub(crate) fn try_store_artifact_future(
		&self,
		id: &tg::artifact::Id,
	) -> BoxFuture<'static, tg::Result<bool>> {
		let server = self.clone();
		let id = id.clone();
		Box::pin(async move { server.try_store_artifact_inner(&id).await })
	}

	pub(crate) async fn try_store_artifact_inner(&self, id: &tg::artifact::Id) -> tg::Result<bool> {
		// Check if the artifact exists in the checkouts directory.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let path = self.cache_path().join(id.to_string());
		let exists = tokio::fs::try_exists(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the file exists"))?;
		if !exists {
			return Ok(false);
		}
		drop(permit);
		let arg = tg::artifact::checkin::Arg {
			deterministic: false,
			destructive: false,
			ignore: false,
			locked: true,
			path,
		};
		self.check_in_or_store_artifact_inner(arg.clone(), Some(id), None)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to store the artifact"))?;
		Ok(true)
	}
}

impl Server {
	pub(crate) async fn handle_check_in_artifact_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = handle.check_in_artifact(arg).await?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None => {
				pin!(stream)
					.try_last()
					.await?
					.and_then(|event| event.try_unwrap_output().ok())
					.ok_or_else(|| tg::error!("stream ended without output"))?;
				(None, Outgoing::empty())
			},

			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Outgoing::sse(stream))
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
