use crate::Server;
use futures::{FutureExt as _, Stream, StreamExt as _, TryFutureExt as _};
use indoc::indoc;
use std::{panic::AssertUnwindSafe, path::PathBuf};
use tangram_client as tg;
use tangram_futures::stream::Ext as _;
use tangram_http::{Body, request::Ext as _};
use tangram_ignore::Matcher;
use tokio_util::task::AbortOnDropHandle;

mod input;
mod lockfile;
mod object;
mod output;
mod unify;

impl Server {
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkin::Output>>>
		+ Send
		+ 'static,
	> {
		let progress = crate::progress::Handle::new();
		let task = tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				let result = AssertUnwindSafe(server.check_in_artifact_inner(arg, Some(&progress)))
					.catch_unwind()
					.await;
				match result {
					Ok(Ok(output)) => {
						progress.output(output);
					},
					Ok(Err(error)) => {
						progress.error(error);
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						progress.error(tg::error!(?message, "the task panicked"));
					},
				}
			}
		});
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = progress.stream().attach(abort_handle);
		Ok(stream)
	}

	// Check in the artifact.
	async fn check_in_artifact_inner(
		&self,
		mut arg: tg::artifact::checkin::Arg,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<tg::artifact::checkin::Output> {
		// Canonicalize the path's parent.
		arg.path = crate::util::fs::canonicalize_parent(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, %path = &arg.path.display(), "failed to canonicalize the path's parent"))?;

		// If this is a checkin of a path in the cache directory, then retrieve the corresponding artifact.
		if let Ok(path) = arg.path.strip_prefix(self.cache_path()) {
			let id = path
				.components()
				.next()
				.map(|component| {
					let std::path::Component::Normal(name) = component else {
						return Err(tg::error!("invalid path"));
					};
					name.to_str().ok_or_else(|| tg::error!("non-utf8 path"))
				})
				.ok_or_else(|| tg::error!("cannot check in the cache directory"))??
				.parse()?;
			if path.components().count() == 1 {
				let output = tg::artifact::checkin::Output { artifact: id };
				return Ok(output);
			}
			let path = path.components().skip(1).collect::<PathBuf>();
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

		// Create the input graph.
		let input_graph = self
			.create_input_graph(arg.clone(), progress)
			.await
			.map_err(
				|source| tg::error!(!source, %path = arg.path.display(), "failed to collect the input"),
			)?;

		// Create the unification graph and get its root node.
		let (unification_graph, root) = self
			.create_unification_graph(&input_graph, arg.deterministic)
			.await
			.map_err(|source| tg::error!(!source, "failed to unify dependencies"))?;

		// Create the object graph.
		let object_graph = self
			.create_object_graph(&input_graph, &unification_graph, &root)
			.await
			.map_err(|source| tg::error!(!source, "failed to create objects"))?;

		// Create the output graph.
		let output_graph = self
			.create_output_graph(&input_graph, &object_graph)
			.await
			.map_err(|source| tg::error!(!source, "failed to write objects"))?;

		// Copy the blobs.
		self.copy_blobs(&output_graph, &input_graph)
			.await
			.map_err(|source| tg::error!(!source, "failed to copy the blobs"))?;

		// Write the output to the database and the store.
		futures::try_join!(
			self.write_output_to_database(output_graph.clone(), object_graph.clone())
				.map_err(|source| tg::error!(
					!source,
					"failed to write the objects to the database"
				)),
			self.write_output_to_store(output_graph.clone(), object_graph.clone())
				.map_err(|source| tg::error!(!source, "failed to store the objects"))
		)?;

		// Copy or move to the cache directory.
		if arg.cache || arg.destructive {
			self.copy_or_move_to_cache_directory(&input_graph, &output_graph, 0, progress)
				.await
				.map_err(|source| tg::error!(!source, "failed to cache the artifact"))?;
		}

		// Get the artifact.
		let artifact = output_graph.nodes[0].id.clone();

		// If this is a non-destructive checkin, then attempt to write a lockfile.
		if arg.lockfile && !arg.destructive && artifact.is_directory() {
			self.try_write_lockfile(&input_graph, &object_graph)
				.await
				.map_err(|source| tg::error!(!source, "failed to write lockfile"))?;
		}

		// Create the output.
		let output = tg::artifact::checkin::Output { artifact };

		Ok(output)
	}

	pub(crate) async fn ignore_matcher_for_checkin(&self) -> tg::Result<Matcher> {
		let file_names = vec![
			".tangramignore".into(),
			".tgignore".into(),
			".gitignore".into(),
		];
		let global = indoc!(
			"
				.DS_Store
				.git
				.tangram
				tangram.lock
			"
		);
		Matcher::new(file_names, Some(global))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the ignore"))
	}
}

impl Server {
	pub(crate) async fn handle_check_in_artifact_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
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
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
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
