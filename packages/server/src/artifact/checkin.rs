use crate::Server;
use futures::{Stream, StreamExt as _};
use indoc::indoc;
use std::path::PathBuf;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};
use tangram_ignore::Ignore;

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
		let task = tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move { server.check_in_artifact_inner(arg, Some(&progress)).await }
		});
		tokio::spawn({
			let progress = progress.clone();
			async move {
				match task.await {
					Ok(Ok(output)) => {
						progress.output(output);
					},
					Ok(Err(error)) => {
						progress.error(error);
					},
					Err(source) => {
						progress.error(tg::error!(!source, "the task panicked"));
					},
				};
			}
		});
		let stream = progress.stream();
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
			.map_err(|source| tg::error!(!source, "failed to construct the object graph"))?;

		// Create the object graph.
		let object_graph = self
			.create_object_graph(&input_graph, &unification_graph, &root)
			.await
			.map_err(|source| tg::error!(!source, "failed to create object graph"))?;

		// Create the output graph.
		let output_graph = self
			.create_output_graph(&input_graph, &object_graph)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the output graph"))?;

		// Copy or move to the cache directory.
		self.copy_or_move_to_cache_directory(&input_graph, &output_graph, 0, progress)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to copy or move to the cache directory")
			})?;

		// Write the output to the database.
		self.write_output_to_database(&output_graph)
			.await
			.map_err(|source| tg::error!(!source, "failed to write to the database"))?;

		// Get the artifact.
		let artifact = self
			.find_output_from_input(&arg.path, &input_graph, &output_graph)
			.await?
			.ok_or_else(|| tg::error!(%path = arg.path.display(), "missing path in output"))?;

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

	async fn find_output_from_input(
		&self,
		path: &PathBuf,
		input: &input::Graph,
		output: &output::Graph,
	) -> tg::Result<Option<tg::artifact::Id>> {
		let mut stack = vec![0];
		let mut visited = vec![false; output.nodes.len()];
		while let Some(output_index) = stack.pop() {
			if visited[output_index] {
				continue;
			}
			visited[output_index] = true;
			let Some(input_index) = output.nodes[output_index].input else {
				continue;
			};
			let input_node = &input.nodes[input_index];
			if &input_node.arg.path == path {
				return Ok(Some(output.nodes[output_index].id.clone()));
			}
			stack.extend(
				output.nodes[output_index]
					.edges
					.iter()
					.map(|edge| edge.node),
			);
		}
		Ok(None)
	}

	pub(crate) async fn ignore_for_checkin(&self) -> tg::Result<Ignore> {
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
		Ignore::new(file_names, Some(global))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the ignore"))
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
