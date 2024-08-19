use crate::Server;
use futures::{future::BoxFuture, stream, FutureExt as _, Stream, StreamExt as _};
use std::{
	collections::BTreeMap,
	sync::{atomic::AtomicU64, Arc},
};
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tokio_stream::wrappers::IntervalStream;

mod input;
mod output;
mod unify;

struct State {
	count: ProgressState,
	weight: ProgressState,
}

struct ProgressState {
	current: AtomicU64,
	total: Option<AtomicU64>,
}

impl Server {
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::artifact::checkin::Event>>> {
		// Create the state.
		let count = ProgressState {
			current: AtomicU64::new(0),
			total: None,
		};
		let weight = ProgressState {
			current: AtomicU64::new(0),
			total: None,
		};
		let state = Arc::new(State { count, weight });

		// Spawn the task.
		let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
		tokio::spawn({
			let server = self.clone();
			let arg = arg.clone();
			let state = state.clone();
			async move {
				let result = server.check_in_artifact_task(arg, &state).await;
				result_sender.send(result).ok();
			}
		});

		// Create the stream.
		let interval = std::time::Duration::from_millis(100);
		let interval = tokio::time::interval(interval);
		let result = result_receiver.map(Result::unwrap).shared();
		let stream = IntervalStream::new(interval)
			.map(move |_| {
				let current = state
					.count
					.current
					.load(std::sync::atomic::Ordering::Relaxed);
				let total = state
					.count
					.total
					.as_ref()
					.map(|total| total.load(std::sync::atomic::Ordering::Relaxed));
				let count = tg::Progress { current, total };
				let current = state
					.weight
					.current
					.load(std::sync::atomic::Ordering::Relaxed);
				let total = state
					.weight
					.total
					.as_ref()
					.map(|total| total.load(std::sync::atomic::Ordering::Relaxed));
				let weight = tg::Progress { current, total };
				let progress = tg::artifact::checkin::Progress { count, weight };
				Ok(tg::artifact::checkin::Event::Progress(progress))
			})
			.take_until(result.clone())
			.chain(stream::once(result.map(|result| match result {
				Ok(id) => Ok(tg::artifact::checkin::Event::End(id)),
				Err(error) => Err(error),
			})));

		Ok(stream)
	}

	/// Attempt to store an artifact in the database.
	async fn check_in_artifact_task(
		&self,
		arg: tg::artifact::checkin::Arg,
		_state: &State,
	) -> tg::Result<tg::artifact::Id> {
		// If this is a checkin of a path in the checkouts directory, then retrieve the corresponding artifact.
		let checkouts_path = self.checkouts_path().try_into()?;
		if let Some(path) = arg.path.diff(&checkouts_path).filter(tg::Path::is_internal) {
			let id = path
				.components()
				.get(1)
				.ok_or_else(|| tg::error!("cannot check in the checkouts directory"))?
				.try_unwrap_normal_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?
				.parse::<tg::artifact::Id>()?;
			let path = tg::Path::with_components(path.components().iter().skip(2).cloned());
			if path.components().len() == 1 {
				return Ok(id);
			}
			let artifact = tg::Artifact::with_id(id);
			let directory = artifact
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?;
			let artifact = directory.get(self, &path).await?;
			let id = artifact.id(self).await?;
			return Ok(id);
		}

		// Check in the artifact.
		self.check_in_or_store_artifact_inner(arg.clone(), None)
			.await
	}

	// Check in the artifact.
	pub async fn check_in_or_store_artifact_inner(
		&self,
		arg: tg::artifact::checkin::Arg,
		store_as: Option<&tg::artifact::Id>,
	) -> tg::Result<tg::artifact::Id> {
		// Collect the input.
		let input = self.collect_input(arg.clone()).await.map_err(
			|source| tg::error!(!source, %path = arg.path, "failed to collect check-in input"),
		)?;

		// Construct the graph for unification.
		let (mut unification_graph, root) = self
			.create_unification_graph(input.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to construct object graph"))?;

		// Unify.
		if !arg.deterministic {
			unification_graph = self
				.unify_dependencies(unification_graph, &root)
				.await
				.map_err(|source| tg::error!(!source, "failed to unify object graph"))?;
		}

		// Validate.
		unification_graph.validate(self)?;

		// Create the lock that is written to disk.
		let output_graph = self.create_output_graph(&unification_graph, &root).await?;

		// Get the output.
		let (output, lock_with_objects) = self.collect_output(input.clone(), &output_graph).await?;

		// Collect all the file output.
		let mut old_files = BTreeMap::new();
		let mut stack = vec![&output];
		while let Some(output) = stack.pop() {
			if let tg::artifact::Data::File(file) = &output.data {
				let id = tg::file::Id::new(&file.serialize()?);
				old_files.insert(id, file.clone());
			}
			stack.extend(output.dependencies.values());
		}

		// Split up locks.
		let graphs = self.split_graphs(&lock_with_objects, &old_files).await?;

		// Replace files.
		// let mut output = output.clone();
		// let mut stack = vec![&mut output];
		// while let Some(output) = stack.pop() {
		// 	if let Some(data) = new_files.get(&output.lock_index) {
		// 		output.data = data.clone().into();
		// 	}
		// 	stack.extend(output.dependencies.values_mut());
		// }

		// Write the lockfile if necessary.
		// if input.write_lock {
		// 	self.write_lockfile(&input.arg.path, &output_graph).await?;
		// }

		// Get the root object. TODO this is garbage, don't do it.
		let (graph, node) = graphs
			.get(&0usize)
			.ok_or_else(|| tg::error!("corupted locks"))?;
		let graph = tg::Graph::with_id(graph.clone());
		let artifact: tg::artifact::Id = match graph.load(self).await?.nodes[*node]
			.data(self)
			.await?
		{
			tg::graph::data::Node::Directory(_) => tg::Directory::with_graph_and_node(graph, *node)
				.id(self)
				.await?
				.into(),
			tg::graph::data::Node::File(_) => tg::File::with_graph_and_node(graph, *node)
				.id(self)
				.await?
				.into(),
			tg::graph::data::Node::Symlink(_) => tg::Symlink::with_graph_and_node(graph, *node)
				.id(self)
				.await?
				.into(),
		};

		if let Some(store_as) = store_as {
			// Store if requested.
			if store_as != &artifact {
				return Err(tg::error!("checkouts directory is corrupted"));
			}
			self.write_output_to_database(&output).await?;
		} else {
			// Otherwise, update hardlinks and xattrs.
			self.write_hardlinks_and_xattrs(input, &output).await?;
		}

		Ok(artifact)
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
		let path = self.checkouts_path().join(id.to_string());
		let exists = tokio::fs::try_exists(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the file exists"))?;
		if !exists {
			return Ok(false);
		}
		drop(permit);

		// Check in the artifact.
		let arg = tg::artifact::checkin::Arg {
			dependencies: true,
			deterministic: false,
			destructive: false,
			locked: true,
			path: path.try_into()?,
		};
		let _artifact = self
			.check_in_or_store_artifact_inner(arg.clone(), Some(id))
			.await?;
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
		let arg = request.json().await?;
		let stream = handle.check_in_artifact(arg).await?;
		let sse = stream.map(|result| match result {
			Ok(tg::artifact::checkin::Event::Progress(progress)) => {
				let data = serde_json::to_string(&progress).unwrap();
				let event = tangram_http::sse::Event {
					data,
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Ok(tg::artifact::checkin::Event::End(artifact)) => {
				let event = "end".to_owned();
				let data = serde_json::to_string(&artifact).unwrap();
				let event = tangram_http::sse::Event {
					event: Some(event),
					data,
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Err(error) => {
				let data = serde_json::to_string(&error).unwrap();
				let event = "error".to_owned();
				let event = tangram_http::sse::Event {
					data,
					event: Some(event),
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
		});
		let body = Outgoing::sse(sse);
		let response = http::Response::builder().ok().body(body).unwrap();
		Ok(response)
	}
}
