use std::sync::atomic::{AtomicU64, Ordering};

use crate::{util, Server};
use futures::{future::BoxFuture, Stream};
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};
mod input;
mod lockfile;
mod output;
mod unify;

struct ProgressState {
	state: Option<tg::progress::State>,
	input_files: AtomicU64,
	dependencies: AtomicU64,
	output_bytes_current: AtomicU64,
	output_bytes_total: AtomicU64,
}

impl Server {
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::Progress<tg::artifact::Id>>>> {
		let stream = tg::progress::progress_stream({
			let server = self.clone();
			|state| async move {
				server
					.clone()
					.check_in_artifact_task(arg, ProgressState::new(Some(state)))
					.await
			}
		});
		Ok(stream)
	}

	/// Attempt to store an artifact in the database.
	async fn check_in_artifact_task(
		&self,
		arg: tg::artifact::checkin::Arg,
		progress: ProgressState,
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
		self.check_in_or_store_artifact_inner(arg.clone(), None, &progress)
			.await
	}

	// Check in the artifact.
	async fn check_in_or_store_artifact_inner(
		&self,
		arg: tg::artifact::checkin::Arg,
		store_as: Option<&tg::artifact::Id>,
		progress: &ProgressState,
	) -> tg::Result<tg::artifact::Id> {
		// Collect the input.
		let input = self.collect_input(arg.clone(), progress).await.map_err(
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
				.unify_dependencies(unification_graph, &root, progress)
				.await
				.map_err(|source| tg::error!(!source, "failed to unify object graph"))?;
		}

		// Validate.
		unification_graph.validate(self)?;

		// Create the lock that is written to disk.
		let lockfile = self
			.create_lockfile(&unification_graph, &root)
			.await
			.map_err(|source| tg::error!(!source, "failed to create lockfile"))?;

		// Get the output.
		let output = self
			.collect_output(input.clone(), lockfile.clone(), progress)
			.await?;

		// Get the artifact ID
		let artifact = output.read().unwrap().data.id()?;

		if let Some(store_as) = store_as {
			// Store if requested.
			if store_as != &artifact {
				return Err(tg::error!("checkouts directory is corrupted"));
			}
			self.write_output_to_database(output, &lockfile).await?;
		} else {
			// Copy or move files.
			self.copy_or_move_to_checkouts_directory(input.clone(), output.clone(), progress)
				.await?;

			// Otherwise, update hardlinks and xattrs.
			self.write_hardlinks_and_xattrs(input.clone(), output)
				.await?;

			// Write lockfiles.
			self.write_lockfiles(input.clone(), &lockfile).await?;
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
			deterministic: false,
			destructive: false,
			locked: true,
			path: path.try_into()?,
		};
		let progress = ProgressState::new(None);
		let _artifact = self
			.check_in_or_store_artifact_inner(arg.clone(), Some(id), &progress)
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
		Ok(util::progress::sse(stream))
	}
}

impl ProgressState {
	fn new(state: Option<tg::progress::State>) -> Self {
		Self {
			state,
			input_files: AtomicU64::new(0),
			dependencies: AtomicU64::new(0),
			output_bytes_current: AtomicU64::new(0),
			output_bytes_total: AtomicU64::new(0),
		}
	}
}

impl ProgressState {
	fn report_input_progress(&self) {
		let Some(state) = &self.state else {
			return;
		};
		let count = self.input_files.fetch_add(1, Ordering::Relaxed);
		state.report_progress([("input files", tg::progress::Data::Count(count))]);
	}

	fn report_dependency_progress(&self) {
		let Some(state) = &self.state else {
			return;
		};
		let count = self.dependencies.fetch_add(1, Ordering::Relaxed);
		state.report_progress([("dependencies", tg::progress::Data::Count(count))]);
	}

	fn report_output_progress(&self, count: u64) {
		let Some(state) = &self.state else {
			return;
		};
		let count = self.output_bytes_current.fetch_add(count, Ordering::Relaxed);
		let total = self.output_bytes_total.load(Ordering::Relaxed);
		state.report_progress([("dependencies", tg::progress::Data::Ratio(count, total))]);
	}
}
