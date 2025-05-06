use crate::{Server, lockfile::Lockfile};
use bytes::Bytes;
use futures::{FutureExt as _, Stream, StreamExt as _, TryFutureExt as _};
use indoc::indoc;
use std::{
	os::unix::fs::PermissionsExt as _,
	panic::AssertUnwindSafe,
	path::{Path, PathBuf},
	sync::Arc,
	time::Instant,
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::stream::Ext as _;
use tangram_http::{Body, request::Ext as _};
use tangram_ignore as ignore;
use tokio_util::task::AbortOnDropHandle;

mod input;
mod lockfile;
mod object;
mod output;
mod unify;

struct State {
	arg: tg::checkin::Arg,
	graph: Graph,
	graph_objects: Vec<GraphObject>,
	lockfile: Option<Lockfile>,
	locked: bool,
	ignorer: Option<ignore::Ignorer>,
	progress: crate::progress::Handle<tg::checkin::Output>,
}

struct GraphObject {
	id: tg::graph::Id,
	data: tg::graph::Data,
	bytes: Bytes,
}

#[derive(Clone, Debug)]
pub struct Graph {
	nodes: im::Vector<Node>,
	paths: radix_trie::Trie<PathBuf, usize>,
	roots: im::OrdMap<usize, Vec<usize>>,
}

#[derive(Clone, Debug)]
struct Node {
	metadata: Option<std::fs::Metadata>,
	object: Option<Object>,
	path: Option<Arc<PathBuf>>,
	root: Option<usize>,
	tag: Option<tg::Tag>,
	variant: Variant,
}

#[derive(Clone, Debug, derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
enum Variant {
	Directory(Directory),
	File(File),
	Symlink(Symlink),
	Object,
}

#[derive(Clone, Debug)]
struct Directory {
	entries: Vec<(String, usize)>,
}

#[derive(Clone, Debug)]
struct File {
	blob: Option<Blob>,
	executable: bool,
	dependencies: Vec<FileDependency>,
}

#[derive(Clone, Debug)]
enum Blob {
	Create(crate::blob::create::Blob),
	Id(tg::blob::Id),
}

#[derive(Clone, Debug)]
enum FileDependency {
	Import {
		import: tg::module::Import,
		path: Option<PathBuf>,
		subpath: Option<PathBuf>,
		node: Option<usize>,
	},
	Referent {
		reference: tg::Reference,
		referent: tg::Referent<Option<Either<tg::object::Id, usize>>>,
	},
}

#[derive(Clone, Debug)]
struct Symlink {
	target: PathBuf,
}

#[derive(Clone, Debug)]
struct Object {
	bytes: Bytes,
	data: tg::object::Data,
	id: tg::object::Id,
}

impl Server {
	pub async fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
	> {
		if arg.destructive && arg.ignore {
			return Err(tg::error!("ignore is forbidden for destructive checkins"));
		}

		let progress = crate::progress::Handle::new();
		let task = tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				let result = AssertUnwindSafe(server.checkin_inner(arg, &progress))
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
	async fn checkin_inner(
		&self,
		mut arg: tg::checkin::Arg,
		progress: &crate::progress::Handle<tg::checkin::Output>,
	) -> tg::Result<tg::checkin::Output> {
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
				let output = tg::checkin::Output { artifact: id };
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
			let output = tg::checkin::Output { artifact: id };
			return Ok(output);
		}

		self.checkin_new(arg, progress).await
	}

	async fn checkin_new(
		&self,
		arg: tg::checkin::Arg,
		progress: &crate::progress::Handle<tg::checkin::Output>,
	) -> tg::Result<tg::checkin::Output> {
		// Create the ignorer if necessary.
		let ignorer = if arg.ignore {
			Some(Self::checkin_create_ignorer()?)
		} else {
			None
		};

		// Search for the root.
		let root = tg::package::try_get_nearest_package_path_for_path(&arg.path)?
			.unwrap_or(&arg.path)
			.to_owned();

		// Parse a lockfile if it exists.
		let lockfile = self
			.try_parse_lockfile(&root)
			.map_err(|source| tg::error!(!source, "failed to read lockfile"))?;

		// Create the state.
		let graph = Graph {
			nodes: im::Vector::new(),
			paths: radix_trie::Trie::default(),
			roots: im::OrdMap::new(),
		};
		let mut state = State {
			arg: arg.clone(),
			graph,
			graph_objects: Vec::new(),
			lockfile,
			locked: arg.locked,
			ignorer,
			progress: progress.clone(),
		};

		// Collect input.
		let start = Instant::now();
		let mut state = tokio::task::spawn_blocking({
			let server = self.clone();
			move || {
				server.checkin_collect_input(&mut state, root)?;
				Ok::<_, tg::Error>(state)
			}
		})
		.await
		.unwrap()?;
		tracing::trace!(elapsed = ?start.elapsed(), "input");

		// Remove the ignorer.
		state.ignorer.take();

		// Unify.
		if !(state.arg.deterministic || state.arg.locked) {
			let start = Instant::now();
			self.unify_file_dependencies(&mut state).await?;
			tracing::trace!(elapsed = ?start.elapsed(), "unify");
		}

		// Create blobs.
		let start = Instant::now();
		self.checkin_create_blobs(&mut state).await?;
		tracing::trace!(elapsed = ?start.elapsed(), "create blobs");

		// Create objects.
		let start = Instant::now();
		Self::checkin_create_objects(&mut state)?;
		tracing::trace!(elapsed = ?start.elapsed(), "create objects");

		// Set the touch time.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Get the root node's ID.
		let root: tg::artifact::Id = state.graph.nodes[0]
			.object
			.as_ref()
			.unwrap()
			.id
			.clone()
			.try_into()
			.unwrap();

		let state = Arc::new(state);

		let cache_and_store_future = tokio::spawn({
			let server = self.clone();
			let state = state.clone();
			async move {
				// Cache the objects.
				let start = Instant::now();
				server
					.checkin_cache_task(state.clone(), touched_at)
					.await
					.map_err(|source| tg::error!(!source, "failed to copy blobs"))?;
				tracing::trace!(elapsed = ?start.elapsed(), "copy blobs");

				// Store the objects.
				let start = Instant::now();
				server
					.checkin_store_task(&state, touched_at)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to write the objects to the store")
					})?;
				tracing::trace!(elapsed = ?start.elapsed(), "write objects to store");
				Ok::<_, tg::Error>(())
			}
			.inspect_err(|error| tracing::error!(?error, "cache and store task failed"))
		})
		.map(|result| result.unwrap());

		let messenger_future = tokio::spawn({
			let server = self.clone();
			let state = state.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_messenger_task(&state, touched_at)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to write the objects to the messenger")
					})?;
				tracing::trace!(elapsed = ?start.elapsed(), "write objects to messenger");
				Ok::<_, tg::Error>(())
			}
			.inspect_err(|error| tracing::error!(?error, "messenger task failed"))
		})
		.map(|result| result.unwrap());

		let lockfile_future = tokio::spawn({
			let server = self.clone();
			let state = state.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_create_lockfile_task(&state)
					.await
					.map_err(|source| tg::error!(!source, "failed to create lockfile"))?;
				tracing::trace!(elapsed = ?start.elapsed(), "writing lockfile");
				Ok::<_, tg::Error>(())
			}
			.inspect_err(|error| tracing::error!(?error, "lockfile task failed"))
		})
		.map(|result| result.unwrap());

		futures::try_join!(cache_and_store_future, messenger_future, lockfile_future)?;

		let _state = Arc::into_inner(state).unwrap();

		// Create the output.
		let output = tg::checkin::Output { artifact: root };

		Ok(output)
	}

	pub(crate) fn checkin_create_ignorer() -> tg::Result<ignore::Ignorer> {
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
		ignore::Ignorer::new(file_names, Some(global))
			.map_err(|source| tg::error!(!source, "failed to create the matcher"))
	}
}

impl Server {
	pub(crate) async fn handle_checkin_request<H>(
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
		let stream = handle.checkin(arg).await?;

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

impl Node {
	fn is_package(&self) -> bool {
		self.root_module_name().is_some()
	}

	fn root_module_name(&self) -> Option<String> {
		self.variant
			.try_unwrap_directory_ref()
			.ok()
			.and_then(|directory| {
				directory.entries.iter().find_map(|(name, _)| {
					tg::package::ROOT_MODULE_FILE_NAMES
						.contains(&name.as_str())
						.then_some(name.to_owned())
				})
			})
	}

	fn path(&self) -> &Path {
		self.path.as_deref().unwrap()
	}

	fn edges(&self) -> Vec<usize> {
		match &self.variant {
			Variant::Directory(directory) => {
				directory.entries.iter().map(|(_, node)| *node).collect()
			},
			Variant::File(file) => file
				.dependencies
				.iter()
				.filter_map(|dependency| match dependency {
					FileDependency::Import { node, .. } => *node,
					FileDependency::Referent { referent, .. } => {
						referent.item.as_ref()?.as_ref().right().copied()
					},
				})
				.collect(),
			Variant::Symlink(_) | Variant::Object => Vec::new(),
		}
	}
}

impl petgraph::visit::GraphBase for Graph {
	type EdgeId = (usize, usize);
	type NodeId = usize;
}

impl petgraph::visit::NodeIndexable for &Graph {
	fn from_index(&self, i: usize) -> Self::NodeId {
		i
	}

	fn node_bound(&self) -> usize {
		self.nodes.len()
	}

	fn to_index(&self, a: Self::NodeId) -> usize {
		a
	}
}

impl petgraph::visit::IntoNeighbors for &Graph {
	type Neighbors = std::vec::IntoIter<usize>;
	fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
		self.nodes[a].edges().into_iter()
	}
}

impl petgraph::visit::IntoNodeIdentifiers for &Graph {
	type NodeIdentifiers = std::ops::Range<usize>;
	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.nodes.len()
	}
}

fn set_permissions_and_times(path: &Path, metadata: &std::fs::Metadata) -> tg::Result<()> {
	if !metadata.is_symlink() {
		let mode = metadata.permissions().mode();
		let executable = mode & 0o111 != 0;
		let new_mode = if metadata.is_dir() || executable {
			0o755
		} else {
			0o644
		};
		if new_mode != mode {
			let permissions = std::fs::Permissions::from_mode(new_mode);
			std::fs::set_permissions(path, permissions).map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to set the permissions"),
			)?;
		}
	}
	let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
	filetime::set_symlink_file_times(path, epoch, epoch).map_err(
		|source| tg::error!(!source, %path = path.display(), "failed to set the modified time"),
	)?;
	Ok(())
}
