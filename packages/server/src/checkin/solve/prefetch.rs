use {crate::Server, dashmap::DashMap, std::sync::Arc, tangram_client::prelude::*};

const PREFETCH_CONCURRENCY: usize = 16;

type Objects = Arc<DashMap<tg::object::Id, tg::object::get::Output, tg::id::BuildHasher>>;

type ObjectTasks = tangram_futures::task::Map<
	tg::object::Id,
	tg::Result<tg::object::get::Output>,
	(),
	tg::id::BuildHasher,
>;

type Tags = Arc<DashMap<tg::tag::Pattern, tg::tag::list::Output, fnv::FnvBuildHasher>>;

type TagTasks = tangram_futures::task::Map<
	tg::tag::Pattern,
	tg::Result<tg::tag::list::Output>,
	(),
	fnv::FnvBuildHasher,
>;

#[derive(Clone)]
pub struct Prefetch {
	arg: tg::checkin::Arg,
	object_tasks: ObjectTasks,
	objects: Objects,
	semaphore: Arc<tokio::sync::Semaphore>,
	tag_tasks: TagTasks,
	tags: Tags,
}

impl Prefetch {
	pub fn new(arg: tg::checkin::Arg) -> Self {
		let object_tasks = tangram_futures::task::Map::default();
		let objects = Arc::new(DashMap::default());
		let semaphore = Arc::new(tokio::sync::Semaphore::new(PREFETCH_CONCURRENCY));
		let tag_tasks = tangram_futures::task::Map::default();
		let tags = Arc::new(DashMap::default());
		Self {
			arg,
			object_tasks,
			objects,
			semaphore,
			tag_tasks,
			tags,
		}
	}

	pub fn abort(&self) {
		self.object_tasks.abort_all();
		self.tag_tasks.abort_all();
	}
}

impl Server {
	pub(super) fn checkin_solve_prefetch_from_lock(
		&self,
		prefetch: &Prefetch,
		lock: &tg::graph::Data,
	) {
		for node in &lock.nodes {
			self.checkin_solve_prefetch_from_graph_node(prefetch, node);
			if let tg::graph::data::Node::File(file) = node {
				for dependency in file.dependencies.values().flatten() {
					if let Some(id) = dependency.id() {
						self.checkin_solve_get_or_spawn_object_task(prefetch, id);
					}
				}
			}
		}
	}

	pub(super) async fn checkin_solve_get_object(
		&self,
		prefetch: &Prefetch,
		id: &tg::object::Id,
	) -> tg::Result<tg::object::get::Output> {
		// Return a cached result if one is available.
		if let Some(output) = prefetch.objects.get(id).map(|value| value.clone()) {
			return Ok(output);
		}

		// Fetch the object directly, bypassing the prefetch semaphore.
		let output = self.checkin_solve_fetch_object(prefetch, id).await?;

		Ok(output)
	}

	fn checkin_solve_get_or_spawn_object_task(
		&self,
		prefetch: &Prefetch,
		id: &tg::object::Id,
	) -> tangram_futures::task::Shared<tg::Result<tg::object::get::Output>, ()> {
		prefetch.object_tasks.get_or_spawn(id.clone(), {
			let server = self.clone();
			let id = id.clone();
			let prefetch = prefetch.clone();
			move |_| async move {
				// Return an existing result if one is available.
				if let Some(output) = prefetch.objects.get(&id).map(|value| value.clone()) {
					return Ok(output);
				}

				// Acquire a permit to limit concurrent requests.
				let permit = prefetch.semaphore.acquire().await;

				// Get the object.
				let output = server.checkin_solve_fetch_object(&prefetch, &id).await;

				// Drop the permit.
				drop(permit);

				output
			}
		})
	}

	async fn checkin_solve_fetch_object(
		&self,
		prefetch: &Prefetch,
		id: &tg::object::Id,
	) -> tg::Result<tg::object::get::Output> {
		// Get the object.
		let arg = tg::object::get::Arg {
			metadata: true,
			..Default::default()
		};
		let output = self
			.get_object(id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
		let data = tg::object::Data::deserialize(id.kind(), output.bytes.clone())
			.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?;

		// If the object is solvable, then spawn tasks to prefetch its descendant objects and tags.
		let solvable = output
			.metadata
			.as_ref()
			.and_then(|metadata| metadata.subtree.solvable)
			.unwrap_or(true);
		if solvable {
			match &data {
				tg::object::Data::Directory(tg::directory::Data::Pointer(pointer))
				| tg::object::Data::File(tg::file::Data::Pointer(pointer))
				| tg::object::Data::Symlink(tg::symlink::Data::Pointer(pointer)) => {
					if let Some(graph_id) = &pointer.graph {
						self.checkin_solve_get_or_spawn_object_task(
							prefetch,
							&graph_id.clone().into(),
						);
					}
				},

				tg::object::Data::Directory(tg::directory::Data::Node(directory)) => {
					let node = tg::graph::data::Node::Directory(directory.clone());
					self.checkin_solve_prefetch_from_graph_node(prefetch, &node);
				},
				tg::object::Data::File(tg::file::Data::Node(file)) => {
					let node = tg::graph::data::Node::File(file.clone());
					self.checkin_solve_prefetch_from_graph_node(prefetch, &node);
				},
				tg::object::Data::Symlink(tg::symlink::Data::Node(symlink)) => {
					let node = tg::graph::data::Node::Symlink(symlink.clone());
					self.checkin_solve_prefetch_from_graph_node(prefetch, &node);
				},

				tg::object::Data::Graph(graph) => {
					for node in &graph.nodes {
						self.checkin_solve_prefetch_from_graph_node(prefetch, node);
					}
				},

				_ => {},
			}
		}

		// Cache the result so prefetch tasks find a cache hit.
		prefetch.objects.insert(id.clone(), output.clone());

		Ok(output)
	}

	fn checkin_solve_prefetch_from_graph_node(
		&self,
		prefetch: &Prefetch,
		node: &tg::graph::data::Node,
	) {
		match node {
			tg::graph::data::Node::Directory(directory) => {
				self.checkin_solve_prefetch_from_directory(prefetch, directory);
			},
			tg::graph::data::Node::File(file) => {
				for reference in file.dependencies.keys() {
					if let tg::reference::Item::Tag(pattern) = reference.item() {
						self.checkin_solve_get_or_spawn_tag_task(prefetch, pattern);
					}
				}
				for (reference, dependency) in &file.dependencies {
					if let Some(dependency) = dependency
						&& let Some(edge) = &dependency.item()
						&& !reference.is_solvable()
					{
						self.checkin_solve_prefetch_from_object_edge(prefetch, edge);
					}
				}
			},
			tg::graph::data::Node::Symlink(symlink) => {
				if let Some(edge) = &symlink.artifact {
					self.checkin_solve_prefetch_from_artifact_edge(prefetch, edge);
				}
			},
		}
	}

	fn checkin_solve_prefetch_from_artifact_edge(
		&self,
		prefetch: &Prefetch,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
	) {
		match edge {
			tg::graph::data::Edge::Object(id) => {
				self.checkin_solve_get_or_spawn_object_task(prefetch, &id.clone().into());
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				if let Some(graph_id) = &pointer.graph {
					self.checkin_solve_get_or_spawn_object_task(prefetch, &graph_id.clone().into());
				}
			},
		}
	}

	fn checkin_solve_prefetch_from_object_edge(
		&self,
		prefetch: &Prefetch,
		edge: &tg::graph::data::Edge<tg::object::Id>,
	) {
		match edge {
			tg::graph::data::Edge::Object(id) => {
				self.checkin_solve_get_or_spawn_object_task(prefetch, id);
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				if let Some(graph_id) = &pointer.graph {
					self.checkin_solve_get_or_spawn_object_task(prefetch, &graph_id.clone().into());
				}
			},
		}
	}

	fn checkin_solve_prefetch_from_directory(
		&self,
		prefetch: &Prefetch,
		directory: &tg::graph::data::Directory,
	) {
		match directory {
			tg::graph::data::Directory::Leaf(leaf) => {
				for edge in leaf.entries.values() {
					self.checkin_solve_prefetch_from_artifact_edge(prefetch, edge);
				}
			},
			tg::graph::data::Directory::Branch(branch) => {
				for child in &branch.children {
					self.checkin_solve_prefetch_from_directory_edge(prefetch, &child.directory);
				}
			},
		}
	}

	fn checkin_solve_prefetch_from_directory_edge(
		&self,
		prefetch: &Prefetch,
		edge: &tg::graph::data::Edge<tg::directory::Id>,
	) {
		match edge {
			tg::graph::data::Edge::Object(id) => {
				self.checkin_solve_get_or_spawn_object_task(prefetch, &id.clone().into());
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				if let Some(graph_id) = &pointer.graph {
					self.checkin_solve_get_or_spawn_object_task(prefetch, &graph_id.clone().into());
				}
			},
		}
	}

	pub(super) async fn checkin_solve_list_tags(
		&self,
		prefetch: &Prefetch,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<tg::tag::list::Output> {
		// Return a cached result if one is available.
		if let Some(output) = prefetch.tags.get(pattern).map(|value| value.clone()) {
			return Ok(output);
		}

		// List tags directly, bypassing the prefetch semaphore.
		let output = self.checkin_solve_fetch_tags(prefetch, pattern).await?;

		Ok(output)
	}

	fn checkin_solve_get_or_spawn_tag_task(
		&self,
		prefetch: &Prefetch,
		pattern: &tg::tag::Pattern,
	) -> tangram_futures::task::Shared<tg::Result<tg::tag::list::Output>, ()> {
		prefetch.tag_tasks.get_or_spawn(pattern.clone(), {
			let server = self.clone();
			let pattern = pattern.clone();
			let prefetch = prefetch.clone();
			move |_| async move {
				// Return an existing result if one is available.
				if let Some(output) = prefetch.tags.get(&pattern).map(|value| value.clone()) {
					return Ok(output);
				}

				// Acquire a permit to limit concurrent requests.
				let permit = prefetch.semaphore.acquire().await;

				// List the tags.
				let output = server.checkin_solve_fetch_tags(&prefetch, &pattern).await;

				// Drop the permit.
				drop(permit);

				output
			}
		})
	}

	async fn checkin_solve_fetch_tags(
		&self,
		prefetch: &Prefetch,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<tg::tag::list::Output> {
		// List tags.
		let output = if prefetch.arg.options.deterministic {
			tg::tag::list::Output { data: Vec::new() }
		} else {
			self.list_tags(tg::tag::list::Arg {
				cached: false,
				length: None,
				local: None,
				pattern: pattern.clone(),
				recursive: false,
				remotes: None,
				reverse: true,
				ttl: prefetch.arg.options.ttl,
			})
			.await
			.map_err(|source| tg::error!(!source, %pattern, "failed to list tags"))?
		};

		// Prefetch the first candidate's object.
		if let Some(output) = output.data.first()
			&& let Some(id) = output.item.as_ref().and_then(|item| item.as_ref().left())
		{
			self.checkin_solve_get_or_spawn_object_task(prefetch, id);
		}

		// Cache the result so prefetch tasks find a cache hit.
		prefetch.tags.insert(pattern.clone(), output.clone());

		Ok(output)
	}
}
