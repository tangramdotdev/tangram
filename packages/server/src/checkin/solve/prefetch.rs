use {
	crate::{Session, checkin::Graph},
	dashmap::{DashMap, DashSet},
	std::sync::Arc,
	tangram_client::prelude::*,
};

const PREFETCH_CONCURRENCY: usize = 16;

type Objects = Arc<DashMap<ObjectKey, Option<ObjectOutput>, fnv::FnvBuildHasher>>;

type ObjectData = Arc<DashMap<tg::object::Id, Arc<tg::object::Data>, tg::id::BuildHasher>>;

type ObjectTasks = tangram_futures::task::Map<
	ObjectKey,
	tg::Result<Option<ObjectOutput>>,
	(),
	fnv::FnvBuildHasher,
>;

type PrefetchedObjects = Arc<DashSet<tg::object::Id, tg::id::BuildHasher>>;

type Tags = Arc<DashMap<tg::specifier::Pattern, tg::list::Output, fnv::FnvBuildHasher>>;

type TagTasks = tangram_futures::task::Map<
	tg::specifier::Pattern,
	tg::Result<tg::list::Output>,
	(),
	fnv::FnvBuildHasher,
>;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(super) struct ObjectOptions {
	location: Option<tg::location::Arg>,
	tokens: tg::authorization::Tokens,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ObjectKey {
	id: tg::object::Id,
	options: ObjectOptions,
}

#[derive(Clone)]
pub(super) struct ObjectOutput {
	pub data: Arc<tg::object::Data>,
	pub output: tg::object::get::Output,
}

impl ObjectOptions {
	#[must_use]
	pub fn from_dependency(
		reference: &tg::Reference,
		dependency: &tg::graph::data::Dependency,
	) -> Self {
		let location = dependency
			.options
			.location
			.clone()
			.map(Into::into)
			.or_else(|| reference.options().location.clone());
		let mut tokens = dependency.options.tokens.clone();
		tokens.inherit(&reference.options().tokens);
		Self { location, tokens }
	}

	#[must_use]
	pub fn from_reference(reference: &tg::Reference) -> Self {
		Self {
			location: reference.options().location.clone(),
			tokens: reference.options().tokens.clone(),
		}
	}

	#[must_use]
	pub fn with_location_and_tokens(
		location: Option<tg::Location>,
		tokens: tg::authorization::Tokens,
	) -> Self {
		let location = location.map(Into::into);
		Self { location, tokens }
	}

	#[must_use]
	pub fn from_referent_options(options: &tg::referent::Options) -> Self {
		Self {
			location: options.location.clone().map(Into::into),
			tokens: options.tokens.clone(),
		}
	}

	pub fn inherit(&mut self, parent: &Self) {
		self.tokens.inherit(&parent.tokens);
		if self.location.is_none() {
			self.location.clone_from(&parent.location);
		}
	}

	pub fn update_from_output(&mut self, output: &tg::object::get::Output) {
		if !output.tokens.is_empty() {
			self.tokens.clone_from(&output.tokens);
		}
	}
}

#[derive(Clone)]
pub struct Prefetch {
	arg: tg::checkin::Arg,
	data: ObjectData,
	object_tasks: ObjectTasks,
	objects: Objects,
	prefetched_objects: PrefetchedObjects,
	semaphore: Arc<tokio::sync::Semaphore>,
	tag_tasks: TagTasks,
	tags: Tags,
}

impl Prefetch {
	pub fn new(arg: tg::checkin::Arg) -> Self {
		let data = Arc::new(DashMap::default());
		let object_tasks = tangram_futures::task::Map::default();
		let objects = Arc::new(DashMap::default());
		let prefetched_objects = Arc::new(DashSet::default());
		let semaphore = Arc::new(tokio::sync::Semaphore::new(PREFETCH_CONCURRENCY));
		let tag_tasks = tangram_futures::task::Map::default();
		let tags = Arc::new(DashMap::default());
		Self {
			arg,
			data,
			object_tasks,
			objects,
			prefetched_objects,
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

impl Session {
	pub(super) fn checkin_solve_prefetch_from_lock(
		&self,
		prefetch: &Prefetch,
		graph: &Graph,
		lock: &tg::graph::Data,
		next: usize,
	) {
		let options = ObjectOptions::default();
		for node in graph
			.nodes
			.range(next..)
			.filter_map(|(_, node)| node.lock_index.and_then(|index| lock.nodes.get(index)))
		{
			self.checkin_solve_prefetch_from_graph_node(prefetch, node, &options);
			if let tg::graph::data::Node::File(file) = node {
				for (reference, dependency) in &file.dependencies {
					let Some(dependency) = dependency else {
						continue;
					};
					if !reference.is_solvable() {
						continue;
					}
					if let Some(id) = dependency.id() {
						let options = ObjectOptions::from_dependency(reference, dependency);
						self.checkin_solve_get_or_spawn_object_task(prefetch, id, options);
					}
				}
			}
		}
	}

	pub(super) async fn checkin_solve_get_object_with_options(
		&self,
		prefetch: &Prefetch,
		graph: &mut Graph,
		next: usize,
		id: &tg::object::Id,
		options: &mut ObjectOptions,
	) -> tg::Result<ObjectOutput> {
		self.checkin_solve_try_get_object_with_options(prefetch, graph, next, id, options)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to get the object"))
	}

	pub(super) async fn checkin_solve_try_get_object_with_options(
		&self,
		prefetch: &Prefetch,
		graph: &mut Graph,
		next: usize,
		id: &tg::object::Id,
		options: &mut ObjectOptions,
	) -> tg::Result<Option<ObjectOutput>> {
		let key = ObjectKey {
			id: id.clone(),
			options: options.clone(),
		};

		// Return a cached result if one is available.
		if let Some(output) = prefetch.objects.get(&key).map(|value| value.clone()) {
			if let Some(output) = &output {
				self.checkin_solve_record_object_observation(graph, next, id, output);
				options.update_from_output(&output.output);
			}
			return Ok(output);
		}

		// Fetch the object directly, bypassing the prefetch semaphore.
		let output = self.checkin_solve_fetch_object(prefetch, &key).await?;
		if let Some(output) = &output {
			self.checkin_solve_record_object_observation(graph, next, id, output);
			options.update_from_output(&output.output);
		}

		Ok(output)
	}

	fn checkin_solve_record_object_observation(
		&self,
		graph: &mut Graph,
		next: usize,
		id: &tg::object::Id,
		output: &ObjectOutput,
	) {
		if let Some(token) = output.output.tokens.local() {
			self.checkin_merge_object_token(graph, next, id, token);
		}
		Self::checkin_record_object_data(graph, next, id, &output.data);
	}

	fn checkin_solve_get_or_spawn_object_task(
		&self,
		prefetch: &Prefetch,
		id: &tg::object::Id,
		options: ObjectOptions,
	) -> tangram_futures::task::Shared<tg::Result<Option<ObjectOutput>>, ()> {
		let key = ObjectKey {
			id: id.clone(),
			options,
		};
		prefetch.object_tasks.get_or_spawn(key.clone(), {
			let session = self.clone();
			let prefetch = prefetch.clone();
			move |_| async move {
				// Return an existing result if one is available.
				if let Some(output) = prefetch.objects.get(&key).map(|value| value.clone()) {
					return Ok(output);
				}

				// Acquire a permit to limit concurrent requests.
				let permit = prefetch.semaphore.acquire().await;

				// Get the object.
				let output = session.checkin_solve_fetch_object(&prefetch, &key).await;

				// Drop the permit.
				drop(permit);

				output
			}
		})
	}

	async fn checkin_solve_fetch_object(
		&self,
		prefetch: &Prefetch,
		key: &ObjectKey,
	) -> tg::Result<Option<ObjectOutput>> {
		// Get the object.
		let arg = tg::object::get::Arg {
			location: key.options.location.clone(),
			metadata: true,
			tokens: key.options.tokens.clone(),
			..Default::default()
		};
		let output = self
			.try_get_object(&key.id, arg)
			.await
			.map_err(|error| tg::error!(!error, id = %key.id, "failed to get the object"))?;
		let Some(output) = output else {
			prefetch.objects.insert(key.clone(), None);
			return Ok(None);
		};
		let data = if let Some(data) = prefetch.data.get(&key.id).map(|data| data.clone()) {
			data
		} else {
			let data = tg::object::Data::deserialize(key.id.kind(), output.bytes.clone())
				.map(Arc::new)
				.map_err(|error| tg::error!(!error, "failed to deserialize the object"))?;
			prefetch.data.entry(key.id.clone()).or_insert(data).clone()
		};

		// If the object requires solving, then prefetch its descendant objects and tags.
		let requires_solving =
			Self::checkin_solve_metadata_requires_solving(output.metadata.as_ref());
		if requires_solving && prefetch.prefetched_objects.insert(key.id.clone()) {
			let mut options = key.options.clone();
			options.update_from_output(&output);
			match data.as_ref() {
				tg::object::Data::Directory(tg::directory::Data::Pointer(pointer))
				| tg::object::Data::File(tg::file::Data::Pointer(pointer))
				| tg::object::Data::Symlink(tg::symlink::Data::Pointer(pointer)) => {
					if let Some(graph_id) = &pointer.graph {
						self.checkin_solve_get_or_spawn_object_task(
							prefetch,
							&graph_id.clone().into(),
							options.clone(),
						);
					}
				},

				tg::object::Data::Directory(tg::directory::Data::Node(directory)) => {
					let node = tg::graph::data::Node::Directory(directory.clone());
					self.checkin_solve_prefetch_from_graph_node(prefetch, &node, &options);
				},
				tg::object::Data::File(tg::file::Data::Node(file)) => {
					let node = tg::graph::data::Node::File(file.clone());
					self.checkin_solve_prefetch_from_graph_node(prefetch, &node, &options);
				},
				tg::object::Data::Symlink(tg::symlink::Data::Node(symlink)) => {
					let node = tg::graph::data::Node::Symlink(symlink.clone());
					self.checkin_solve_prefetch_from_graph_node(prefetch, &node, &options);
				},

				_ => {},
			}
		}

		// Cache the result so prefetch tasks find a cache hit.
		let output = ObjectOutput { data, output };
		prefetch.objects.insert(key.clone(), Some(output.clone()));

		Ok(Some(output))
	}

	pub(super) fn checkin_solve_prefetch_from_graph_node(
		&self,
		prefetch: &Prefetch,
		node: &tg::graph::data::Node,
		parent_options: &ObjectOptions,
	) {
		match node {
			tg::graph::data::Node::Directory(directory) => {
				self.checkin_solve_prefetch_from_directory(prefetch, directory, parent_options);
			},
			tg::graph::data::Node::File(file) => {
				for reference in file.dependencies.keys() {
					if let tg::reference::Node::Specifier(pattern) = reference.node() {
						self.checkin_solve_get_or_spawn_tag_task(prefetch, pattern);
					}
				}
				for (reference, dependency) in &file.dependencies {
					if let Some(dependency) = dependency
						&& let Some(edge) = &dependency.node()
						&& !reference.is_solvable()
					{
						let mut options = ObjectOptions::from_dependency(reference, dependency);
						options.inherit(parent_options);
						self.checkin_solve_prefetch_from_object_edge(prefetch, edge, options);
					}
				}
			},
			tg::graph::data::Node::Symlink(symlink) => {
				if let Some(edge) = &symlink.artifact {
					self.checkin_solve_prefetch_from_artifact_edge(
						prefetch,
						edge,
						parent_options.clone(),
					);
				}
			},
		}
	}

	fn checkin_solve_prefetch_from_artifact_edge(
		&self,
		prefetch: &Prefetch,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
		options: ObjectOptions,
	) {
		match edge {
			tg::graph::data::Edge::Object(id) => {
				self.checkin_solve_get_or_spawn_object_task(prefetch, &id.clone().into(), options);
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				if let Some(graph_id) = &pointer.graph {
					self.checkin_solve_get_or_spawn_object_task(
						prefetch,
						&graph_id.clone().into(),
						options,
					);
				}
			},
		}
	}

	fn checkin_solve_prefetch_from_object_edge(
		&self,
		prefetch: &Prefetch,
		edge: &tg::graph::data::Edge<tg::object::Id>,
		options: ObjectOptions,
	) {
		match edge {
			tg::graph::data::Edge::Object(id) => {
				self.checkin_solve_get_or_spawn_object_task(prefetch, id, options);
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				if let Some(graph_id) = &pointer.graph {
					self.checkin_solve_get_or_spawn_object_task(
						prefetch,
						&graph_id.clone().into(),
						options,
					);
				}
			},
		}
	}

	fn checkin_solve_prefetch_from_directory(
		&self,
		prefetch: &Prefetch,
		directory: &tg::graph::data::Directory,
		options: &ObjectOptions,
	) {
		match directory {
			tg::graph::data::Directory::Leaf(leaf) => {
				for edge in leaf.entries.values() {
					self.checkin_solve_prefetch_from_artifact_edge(prefetch, edge, options.clone());
				}
			},
			tg::graph::data::Directory::Branch(branch) => {
				for child in &branch.children {
					self.checkin_solve_prefetch_from_directory_edge(
						prefetch,
						&child.directory,
						options.clone(),
					);
				}
			},
		}
	}

	fn checkin_solve_prefetch_from_directory_edge(
		&self,
		prefetch: &Prefetch,
		edge: &tg::graph::data::Edge<tg::directory::Id>,
		options: ObjectOptions,
	) {
		match edge {
			tg::graph::data::Edge::Object(id) => {
				self.checkin_solve_get_or_spawn_object_task(prefetch, &id.clone().into(), options);
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				if let Some(graph_id) = &pointer.graph {
					self.checkin_solve_get_or_spawn_object_task(
						prefetch,
						&graph_id.clone().into(),
						options,
					);
				}
			},
		}
	}

	pub(super) async fn checkin_solve_list_tag_entries(
		&self,
		prefetch: &Prefetch,
		pattern: &tg::specifier::Pattern,
	) -> tg::Result<tg::list::Output> {
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
		pattern: &tg::specifier::Pattern,
	) -> tangram_futures::task::Shared<tg::Result<tg::list::Output>, ()> {
		prefetch.tag_tasks.get_or_spawn(pattern.clone(), {
			let session = self.clone();
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
				let output = session.checkin_solve_fetch_tags(&prefetch, &pattern).await;

				// Drop the permit.
				drop(permit);

				output
			}
		})
	}

	async fn checkin_solve_fetch_tags(
		&self,
		prefetch: &Prefetch,
		pattern: &tg::specifier::Pattern,
	) -> tg::Result<tg::list::Output> {
		// List tags.
		let output = if prefetch.arg.options.deterministic {
			tg::list::Output {
				cursor: None,
				data: Vec::new(),
			}
		} else {
			self.match_tags_for_get(pattern, None, false, None, prefetch.arg.options.tag_ttl)
				.await
				.map_err(|error| tg::error!(!error, %pattern, "failed to list entries"))?
		};

		// Prefetch the first candidate's object.
		if let Some(target) = output.data.first().and_then(tg::list::Entry::target)
			&& let Some(id) = target.node.as_ref().left()
		{
			let options = ObjectOptions::from_referent_options(&target.options);
			self.checkin_solve_get_or_spawn_object_task(prefetch, id, options);
		}

		// Cache the result so prefetch tasks find a cache hit.
		prefetch.tags.insert(pattern.clone(), output.clone());

		Ok(output)
	}
}
