use crate::Server;
use either::Either;
use futures::{
	stream::{self, FuturesUnordered},
	TryStreamExt,
};
use indoc::formatdoc;
use std::{
	collections::{BTreeMap, BTreeSet},
	os::unix::fs::PermissionsExt,
	path::PathBuf,
	sync::RwLock,
};
use tangram_client as tg;
use tangram_database as db;
use tangram_database::{Connection, Database, Query, Transaction};
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug)]
struct State {
	// The current graph.
	graph: Graph,

	// The edge that we are currently following.
	edge: Edge,

	// A work queue of edges we will have to follow, in depth-first order.
	queue: im::Vector<Edge>,

	// A lazily-initialized set of packages to try.
	objects: Option<im::Vector<(tg::Tag, tg::Object)>>,
}

// A graph of packages.
#[derive(Clone, Default, Debug)]
pub(super) struct Graph {
	// A counter used to create IDs for nodes that don't have a repository ID.
	counter: usize,

	// The set of nodes in the graph.
	pub nodes: im::HashMap<Id, Node>,

	// The set of paths in the graph.
	pub paths: im::HashMap<tg::Path, Id>,
}

// A node within the package graph.
#[derive(Clone, Debug)]
#[allow(clippy::struct_field_names)]
pub struct Node {
	// A unique identifier of the node within the package graph.
	pub id: Id,

	// The result of this node (None if we do not know if it is successful or not).
	pub errors: Vec<tg::Error>,

	// Direct dependencies.
	pub outgoing: BTreeMap<tg::Reference, Id>,

	// Whether to inline the object in the output.
	// TODO: inline objects
	pub _inline_object: bool,

	// The underlying object.
	pub object: Option<tg::object::Id>,

	// The tag of this node, if it exists.
	pub tag: Option<tg::Tag>,
}

pub(super) type Id = Either<tg::Reference, usize>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct Edge {
	// The source node of the edge (referrer)
	pub src: Id,

	// The destination of the edge (dependency)
	pub dst: Id,

	// The constraint the referrer places on the dependency.
	pub reference: tg::Reference,
}

#[derive(Clone, Debug)]
struct Input {
	arg: tg::artifact::checkin::Arg,
	metadata: std::fs::Metadata,
	write_lock: bool,
	lock_data: Option<(tg::Lock, usize)>,
	dependencies: BTreeMap<tg::Reference, Option<Self>>,
	is_root: bool,
}

#[derive(Clone, Debug)]
struct Output {
	data: tg::artifact::Data,
	lock_index: usize,
	dependencies: BTreeMap<tg::Reference, Self>,
}

// Artifact checkin
impl Server {
	pub async fn check_in_or_store_artifact_inner(
		&self,
		arg: tg::artifact::checkin::Arg,
		store_as: Option<&tg::artifact::Id>,
	) -> tg::Result<tg::artifact::Id> {
		// Collect the input.
		let input = self.collect_input(arg.clone()).await.map_err(
			|source| tg::error!(!source, %path = arg.path, "failed to collect check-in input"),
		)?;

		// Construct the graph.
		let (mut graph, root) = self
			.create_graph(&input)
			.await
			.map_err(|source| tg::error!(!source, "failed to construct object graph"))?;

		// Unify.
		if !arg.deterministic {
			graph = self
				.unify_dependencies(graph, &root)
				.await
				.map_err(|source| tg::error!(!source, "failed to unify object graph"))?;
		}

		// Validate
		graph.validate(self)?;

		// Create the lock that is written to disk.
		let lock = self.create_lock(&graph, &root).await?;

		// Get the output.
		let (output, lock_with_objects) = self.collect_output(&input, &lock).await?;

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
		let (locks, new_files) = self.split_locks(&lock_with_objects, &old_files).await?;

		// Replace files.
		let mut output = output.clone();
		let mut stack = vec![&mut output];
		while let Some(output) = stack.pop() {
			if let Some(data) = new_files.get(&output.lock_index) {
				output.data = data.clone().into();
			}
			stack.extend(output.dependencies.values_mut());
		}

		// Write the lockfile if necessary.
		if input.write_lock {
			self.write_lockfile(&input.arg.path, &lock).await?;
		}

		// Get the root object. TODO this is garbage, don't do it.
		let (lock, node) = locks
			.get(&0usize)
			.ok_or_else(|| tg::error!("corupted locks"))?;
		let object = tg::Lock::with_id(lock.clone()).load(self).await?.nodes[*node]
			.data(self)
			.await?
			.object
			.ok_or_else(|| tg::error!("expected an object"))?;
		let id = match object {
			tg::object::Id::Directory(id) => id.into(),
			tg::object::Id::File(id) => id.into(),
			tg::object::Id::Symlink(id) => id.into(),
			_ => return Err(tg::error!("expected an artifact")),
		};

		if let Some(store_as) = store_as {
			// Store if requested.
			if store_as != &id {
				return Err(tg::error!("checkouts directory is corrupted"));
			}
			self.write_output_to_database(&output).await?;
		} else {
			// Otherwise, update hardlinks and xattrs.
			self.write_hardlinks_and_xattrs(&input, &output).await?;
		}

		Ok(id)
	}

	async fn try_read_lockfile(&self, path: &tg::Path) -> tg::Result<Option<tg::Lock>> {
		if !tg::artifact::module::is_root_module_path(path.as_ref()) {
			return Ok(None);
		}
		let path = path
			.clone()
			.parent()
			.join(tg::artifact::module::LOCKFILE_FILE_NAME)
			.normalize();
		match tokio::fs::read_to_string(&path).await {
			Ok(contents) => {
				let data = serde_json::from_str::<tg::lock::Data>(&contents).map_err(
					|source| tg::error!(!source, %path, "failed to deserialize lockfile"),
				)?;
				let object: tg::lock::Object = data.try_into()?;
				Ok(Some(tg::Lock::with_object(object)))
			},
			Err(error) if error.raw_os_error() == Some(libc::EEXIST) => Ok(None),
			Err(source) => Err(tg::error!(!source, %path, "failed to read lockfile contents")),
		}
	}

	async fn write_lockfile(&self, path: &tg::Path, lock: &tg::lock::Data) -> tg::Result<()> {
		let path = path
			.clone()
			.parent()
			.join(tg::artifact::module::LOCKFILE_FILE_NAME)
			.normalize();
		let contents = serde_json::to_vec_pretty(lock)
			.map_err(|source| tg::error!(!source, "failed to serialize lock"))?;
		tokio::fs::File::options()
			.create(true)
			.write(true)
			.truncate(true)
			.append(false)
			.open(&path)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to open file"))?
			.write_all(&contents)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to write lockfile"))?;
		Ok(())
	}

	async fn write_output_to_database(&self, output: &Output) -> tg::Result<()> {
		let mut connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to create a transaction"))?;

		let mut stack = vec![output];
		while let Some(output) = stack.pop() {
			// TODO: count/weight.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into objects (id, bytes, complete, touched_at)
					values ({p}1, {p}2, {p}3, {p}4)
					on conflict (id) do update set touched_at = {p}4;
				"
			);
			let id = output.data.id()?;
			let bytes = output.data.serialize()?;
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
			let params = db::params![id, bytes, 1, now];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to put the artifact into the database")
				})?;

			stack.extend(output.dependencies.values());
		}

		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		Ok(())
	}
}

// Input
impl Server {
	// Recursively scan the file system for input.
	async fn collect_input(&self, arg: tg::artifact::checkin::Arg) -> tg::Result<Input> {
		let visited = RwLock::new(BTreeMap::new());
		let roots = RwLock::new(vec![arg.path.clone()]);
		self.collect_input_inner(arg.clone(), &visited, None, &roots)
			.await?;
		let root = visited.read().unwrap().get(&arg.path).unwrap().clone();
		Ok(root)
	}

	async fn collect_input_inner(
		&self,
		arg: tg::artifact::checkin::Arg,
		visited: &RwLock<BTreeMap<tg::Path, Input>>,
		parent: Option<(tg::Lock, usize, tg::Path)>,
		roots: &RwLock<Vec<tg::Path>>,
	) -> tg::Result<Input> {
		{
			let visited = visited.read().unwrap();
			if let Some(input) = visited.get(&arg.path) {
				return Ok(input.clone());
			};
		}

		// Get the file system metadata.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let metadata = tokio::fs::symlink_metadata(&arg.path).await.map_err(
			|source| tg::error!(!source, %path = arg.path, "failed to get file metadata"),
		)?;

		// Get or create a lock if necessary.
		let (lock_data, write_lock) = if !arg.dependencies {
			(None, false)
		} else if let Some(lock) = self.try_read_lockfile(&arg.path).await? {
			(Some((lock, 0)), true)
		} else if let Some((lock, node, parent)) = parent {
			let object = lock.object(self).await?;

			// Check if this path is in the lock already.
			let either = object.nodes[node]
				.dependencies
				.iter()
				.flat_map(|map| {
					map.iter().filter_map(|(reference, either)| {
						let reference = reference
							.path()
							.try_unwrap_path_ref()
							.ok()
							.or_else(|| reference.query()?.path.as_ref())?;
						Some((reference, either))
					})
				})
				.find_map(|(child, either)| {
					(&parent.clone().join(child.clone()).normalize() == &arg.path).then_some(either)
				});

			if let Some(either) = either {
				match either {
					Either::Left(node) => (Some((lock, *node)), false),
					Either::Right(tg::Object::File(file)) => {
						let Some(tg::file::Dependencies::Lock(lock, node)) =
							&*file.dependencies(self).await?
						else {
							todo!()
						};
						(Some((lock.clone(), *node)), false)
					},
					Either::Right(_) => todo!(), // TODO: this should be unreachable.
				}
			} else if arg.locked {
				return Err(tg::error!("lock is out of date"));
			} else {
				(None, false)
			}
		} else {
			(None, false)
		};

		// Get the children and dependencies.
		let dependencies = if metadata.is_dir() {
			let read_dir = tokio::fs::read_dir(&arg.path)
				.await
				.map_err(|source| tg::error!(!source, "failed to read directory"))?;
			stream::try_unfold(read_dir, |mut read_dir| async {
				let Some(next) = read_dir
					.next_entry()
					.await
					.map_err(|source| tg::error!(!source, "failed to get next directory entry"))?
				else {
					return Ok(None);
				};
				let name = next.file_name();
				let name = name
					.to_str()
					.ok_or_else(|| tg::error!("non utf8-path name"))?;
				let path = name.parse()?;
				let reference = tg::Reference::with_path(&path);
				Ok::<_, tg::Error>(Some((reference, read_dir)))
			})
			.try_collect::<BTreeSet<_>>()
			.await?
		} else if tg::artifact::module::is_module_path(arg.path.as_ref()) && arg.dependencies {
			let text = tokio::fs::read_to_string(&arg.path)
				.await
				.map_err(|source| tg::error!(!source, %path = arg.path, "failed to read module"))?;
			let analysis = crate::compiler::Compiler::analyze_module(text).map_err(
				|source| tg::error!(!source, %path = arg.path, "failed to analyze module"),
			)?;
			analysis
				.imports
				.into_iter()
				.map(|import| import.reference)
				.collect::<BTreeSet<_>>()
		} else {
			BTreeSet::new()
		};

		drop(permit);

		// Detect if this is a root or not.
		let is_root = roots.read().unwrap().iter().all(|root| {
			let first_component = root
				.diff(&arg.path)
				.and_then(|p| p.components().first().cloned());
			!matches!(first_component, Some(tg::path::Component::Parent))
		});
		if is_root {
			roots.write().unwrap().push(arg.path.clone());
		}

		// Create the input.
		let input = Input {
			arg: arg.clone(),
			metadata: metadata.clone(),
			write_lock,
			lock_data: lock_data.clone(),
			dependencies: BTreeMap::new(),
			is_root,
		};
		visited.write().unwrap().insert(arg.path.clone(), input);

		// Get the children.
		let dependencies = dependencies
			.into_iter()
			.map(|reference| async {
				let Some(path) = reference
					.path()
					.try_unwrap_path_ref()
					.ok()
					.or_else(|| reference.query()?.path.as_ref())
				else {
					return Ok((reference, None));
				};
				let parent = lock_data
					.clone()
					.map(|(lock, node)| (lock, node, arg.path.clone()));
				let path = if metadata.is_dir() {
					arg.path.clone().join(path.clone()).normalize()
				} else {
					arg.path.clone().parent().join(path.clone()).normalize()
				};
				let arg = tg::artifact::checkin::Arg {
					path,
					..arg.clone()
				};
				let input = Box::pin(self.collect_input_inner(arg, visited, parent, roots)).await?;
				Ok::<_, tg::Error>((reference, Some(input)))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Update this node's children.
		visited
			.write()
			.unwrap()
			.get_mut(&arg.path)
			.unwrap()
			.dependencies = dependencies;

		// Return this node's input.
		Ok(visited.read().unwrap().get(&arg.path).unwrap().clone())
	}
}

// Graph creation
impl Server {
	async fn create_graph(&self, input: &Input) -> tg::Result<(Graph, Id)> {
		let mut graph = Graph::default();
		let mut visited_lock_nodes = BTreeMap::new();

		let root = self
			.create_graph_inner(input, &mut graph, &mut visited_lock_nodes)
			.await?;
		Ok((graph, root))
	}

	async fn create_graph_inner(
		&self,
		input: &Input,
		graph: &mut Graph,
		visited_lock_nodes: &mut BTreeMap<(tg::lock::Id, usize), Id>,
	) -> tg::Result<Id> {
		if let Some(id) = graph.paths.get(&input.arg.path).cloned() {
			return Ok(id);
		}
		let id = Either::Right(graph.counter);
		graph.counter += 1;
		graph.paths.insert(input.arg.path.clone(), id.clone());

		// Get the outgoing edges.
		let mut outgoing = BTreeMap::new();

		// Add dependencies.
		for (dependency, child) in &input.dependencies {
			// Recurse on existing input.
			if let Some(input) = child {
				let id =
					Box::pin(self.create_graph_inner(input, graph, visited_lock_nodes)).await?;
				outgoing.insert(dependency.clone(), id);
				continue;
			}

			// Check if there is a solution in the lock.
			if let Some((lock, root)) = &input.lock_data {
				let object = lock.object(self).await?;
				let lock_node = &object.nodes[*root];
				match lock_node
					.dependencies
					.as_ref()
					.and_then(|map| map.get(dependency))
				{
					Some(Either::Left(node)) => {
						let id = self
							.create_graph_node_from_lock_node(
								graph,
								lock,
								*node,
								visited_lock_nodes,
							)
							.await?;
						outgoing.insert(dependency.clone(), id);
						continue;
					},
					Some(Either::Right(tg::Object::File(file))) => {
						let Some(tg::file::Dependencies::Lock(lock, root)) =
							&*file.dependencies(self).await?
						else {
							todo!()
						};
						let id = self
							.create_graph_node_from_lock_node(
								graph,
								&lock,
								*root,
								visited_lock_nodes,
							)
							.await?;
						outgoing.insert(dependency.clone(), id);
						continue;
					},
					Some(Either::Right(object)) => {
						let id = object.id(self).await?;
						let id = self.create_graph_node_from_object(graph, id).await?;
						outgoing.insert(dependency.clone(), id);
						continue;
					},
					None if input.arg.deterministic || input.arg.locked => {
						return Err(tg::error!("lock is out of date"))
					},
					None => (),
				}
			}

			// Otherwise, create partial nodes.
			match dependency.path() {
				tg::reference::Path::Build(_) => {
					return Err(tg::error!(%dependency, "invalid reference"))
				},
				tg::reference::Path::Object(object) => {
					let id = self
						.create_graph_node_from_object(graph, object.clone())
						.await?;
					outgoing.insert(dependency.clone(), id);
				},
				tg::reference::Path::Tag(pattern) => {
					let id = get_reference_from_pattern(pattern);
					outgoing.insert(dependency.clone(), Either::Left(id));
				},
				tg::reference::Path::Path(_) => return Err(tg::error!("unimplemented")),
			}
		}

		// Create the node.
		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing,
			_inline_object: false,
			object: None,
			tag: None,
		};

		graph.nodes.insert(id.clone(), node);

		Ok(id)
	}

	async fn create_graph_node_from_lock_node(
		&self,
		graph: &mut Graph,
		lock: &tg::Lock,
		node: usize,
		visited: &mut BTreeMap<(tg::lock::Id, usize), Id>,
	) -> tg::Result<Id> {
		let key = (lock.id(self).await?, node);
		if let Some(id) = visited.get(&key) {
			return Ok(id.clone());
		}
		let id = Either::Right(graph.counter);
		graph.counter += 1;
		visited.insert(key, id.clone());

		let object = lock.object(self).await?;
		let lock_node = &object.nodes[node];

		let mut outgoing = BTreeMap::new();
		for (dependency, either) in lock_node.dependencies.iter().flatten() {
			match either {
				Either::Left(node) => {
					let id = Box::pin(
						self.create_graph_node_from_lock_node(graph, lock, *node, visited),
					)
					.await?;
					outgoing.insert(dependency.clone(), id);
				},
				Either::Right(tg::Object::File(file)) => {
					let Some(tg::file::Dependencies::Lock(lock, root)) =
						&*file.dependencies(self).await?
					else {
						todo!()
					};
					let id = Box::pin(
						self.create_graph_node_from_lock_node(graph, &lock, *root, visited),
					)
					.await?;
					outgoing.insert(dependency.clone(), id);
				},
				Either::Right(object) => {
					let id = object.id(self).await?;
					let id = self.create_graph_node_from_object(graph, id).await?;
					outgoing.insert(dependency.clone(), id);
				},
			}
		}

		// Get the underlying object.
		let object = lock_node
			.object
			.as_ref()
			.ok_or_else(|| tg::error!("missing object"))?
			.id(self)
			.await?;

		// Create thenode.
		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing,
			_inline_object: false,
			object: Some(object),
			tag: None,
		};
		graph.nodes.insert(id.clone(), node);
		Ok(id)
	}

	#[allow(clippy::unused_async)]
	async fn create_graph_node_from_object(
		&self,
		graph: &mut Graph,
		object: tg::object::Id,
	) -> tg::Result<Id> {
		// Get an ID.
		let id = Either::Right(graph.counter);
		graph.counter += 1;

		// Create a node.
		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing: BTreeMap::new(),
			_inline_object: true,
			object: Some(object),
			tag: None,
		};
		graph.nodes.insert(id.clone(), node);
		Ok(id)
	}
}

// Unification
impl Server {
	async fn unify_dependencies(&self, mut graph: Graph, root: &Id) -> tg::Result<Graph> {
		// Get the overrides.
		let mut overrides: BTreeMap<Id, BTreeMap<String, tg::Reference>> = BTreeMap::new();
		let root_node = graph.nodes.get_mut(root).unwrap();
		for (reference, node) in &root_node.outgoing {
			let Some(overrides_) = reference
				.query()
				.as_ref()
				.and_then(|query| query.overrides.clone())
			else {
				continue;
			};
			overrides
				.entry(node.clone())
				.or_default()
				.extend(overrides_);
		}

		let mut queue = graph
			.outgoing(root.clone())
			.filter(|edge| !graph.nodes.contains_key(&edge.dst))
			.collect::<im::Vector<_>>();

		// Get the first edge to solve.
		let Some(edge) = queue.pop_back() else {
			return Ok(graph);
		};

		// Construct the initial state.
		let packages = None;
		let mut current = State {
			graph,
			edge,
			queue,
			objects: packages,
		};

		// Create a vec of checkpoints to support backtracking.
		let mut checkpoints = Vec::new();

		// Walk the graph until we have no more edges to solve.
		loop {
			self.walk_edge(&mut checkpoints, &mut current, &overrides)
				.await;

			// Changing this to pop_back() would convert the algorithm from breadth-first to depth-first. The algorithm should be correct regardless of traversel order. However, using BFS to walk the graph allows us to propogate constraints when backtracking to shrink the search sapce.
			let Some(next) = current.queue.pop_front() else {
				break;
			};

			current.edge = next;
			current.objects.take();
		}

		Ok(current.graph)
	}

	async fn walk_edge(
		&self,
		state: &mut Vec<State>,
		current: &mut State,
		overrides: &BTreeMap<Id, BTreeMap<String, tg::Reference>>,
	) {
		// Check if an override exists.
		let reference = overrides
			.get(&current.edge.src)
			.and_then(|overrides| {
				let name = current.edge.reference.query().as_ref()?.name.as_ref()?;
				overrides.get(name)
			})
			.unwrap_or(&current.edge.reference)
			.clone();

		// If the graph doesn't contain the destination node, attempt to select a version.
		if !current.graph.nodes.contains_key(&current.edge.dst) {
			match self
				.resolve_dependency(&mut current.graph, &reference, &mut current.objects)
				.await
			{
				Ok(node) => {
					// Save the current state that we will return to later.
					state.push(current.clone());

					// Add this edge to the top of the stack.
					current.queue.push_back(current.edge.clone());

					// Add the direct dependencies to the stack and increment their counters.
					let edges = std::iter::once(current.edge.clone())
						.chain(current.graph.outgoing(node.clone()))
						.collect::<Vec<_>>();
					current.queue.extend(edges);
				},
				Err(error) => {
					current.graph.add_error(&current.edge.src, error);
				},
			};
			return;
		}

		// Check if the destination node contains errors.
		let contains_errors = !current
			.graph
			.nodes
			.get(&current.edge.dst)
			.unwrap()
			.errors
			.is_empty();

		// If the destination is an error, add an error to the referrer.
		if contains_errors {
			let error =
				tg::error!("could not solve {reference} because the dependency contains errors");
			current.graph.add_error(&current.edge.src, error);
		}

		// If there is no tag it is not a tag dependency, so return.
		if current.edge.dst.is_right() {
			return;
		}

		// Get the tag of the current node.
		let tag = current
			.graph
			.nodes
			.get(&current.edge.dst)
			.unwrap()
			.tag
			.as_ref()
			.unwrap();

		// Validate the constraint.
		match reference
			.path()
			.try_unwrap_tag_ref()
			.map(|pat| pat.matches(tag))
		{
			// If the version matches do nothing.
			Ok(true) | Err(_) => (),

			// If the version does not match, attempt to backtrack, but only if the destination contains no errors.
			Ok(false) => match try_backtrack(state, &current.edge) {
				Some(old) if !contains_errors => {
					*current = old;
				},
				_ => {
					let error = tg::error!(%reference, "package version conflict");
					current.graph.add_error(&current.edge.src, error);
				},
			},
		}
	}

	async fn resolve_dependency(
		&self,
		graph: &mut Graph,
		reference: &tg::Reference,
		objects: &mut Option<im::Vector<(tg::Tag, tg::Object)>>,
	) -> tg::Result<Id> {
		// Seed the remaining packages if necessary.
		if objects.is_none() {
			// Get the tag pattern and remote if necessary.
			let pattern = reference
				.path()
				.try_unwrap_tag_ref()
				.map_err(|_| tg::error!(%reference, "expected a tag pattern"))?
				.clone();
			let remote = reference
				.query()
				.as_ref()
				.and_then(|query| query.remote.clone());

			// List tags that match the pattern.
			let objects_: im::Vector<_> = self
				.list_tags(tg::tag::list::Arg {
					length: None,
					pattern: pattern.clone(),
					remote,
				})
				.await
				.map_err(|source| tg::error!(!source, %pattern, "failed to get tags"))?
				.data
				.into_iter()
				.filter_map(|output| {
					let object = output.item?.right()?;
					Some((output.tag, tg::Object::with_id(object)))
				})
				.collect();

			// Update the remaining packages.
			objects.replace(objects_);
		}

		// Pop the next version off the list.
		let (tag, object) = objects
			.as_mut()
			.unwrap()
			.pop_back()
			.ok_or_else(|| tg::error!(%reference, "no solution exists"))?;

		self.create_graph_node_from_tagged_object(graph, &object, tag)
			.await
	}

	async fn create_graph_node_from_tagged_object(
		&self,
		graph: &mut Graph,
		object: &tg::Object,
		tag: tg::Tag,
	) -> tg::Result<Id> {
		let mut visited = BTreeMap::new();
		self.create_graph_node_from_tagged_object_inner(graph, object, Some(tag), &mut visited)
			.await
	}

	async fn create_graph_node_from_tagged_object_inner(
		&self,
		graph: &mut Graph,
		object: &tg::Object,
		tag: Option<tg::Tag>,
		visited: &mut BTreeMap<tg::object::Id, Id>,
	) -> tg::Result<Id> {
		let object_id = object.id(self).await?;
		if let Some(id) = visited.get(&object_id) {
			return Ok(id.clone());
		}

		let id = tag.as_ref().map(|_tag| todo!()).unwrap_or_else(|| {
			let id = Either::Right(graph.counter);
			graph.counter += 1;
			id
		});

		visited.insert(object_id.clone(), id.clone());

		// If this is a file, get the outgoing edges.
		let mut outgoing = BTreeMap::new();
		if let tg::Object::File(file) = object {
			match &*file.dependencies(self).await? {
				Some(tg::file::Dependencies::Set(set)) => {
					for object in set {
						let id = Box::pin(self.create_graph_node_from_tagged_object_inner(
							graph, object, None, visited,
						))
						.await?;
						outgoing.insert(tg::Reference::with_object(&object.id(self).await?), id);
					}
				},
				Some(tg::file::Dependencies::Map(_)) => {
					todo!()
				},
				Some(tg::file::Dependencies::Lock(lock, node)) => {
					// Get the lock node.
					let object = lock.object(self).await?;

					// Create a map of visited lock nodes.
					let mut visited = BTreeMap::new();

					// Add the root.
					visited.insert((lock.id(self).await?, *node), id.clone());

					// Convert lock nodes to graph nodes.
					let node = &object.nodes[*node];
					for (reference, either) in node.dependencies.iter().flatten() {
						// TODO: accept unification args here.
						let id = if let Some(id) = try_get_id(reference) {
							id
						} else {
							match either {
								Either::Left(node) => {
									self.create_graph_node_from_lock_node(
										graph,
										lock,
										*node,
										&mut visited,
									)
									.await?
								},
								Either::Right(object) => {
									self.create_graph_node_from_object(
										graph,
										object.id(self).await?,
									)
									.await?
								},
							}
						};
						outgoing.insert(reference.clone(), id);
					}
				},
				None => (),
			}
		}
		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing,
			_inline_object: false,
			object: Some(object_id),
			tag,
		};
		graph.nodes.insert(id.clone(), node);

		Ok(id)
	}
}

// Lock creation
impl Server {
	async fn create_lock(&self, graph: &Graph, root: &Id) -> tg::Result<tg::lock::Data> {
		let mut lock_nodes = Vec::with_capacity(graph.nodes.len());
		let mut visited = BTreeMap::new();
		self.create_lock_inner(graph, root, &mut lock_nodes, &mut visited)
			.left()
			.ok_or_else(|| tg::error!("expected a the root to have an index"))?;
		let lock = tg::lock::Data { nodes: lock_nodes };
		Ok(lock)
	}

	fn create_lock_inner(
		&self,
		graph: &Graph,
		id: &Id,
		lock_nodes: &mut Vec<tg::lock::data::Node>,
		visited: &mut BTreeMap<Id, Either<usize, tg::object::Id>>,
	) -> Either<usize, tg::object::Id> {
		// Check if we've visited this node already.
		if let Some(either) = visited.get(id) {
			return either.clone();
		}

		// Get the graph node.
		let graph_node = graph.nodes.get(id).unwrap();

		// Get the object ID.
		let object = match &graph_node.object {
			// If this is an artifact on disk, the object is None.
			None => None,

			// If this is an object without any dependencies, inline it.
			Some(object) if graph_node.outgoing.is_empty() => {
				let either = Either::Right(object.clone());
				visited.insert(id.clone(), either.clone());
				return either;
			},

			// Otherwise, create a node.
			Some(object) => Some(object.clone()),
		};

		// Create the node in the global lock.
		let lock_node = tg::lock::data::Node {
			object,
			dependencies: None,
		};

		// Add the lock_node and get its index.
		let index = lock_nodes.len();
		lock_nodes.push(lock_node);

		// Update the visited table.
		visited.insert(id.clone(), Either::Left(index));

		// Recurse over dependencies.
		let dependencies = graph_node
			.outgoing
			.iter()
			.map(|(reference, id)| {
				let either = self.create_lock_inner(graph, id, lock_nodes, visited);
				(reference.clone(), either)
			})
			.collect();

		// Update the node and return the index.
		lock_nodes[index].dependencies.replace(dependencies);
		Either::Left(index)
	}
}

struct CollectOutputState {
	visited: RwLock<BTreeMap<tg::Path, Option<Output>>>,
	lock: RwLock<tg::lock::Data>,
}

// Output
impl Server {
	async fn collect_output(
		&self,
		input: &Input,
		lock: &tg::lock::Data,
	) -> tg::Result<(Output, tg::lock::Data)> {
		// Create the initial state.
		let state = CollectOutputState {
			lock: RwLock::new(lock.clone()),
			visited: RwLock::new(BTreeMap::new()),
		};

		// Get the output.
		let output = self.collect_output_inner(input, 0, &state).await?;

		// Add the lock to the root object if it is a file with dependencies.
		let lock = state.lock.into_inner().unwrap();

		Ok((output, lock))
	}

	async fn collect_output_inner(
		&self,
		input: &Input,
		node: usize,
		state: &CollectOutputState,
	) -> tg::Result<Output> {
		// Check if this node has been visited or there is a cycle in object creation.
		{
			let visited = state.visited.read().unwrap();
			if let Some(output) = visited.get(&input.arg.path) {
				return output
					.clone()
					.ok_or_else(|| tg::error!("cycle detected when collecting output"));
			}
		}

		state
			.visited
			.write()
			.unwrap()
			.insert(input.arg.path.clone(), None);

		state
			.visited
			.write()
			.unwrap()
			.insert(input.arg.path.clone(), None);

		// Create the output.
		let output = if input.metadata.is_dir() {
			self.create_directory_output(input, node, state).await?
		} else if input.metadata.is_file() {
			self.create_file_output(input, node, state).await?
		} else if input.metadata.is_symlink() {
			self.create_symlink_output(input, node, state).await?
		} else {
			return Err(tg::error!("invalid file type"));
		};

		// Get the artifact ID.
		let id = output.data.id()?;

		// Copy or move the file.
		if input.is_root {
			self.copy_or_move_file(&input.arg, &id).await.map_err(
				|source| tg::error!(!source, %path = input.arg.path, "failed to copy or move file to checkouts"),
			)?;
		}

		// Update the lock.
		state.lock.write().unwrap().nodes[node]
			.object
			.replace(id.into());

		// Update the output.
		state
			.visited
			.write()
			.unwrap()
			.get_mut(&input.arg.path)
			.unwrap()
			.replace(output.clone());

		Ok(output)
	}

	async fn create_directory_output(
		&self,
		input: &Input,
		node: usize,
		state: &CollectOutputState,
	) -> tg::Result<Output> {
		let children = input
			.dependencies
			.iter()
			.filter_map(|(reference, input)| {
				let lock = state.lock.read().unwrap();
				let node = &lock.nodes[node];
				let input = input.as_ref()?.clone();
				let name = reference
					.path()
					.try_unwrap_path_ref()
					.ok()?
					.components()
					.last()?
					.try_unwrap_normal_ref()
					.ok()?
					.clone();
				let server = self.clone();
				let node = *node
					.dependencies
					.as_ref()?
					.get(reference)?
					.as_ref()
					.left()?;
				let fut = async move {
					let output = Box::pin(server.collect_output_inner(&input, node, state)).await?;
					Ok::<_, tg::Error>((name, output))
				};
				Some(fut)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		let entries = children
			.iter()
			.filter_map(|(name, output)| Some((name.clone(), output.data.id().ok()?)))
			.collect();
		let dependencies = children
			.into_iter()
			.map(|(name, output)| (tg::Reference::with_path(&name.into()), output))
			.collect();
		let data = tg::artifact::Data::Directory(tg::directory::Data { entries });

		let output = Output {
			data,
			dependencies,
			lock_index: node,
		};

		Ok(output)
	}

	async fn create_file_output(
		&self,
		input: &Input,
		node: usize,
		state: &CollectOutputState,
	) -> tg::Result<Output> {
		// Create a blob.
		let _permit = self.file_descriptor_semaphore.acquire().await.ok();
		let reader = tokio::fs::File::open(&input.arg.path)
			.await
			.map_err(|source| tg::error!(!source, %path = input.arg.path, "failed to open file"))?;
		let blob = self.create_blob(reader).await.map_err(
			|source| tg::error!(!source, %path = input.arg.path, "failed to create blob"),
		)?;

		// Create the file data.
		let contents = blob.blob;
		let executable = (input.metadata.permissions().mode() & 0o111) != 0;
		let dependencies = if let Some(dependencies) = xattr::get(
			&input.arg.path,
			tg::file::TANGRAM_FILE_DEPENDENCIES_XATTR_NAME,
		)
		.map_err(|source| tg::error!(!source, %path = input.arg.path, "failed to read xattrs"))?
		{
			let dependencies = serde_json::from_slice(&dependencies)
				.map_err(|source| tg::error!(!source, "failed to deserialize xattr"))?;
			Some(dependencies)
		} else {
			None
		};
		let metadata = if let Some(dependencies) =
			xattr::get(&input.arg.path, tg::file::TANGRAM_FILE_METADATA_XATTR_NAME).map_err(
				|source| tg::error!(!source, %path = input.arg.path, "failed to read xattrs"),
			)? {
			let metadata = serde_json::from_slice(&dependencies)
				.map_err(|source| tg::error!(!source, "failed to deserialize xattr"))?;
			Some(metadata)
		} else {
			None
		};
		let data = tg::artifact::Data::File(tg::file::Data {
			contents,
			dependencies,
			executable,
			metadata,
		});

		// Get the children.
		let dependencies = input
			.dependencies
			.iter()
			.filter_map(|(reference, input)| {
				let node = *state.lock.read().unwrap().nodes[node]
					.dependencies
					.as_ref()?
					.get(reference)?
					.as_ref()
					.left()?;
				let input = input.as_ref()?.clone();
				let fut = async move {
					let output = Box::pin(self.collect_output_inner(&input, node, state)).await?;
					Ok::<_, tg::Error>((reference.clone(), output))
				};
				Some(fut)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<BTreeMap<_, _>>()
			.await?;

		// Create the output.
		Ok(Output {
			data,
			dependencies,
			lock_index: node,
		})
	}

	async fn create_symlink_output(
		&self,
		input: &Input,
		node: usize,
		_state: &CollectOutputState,
	) -> tg::Result<Output> {
		// Read the target from the symlink.
		let target = tokio::fs::read_link(&input.arg.path).await.map_err(
			|source| tg::error!(!source, %path = input.arg.path, r#"failed to read the symlink at path"#,),
		)?;

		// Unrender the target.
		let target = target
			.to_str()
			.ok_or_else(|| tg::error!("the symlink target must be valid UTF-8"))?;
		let artifacts_path = self.artifacts_path();
		let artifacts_path = artifacts_path
			.to_str()
			.ok_or_else(|| tg::error!("the artifacts path must be valid UTF-8"))?;
		let target = tg::template::Data::unrender(artifacts_path, target)?;

		// Get the artifact and path.
		let (artifact, path) = if target.components.len() == 1 {
			let path = target.components[0]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(None, Some(path))
		} else if target.components.len() == 2 {
			let artifact = target.components[0]
				.try_unwrap_artifact_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = target.components[1]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid sylink"))?
				.clone();
			let path = &path[1..];
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(Some(artifact), Some(path))
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		// Create the symlink.
		let data = tg::artifact::Data::Symlink(tg::symlink::Data {
			artifact: artifact.clone(),
			path,
		});

		let output = Output {
			data,
			dependencies: BTreeMap::new(),
			lock_index: node,
		};

		Ok(output)
	}

	async fn copy_or_move_file(
		&self,
		arg: &tg::artifact::checkin::Arg,
		id: &tg::artifact::Id,
	) -> tg::Result<()> {
		// Skip copying anything in the checkouts directory.
		let checkouts_directory: tg::Path = self.checkouts_path().try_into()?;
		if let Some(path) = arg.path.diff(&checkouts_directory) {
			if matches!(
				path.components().first(),
				Some(tg::path::Component::Current)
			) {
				return Ok(());
			}
		}

		let destination = self.checkouts_path().join(id.to_string());
		if arg.destructive {
			match tokio::fs::rename(&arg.path, &destination).await {
				Ok(()) => return Ok(()),
				Err(error) if error.raw_os_error() == Some(libc::EEXIST) => return Ok(()),
				Err(error) if error.raw_os_error() == Some(libc::ENODEV) => (),
				Err(source) => return Err(tg::error!(!source, "failed to rename file")),
			};
		}

		match copy_all(arg.path.as_ref(), &destination).await {
			Ok(()) => Ok(()),
			Err(error) if error.raw_os_error() == Some(libc::EEXIST) => Ok(()),
			Err(source) => Err(tg::error!(!source, "failed to copy file")),
		}
	}
}

impl Server {
	pub async fn split_locks(
		&self,
		lock: &tg::lock::Data,
		old_files: &BTreeMap<tg::file::Id, tg::file::Data>,
	) -> tg::Result<(
		BTreeMap<usize, (tg::lock::Id, usize)>,
		BTreeMap<usize, tg::file::Data>,
	)> {
		let mut locks: Vec<tg::lock::Id> = Vec::new();
		let mut indices = BTreeMap::new();
		let mut new_files = BTreeMap::new();

		for (lock_index, scc) in petgraph::algo::tarjan_scc(GraphImpl(lock))
			.into_iter()
			.enumerate()
		{
			let mut nodes = Vec::with_capacity(scc.len());

			// Create new indices for each node.
			for (new_index, old_index) in scc.iter().copied().enumerate() {
				indices.insert(old_index, (lock_index, new_index));
			}

			// Remap nodes.
			for old_index in scc {
				let old_node = lock.nodes[old_index].clone();
				let Some(old_dependencies) = &old_node.dependencies else {
					nodes.push(old_node.clone());
					continue;
				};

				let mut dependencies = BTreeMap::new();
				'b: for (reference, either) in old_dependencies {
					let either = match either {
						Either::Left(index) => 'a: {
							let (lock_, node_) = indices.get(index).copied().unwrap();
							if lock_ == lock_index {
								break 'a Either::Left(node_);
							}

							let Some(tg::object::Id::File(file)) =
								lock.nodes[old_index].object.as_ref()
							else {
								continue 'b;
								// return Err(tg::error!("expected a file"));
							};

							let mut data = old_files
								.get(file)
								.ok_or_else(|| tg::error!("missing file data"))?
								.clone();

							data.dependencies
								.replace(tg::file::data::Dependencies::Lock(
									locks[lock_].clone(),
									node_,
								));

							let id = tg::file::Id::new(&data.serialize()?);
							new_files.insert(old_index, data);

							Either::Right(id.into())
						},
						Either::Right(object) => Either::Right(object.clone()),
					};
					dependencies.insert(reference.clone(), either);
				}

				let new_node = tg::lock::data::Node {
					dependencies: Some(dependencies),
					..old_node.clone()
				};
				nodes.push(new_node);
			}

			// Create the lock.
			let lock = tg::lock::data::Data { nodes };
			let lock = tg::lock::Object::try_from(lock)?;
			let lock = tg::Lock::with_object(lock).id(self).await?;
			locks.push(lock);
		}

		let locks = indices
			.into_iter()
			.map(|(index, (lock, node))| (index, (locks[lock].clone(), node)))
			.collect();

		Ok((locks, new_files))
	}
}

impl Server {
	async fn write_hardlinks_and_xattrs(&self, input: &Input, output: &Output) -> tg::Result<()> {
		let path = self
			.checkouts_path()
			.join(output.data.id()?.to_string())
			.try_into()?;
		let mut visited = BTreeSet::new();
		self.write_hardlinks_and_xattrs_inner(&path, input, output, &mut visited)
			.await?;
		Ok(())
	}

	async fn write_hardlinks_and_xattrs_inner(
		&self,
		path: &tg::Path,
		input: &Input,
		output: &Output,
		visited: &mut BTreeSet<tg::Path>,
	) -> tg::Result<()> {
		if visited.contains(path) {
			return Ok(());
		};
		visited.insert(path.clone());

		if let tg::artifact::Data::File(file) = &output.data {
			let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();

			let dst: tg::Path = self
				.checkouts_path()
				.join(output.data.id()?.to_string())
				.try_into()?;

			if let Some(dependencies) = &file.dependencies {
				let data = serde_json::to_vec(dependencies)
					.map_err(|source| tg::error!(!source, "failed to serialize dependencies"))?;
				xattr::set(&dst, tg::file::TANGRAM_FILE_DEPENDENCIES_XATTR_NAME, &data)
					.map_err(|source| tg::error!(!source, "failed to set xattrs"))?;
			}

			// Create hard link to the file.
			match tokio::fs::hard_link(path, &dst).await {
				Ok(()) => (),
				Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
				Err(source) => {
					return Err(tg::error!(!source, %src = path, %dst, "failed to create hardlink"))
				},
			}

			// Create a symlink to the file in the blobs directory.
			let symlink_target = PathBuf::from("../checkouts").join(output.data.id()?.to_string());
			let symlink_path = self.blobs_path().join(file.contents.to_string());
			match tokio::fs::symlink(&symlink_target, &symlink_path).await {
				Ok(()) => (),
				Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
				Err(source) => {
					return Err(
						tg::error!(!source, %src = symlink_target.display(), %dst = symlink_path.display(), "failed to create blob symlink"),
					)
				},
			}
		}

		for (reference, child_input) in input
			.dependencies
			.iter()
			.filter_map(|(reference, input)| Some((reference, input.as_ref()?)))
		{
			let child_output = output.dependencies.get(reference).ok_or_else(
				|| tg::error!(%referrer = path, %reference, "missing output reference"),
			)?;

			let path = if child_input.is_root {
				self.checkouts_path()
					.join(child_output.data.id()?.to_string())
					.try_into()?
			} else {
				let path_ = reference
					.path()
					.try_unwrap_path_ref()
					.ok()
					.or_else(|| reference.query()?.path.as_ref())
					.cloned()
					.ok_or_else(|| tg::error!("expected a path dependency"))?;

				if input.metadata.is_dir() {
					path.clone().join(path_)
				} else {
					path.clone().parent().join(path_).normalize()
				}
			};

			Box::pin(self.write_hardlinks_and_xattrs_inner(
				&path,
				child_input,
				child_output,
				visited,
			))
			.await?;
		}

		Ok(())
	}
}

impl Graph {
	pub fn validate(&self, server: &Server) -> tg::Result<()> {
		let mut errors = Vec::new();
		for node in self.nodes.values() {
			let errors_ = node
				.errors
				.iter()
				.map(|error| tg::error!(%error, %node = node.id, "node contains error"));
			errors.extend(errors_);
			for (reference, id) in &node.outgoing {
				if !self.nodes.contains_key(id) {
					let error =
						tg::error!(%reference, %node = node.id, "failed to resolve dependency");
					errors.push(error);
				}
			}
		}
		if errors.is_empty() {
			return Ok(());
		}
		for error in errors {
			let trace = error.trace(&server.options.advanced.error_trace_options);
			tracing::error!("{trace}");
		}
		Err(tg::error!("invalid graph"))
	}

	pub fn outgoing(&self, src: Id) -> impl Iterator<Item = Edge> + '_ {
		self.nodes
			.get(&src)
			.unwrap()
			.outgoing
			.iter()
			.map(move |(dependency, dst)| Edge {
				src: src.clone(),
				dst: dst.clone(),
				reference: dependency.clone(),
			})
	}

	fn add_error(&mut self, node: &Id, error: tg::Error) {
		self.nodes.get_mut(node).unwrap().errors.push(error);
	}
}

fn try_backtrack(state: &mut Vec<State>, edge: &Edge) -> Option<State> {
	// Find the index of the state where the node was first added.
	let position = state
		.iter()
		.position(|state| state.graph.nodes.contains_key(&edge.dst))?;

	// Backtrack.
	state.truncate(position);
	let mut state = state.pop()?;

	// This bit is a little weird. TODO don't make this so jank.
	state.queue.push_front(edge.clone());

	// If the edge we failed at is still in the graph, it means that we can use the dependency as an additional heuristic to inform the next selection.
	let edge_in_graph = state
		.graph
		.nodes
		.get(&edge.src)
		.map_or(false, |src| src.outgoing.contains_key(&edge.reference));
	if edge_in_graph {
		let packages = state
			.objects
			.as_ref()
			.unwrap()
			.iter()
			.filter(|(version, _)| edge.reference.path().unwrap_tag_ref().matches(version))
			.cloned()
			.collect();
		state.objects.replace(packages);
	}

	Some(state)
}

fn get_reference_from_pattern(pattern: &tg::tag::Pattern) -> tg::Reference {
	let components = pattern.components();
	let string = match components.last() {
		Some(tg::tag::pattern::Component::Semver(_)) if components.len() >= 2 => {
			let mut components = components.clone();
			components.pop();
			let pattern = tg::tag::Pattern::with_components(components);
			format!("{pattern}/*")
		},
		_ => pattern.to_string(),
	};
	string.parse().unwrap()
}

fn try_get_id(reference: &tg::Reference) -> Option<Id> {
	reference
		.path()
		.try_unwrap_tag_ref()
		.ok()
		.map(get_reference_from_pattern)
		.map(Either::Left)
}

#[derive(Copy, Clone)]
struct GraphImpl<'a>(&'a tg::lock::Data);

impl<'a> petgraph::visit::GraphBase for GraphImpl<'a> {
	type EdgeId = (usize, usize);
	type NodeId = usize;
}

impl<'a> petgraph::visit::GraphRef for GraphImpl<'a> {}

#[allow(clippy::needless_arbitrary_self_type)]
impl<'a> petgraph::visit::NodeIndexable for GraphImpl<'a> {
	fn from_index(self: &Self, i: usize) -> Self::NodeId {
		i
	}

	fn node_bound(self: &Self) -> usize {
		self.0.nodes.len()
	}

	fn to_index(self: &Self, a: Self::NodeId) -> usize {
		a
	}
}

impl<'a> petgraph::visit::IntoNeighbors for GraphImpl<'a> {
	type Neighbors = Box<dyn Iterator<Item = usize> + 'a>;
	fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
		let iter = self.0.nodes[a]
			.dependencies
			.iter()
			.flat_map(|map| map.values().filter_map(|e| e.as_ref().left()).copied());
		Box::new(iter)
	}
}

impl<'a> petgraph::visit::IntoNodeIdentifiers for GraphImpl<'a> {
	type NodeIdentifiers = std::ops::Range<usize>;
	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.0.nodes.len()
	}
}

async fn copy_all(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
	let mut stack = vec![(from.to_owned(), to.to_owned())];
	while let Some((from, to)) = stack.pop() {
		let metadata = tokio::fs::symlink_metadata(&from).await?;
		let file_type = metadata.file_type();
		if file_type.is_dir() {
			tokio::fs::create_dir_all(&to).await?;
			let mut entries = tokio::fs::read_dir(&from).await?;
			while let Some(entry) = entries.next_entry().await? {
				let from = from.join(entry.file_name());
				let to = to.join(entry.file_name());
				stack.push((from, to));
			}
		} else if file_type.is_file() {
			tokio::fs::copy(&from, &to).await?;
		} else if file_type.is_symlink() {
			let target = tokio::fs::read_link(&from).await?;
			tokio::fs::symlink(&target, &to).await?;
		} else {
			return Err(std::io::Error::other("invalid file type"));
		}
	}
	Ok(())
}
