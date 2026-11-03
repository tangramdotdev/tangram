use {
	crate::{Component, Graph, Variant},
	futures::TryStreamExt as _,
	std::{
		collections::BTreeMap,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
	tangram_server_core::Session,
};

pub type Paths = BTreeMap<(usize, tg::Reference), tg::graph::data::Edge<tg::object::Id>>;

impl Component {
	pub(super) async fn try_checkin_store_path(
		&self,
		session: &Session,
		path: &Path,
	) -> tg::Result<Option<tg::checkin::Output>> {
		let checkout_path = session.server.checkout_path();
		let store_path = session.server.store_path();
		let Ok(path) = path
			.strip_prefix(&checkout_path)
			.or_else(|_| path.strip_prefix(&store_path))
		else {
			return Ok(None);
		};

		// Parse the root artifact and the path within it.
		let mut components = path.components();
		let component = components
			.next()
			.ok_or_else(|| tg::error!("cannot check in the store directory"))?;
		let component = component
			.as_os_str()
			.to_str()
			.ok_or_else(|| tg::error!("the store path component is not valid utf-8"))?;
		let component = tg::store::path::parse_component(component)
			.map_err(|error| tg::error!(!error, "failed to parse the store path component"))?;
		let tg::store::path::Component::Id { id, .. } = component else {
			return Ok(None);
		};
		let path = components.collect::<PathBuf>();
		let output = self
			.checkin_store_path_inner(session, id, &path)
			.await
			.map_err(|error| tg::error!(!error, "failed to check in the store path"))?;

		Ok(Some(output))
	}

	async fn checkin_store_path_inner(
		&self,
		session: &Session,
		id: tg::artifact::Id,
		path: &Path,
	) -> tg::Result<tg::checkin::Output> {
		// Recover proofs from the physical checkout, the authenticated origin sandbox, and its VFS.
		let checkout_path = session.server.checkout_path().join(id.to_string());
		let mut tokens = Self::checkin_read_file_tokens(&checkout_path)?;
		let sandbox = Self::try_get_checkin_origin_sandbox(session)?;
		if let Some(sandbox) = &sandbox
			&& let Some(state) = session
				.server
				.components
				.runner
				.state()
				.sandboxes()
				.get_by_id(sandbox)
		{
			if let Some(token) = state.tokens.get(&id) {
				tokens.insert_local_authorization(token.clone());
			}
			if let Some(vfs) = &state.vfs {
				tokens.inherit(&vfs.root_tokens(&id));
			}
		}

		// Authorize the root and bound both returned tokens by the accepted proof.
		let mut referent = tg::Referent::with_node_and_tokens(id.clone(), tokens);
		let subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let authorization = session
			.authorize_object_read(referent.clone())
			.await?
			.check_exhaustion()?;
		let expiration = if authorization.permissions.contains(subtree) {
			authorization.expires_at
		} else {
			let expiration = self
				.checkin_store_path_pull(session, &referent)
				.await?
				.ok_or_else(|| tg::error!("unauthorized"))?;
			Some(expiration)
		};
		let now = session.server.clock.unix_timestamp();
		let time_to_live = i64::try_from(
			session
				.server
				.config
				.object
				.permission_time_to_live
				.as_secs(),
		)
		.map_err(|error| tg::error!(!error, "failed to convert the permission time to live"))?;
		let expires_at = now
			.checked_add(time_to_live)
			.ok_or_else(|| tg::error!("the permission expiration overflowed"))?;
		let expires_at = expiration.map_or(expires_at, |expiration| expiration.min(expires_at));
		let root_token = session.server.components.authorization.create_token(
			session,
			id.clone().into(),
			vec![subtree],
			expires_at,
		)?;
		if let Some(token) = &root_token {
			referent
				.options
				.tokens
				.insert_local_authorization(token.clone());
		}

		// Resolve through artifact handles carrying the proof and the derived child tokens.
		if !path.as_os_str().is_empty() {
			let directory = tg::Artifact::with_referent(referent)
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("the root artifact is not a directory"))?;
			let artifact = directory
				.get_with_instance(session, path)
				.await
				.map_err(|error| tg::error!(!error, "failed to resolve the artifact path"))?;
			let node = artifact
				.store_with_instance(session)
				.await
				.map_err(|error| tg::error!(!error, "failed to store the resolved artifact"))?;
			referent = tg::Referent::with_node(node);
			referent.options.id = Some(id.into());
			referent.options.path = Some(path.to_owned());
		}

		// Return exact tokens for the resolved artifact and its containing root.
		referent.options.tokens.clear();
		if referent.options.id.is_some()
			&& let Some(token) = session.server.components.authorization.create_token(
				session,
				referent.node.clone().into(),
				vec![subtree],
				expires_at,
			)? {
			referent.options.tokens.insert_local_authorization(token);
		}
		if let Some(token) = root_token {
			referent.options.tokens.insert_local_authorization(token);
		}
		if let Some(sandbox) = &sandbox
			&& let Some(mut state) = session
				.server
				.components
				.runner
				.state()
				.sandboxes()
				.get_mut_by_id(sandbox)
		{
			for token in referent.options.tokens.local_authorization() {
				let id = token.body.resource.clone().try_into().unwrap();
				if state
					.tokens
					.get(&id)
					.is_none_or(|existing| existing.body.expires_at < token.body.expires_at)
				{
					state.tokens.insert(id, token.clone());
				}
			}
		}
		let output = tg::checkin::Output { artifact: referent };

		Ok(output)
	}

	/// Pull a store path's root from the configured remote with its tokens, which waits for a root that is still being pushed, and return the expiration of the subtree authorization that the pull proves.
	async fn checkin_store_path_pull(
		&self,
		session: &Session,
		referent: &tg::Referent<tg::artifact::Id>,
	) -> tg::Result<Option<i64>> {
		// Get the configured remote.
		let remote = tg::remote::Selector::Name("default".to_owned());
		let remote = session
			.try_get_remote(&remote, tg::remote::get::Arg::default())
			.await?;
		if remote.is_none() {
			return Ok(None);
		}
		let source = tg::Location::Remote(tg::location::Remote {
			name: "default".to_owned(),
			region: None,
		});

		// Pull the root.
		let arg = tg::pull::Arg {
			nodes: vec![referent.clone().map(tg::Id::from)],
			source: Some(source),
			..Default::default()
		};
		let (_, stream) = session
			.pull(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to pull the store path"))?;
		let mut stream = std::pin::pin!(stream);
		let mut output = None;
		while let Some(event) = stream.try_next().await? {
			if let tg::progress::Event::Output(value) = event {
				output = Some(value);
				break;
			}
		}
		let output = output.ok_or_else(|| tg::error!("the pull ended without an output"))?;

		// Find the subtree authorization that the pull returned for the root.
		let resource = tg::Id::from(tg::object::Id::from(referent.node.clone()));
		let expiration = output
			.nodes
			.iter()
			.filter(|node| node.node == resource)
			.flat_map(|node| node.options.tokens.iter())
			.flat_map(|(_, entry)| &entry.authorization)
			.filter(|token| token.authorizes_object_subtree(&resource))
			.map(|token| token.body.expires_at)
			.max();

		Ok(expiration)
	}

	fn try_get_checkin_origin_sandbox(session: &Session) -> tg::Result<Option<tg::sandbox::Id>> {
		let Some(sandbox) = session
			.server
			.try_get_request_origin_sandbox(session.context.origin)?
		else {
			return Ok(None);
		};
		let authorized = match &session.context.principal {
			tg::Principal::Process(id) => sandbox.processes.get(id).is_some(),
			tg::Principal::Sandbox(id) => sandbox.id == *id,
			_ => false,
		};
		let id = authorized.then(|| sandbox.id.clone());

		Ok(id)
	}

	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn checkin_path_get_edges(
		&self,
		session: &Session,
		graph: &Graph,
		next: usize,
	) -> tg::Result<Paths> {
		let mut paths = Paths::new();
		for index in next..graph.next {
			let node = graph.nodes.get(&index).unwrap();
			if let Variant::File(file) = &node.variant {
				for (reference, dependency) in &file.dependencies {
					if let Some(get) = reference.options().get.as_ref()
						&& let Some(dependency) = dependency
						&& let Some(edge) = dependency.node()
					{
						let visit = if let Some(id) = dependency.id() {
							Self::checkin_path_edge_matches_id(graph, edge, id)
						} else {
							Self::checkin_path_edge_is_directory(graph, edge)
						};
						if visit {
							let target = self
								.checkin_path_visit(session, graph, edge.clone(), get)
								.await?;
							paths.insert((index, reference.clone()), target);
						}
					}
				}
			}
		}
		Ok(paths)
	}

	fn checkin_path_edge_matches_id(
		graph: &Graph,
		edge: &tg::graph::data::Edge<tg::object::Id>,
		id: &tg::object::Id,
	) -> bool {
		let (is_directory, node_id) = match edge {
			tg::graph::data::Edge::Index(target_index) => {
				let node = graph.nodes.get(target_index);
				let is_directory =
					node.is_some_and(|node| matches!(&node.variant, Variant::Directory(_)));
				let node_id = node.and_then(|node| node.id.clone());
				(is_directory, node_id)
			},
			tg::graph::data::Edge::Pointer(p) => {
				(matches!(p.kind, tg::artifact::Kind::Directory), None)
			},
			tg::graph::data::Edge::Object(edge_id) => return edge_id == id,
		};
		let options_is_directory = id.kind() == tg::object::Kind::Directory;
		if options_is_directory && !is_directory {
			return false;
		}
		node_id.is_none_or(|id_| &id_ == id)
	}

	fn checkin_path_edge_is_directory(
		graph: &Graph,
		edge: &tg::graph::data::Edge<tg::object::Id>,
	) -> bool {
		match edge {
			tg::graph::data::Edge::Index(target_index) => graph
				.nodes
				.get(target_index)
				.is_some_and(|n| matches!(&n.variant, Variant::Directory(_))),
			tg::graph::data::Edge::Pointer(pointer) => {
				matches!(pointer.kind, tg::artifact::Kind::Directory)
			},
			tg::graph::data::Edge::Object(id) => id.kind() == tg::object::Kind::Directory,
		}
	}

	async fn checkin_path_visit(
		&self,
		session: &Session,
		graph: &Graph,
		edge: tg::graph::data::Edge<tg::object::Id>,
		path: &Path,
	) -> tg::Result<tg::graph::data::Edge<tg::object::Id>> {
		let mut current = edge;
		for component in path.components() {
			let name = match component {
				std::path::Component::Normal(n) => n
					.to_str()
					.ok_or_else(|| tg::error!("invalid path component"))?,
				_ => return Err(tg::error!("unexpected path component")),
			};
			current = self
				.checkin_path_get_directory_entry(session, graph, &current, name)
				.await?;
		}
		Ok(current)
	}

	async fn checkin_path_get_directory_entry(
		&self,
		session: &Session,
		graph: &Graph,
		edge: &tg::graph::data::Edge<tg::object::Id>,
		name: &str,
	) -> tg::Result<tg::graph::data::Edge<tg::object::Id>> {
		match edge {
			tg::graph::data::Edge::Index(target_index) => {
				let node = graph
					.nodes
					.get(target_index)
					.ok_or_else(|| tg::error!("node not found"))?;
				let directory = node
					.variant
					.try_unwrap_directory_ref()
					.map_err(|_| tg::error!("expected a directory"))?;
				let entry = directory
					.entries
					.get(name)
					.ok_or_else(|| tg::error!(%name, "entry not found"))?;
				Ok(Self::checkin_path_convert_artifact_edge(entry))
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				let graph_id = &pointer.graph;
				let data = self.checkin_path_load_graph(session, graph_id).await?;
				let node = data
					.nodes
					.get(pointer.index)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let directory = node
					.try_unwrap_directory_ref()
					.map_err(|_| tg::error!("expected a directory"))?
					.clone();
				let context = Some((graph_id.clone(), data));
				self.checkin_path_get_entry(session, directory, context, name)
					.await
			},
			tg::graph::data::Edge::Object(id) => {
				let artifact_id: tg::artifact::Id = id
					.clone()
					.try_into()
					.map_err(|_| tg::error!("expected artifact"))?;
				let (directory, context) = self
					.checkin_path_load_directory(session, &artifact_id)
					.await?;
				self.checkin_path_get_entry(session, directory, context, name)
					.await
			},
		}
	}

	/// Get an entry of a directory by name, descending through the directory's branches.
	async fn checkin_path_get_entry(
		&self,
		session: &Session,
		directory: tg::graph::data::Directory,
		context: Option<(tg::graph::Id, tg::graph::data::Leaf)>,
		name: &str,
	) -> tg::Result<tg::graph::data::Edge<tg::object::Id>> {
		let mut directory = directory;
		let mut context = context;
		loop {
			// Get the entry from a leaf.
			let branch = match directory {
				tg::graph::data::Directory::Leaf(leaf) => {
					let entry = leaf
						.entries
						.get(name)
						.ok_or_else(|| tg::error!(%name, "entry not found"))?;
					return match &context {
						Some((graph_id, graph)) => {
							Self::checkin_path_convert_entry_with_graph(entry, graph_id, graph)
						},
						None => Ok(Self::checkin_path_convert_artifact_edge(entry)),
					};
				},
				tg::graph::data::Directory::Branch(branch) => branch,
			};

			// Find the child whose range of names contains the name.
			let child = branch
				.children
				.into_iter()
				.find(|child| name <= child.last.as_str())
				.ok_or_else(|| tg::error!(%name, "entry not found"))?;

			// Get the child directory.
			(directory, context) = match child.directory {
				tg::graph::data::Edge::Index(index) => {
					let (_, graph) = context
						.as_ref()
						.ok_or_else(|| tg::error!("missing graph"))?;
					let directory = graph
						.nodes
						.get(index)
						.ok_or_else(|| tg::error!("invalid index"))?
						.try_unwrap_directory_ref()
						.map_err(|_| tg::error!("expected a directory"))?
						.clone();
					(directory, context)
				},
				tg::graph::data::Edge::Pointer(pointer) => {
					let graph = self
						.checkin_path_load_graph(session, &pointer.graph)
						.await?;
					let directory = graph
						.nodes
						.get(pointer.index)
						.ok_or_else(|| tg::error!("invalid index"))?
						.try_unwrap_directory_ref()
						.map_err(|_| tg::error!("expected a directory"))?
						.clone();
					(directory, Some((pointer.graph, graph)))
				},
				tg::graph::data::Edge::Object(id) => {
					self.checkin_path_load_directory(session, &id.into())
						.await?
				},
			};
		}
	}

	/// Load all of a graph's nodes as a single leaf, loading each leaf of a branched graph.
	async fn checkin_path_load_graph(
		&self,
		session: &Session,
		id: &tg::graph::Id,
	) -> tg::Result<tg::graph::data::Leaf> {
		let mut nodes = Vec::new();
		let mut stack = vec![id.clone()];
		while let Some(id) = stack.pop() {
			let output = session
				.server
				.components
				.object
				.try_get_object_local(session, &id.clone().into(), false, false, &[])
				.await?
				.ok_or_else(|| tg::error!(%id, "graph not found"))?;
			let data = tg::graph::Data::deserialize(output.bytes)
				.map_err(|error| tg::error!(!error, %id, "failed to deserialize graph"))?;
			match data {
				tg::graph::Data::Leaf(leaf) => {
					nodes.extend(leaf.nodes);
				},
				tg::graph::Data::Branch(branch) => {
					stack.extend(branch.children.into_iter().rev().map(|child| child.graph));
				},
			}
		}
		let leaf = tg::graph::data::Leaf { nodes };
		Ok(leaf)
	}

	async fn checkin_path_load_directory(
		&self,
		session: &Session,
		id: &tg::artifact::Id,
	) -> tg::Result<(
		tg::graph::data::Directory,
		Option<(tg::graph::Id, tg::graph::data::Leaf)>,
	)> {
		let output = session
			.server
			.components
			.object
			.try_get_object_local(session, &id.clone().into(), false, false, &[])
			.await?
			.ok_or_else(|| tg::error!("directory not found"))?;
		let data = tg::directory::Data::deserialize(output.bytes)
			.map_err(|e| tg::error!(!e, "failed to deserialize"))?;
		match data {
			tg::directory::Data::Node(node) => Ok((node, None)),
			tg::directory::Data::Pointer(pointer) => {
				let graph_id = &pointer.graph;
				let graph = self.checkin_path_load_graph(session, graph_id).await?;
				let node = graph
					.nodes
					.get(pointer.index)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let directory = node
					.try_unwrap_directory_ref()
					.cloned()
					.map_err(|_| tg::error!("expected a directory"))?;
				Ok((directory, Some((pointer.graph, graph))))
			},
		}
	}

	fn checkin_path_convert_artifact_edge(
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
	) -> tg::graph::data::Edge<tg::object::Id> {
		match edge {
			tg::graph::data::Edge::Index(index) => tg::graph::data::Edge::Index(*index),
			tg::graph::data::Edge::Pointer(pointer) => {
				tg::graph::data::Edge::Pointer(pointer.clone())
			},
			tg::graph::data::Edge::Object(id) => tg::graph::data::Edge::Object(id.clone().into()),
		}
	}

	fn checkin_path_convert_entry_with_graph(
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
		graph_id: &tg::graph::Id,
		graph: &tg::graph::data::Leaf,
	) -> tg::Result<tg::graph::data::Edge<tg::object::Id>> {
		let edge = match edge {
			tg::graph::data::Edge::Index(index) => {
				let node = graph
					.nodes
					.get(*index)
					.ok_or_else(|| tg::error!("invalid node index"))?;
				tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph: graph_id.clone(),
					index: *index,
					kind: node.kind(),
				})
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				let graph = pointer.graph.clone();
				tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph,
					index: pointer.index,
					kind: pointer.kind,
				})
			},
			tg::graph::data::Edge::Object(id) => tg::graph::data::Edge::Object(id.clone().into()),
		};
		Ok(edge)
	}
}
