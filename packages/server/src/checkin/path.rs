use {
	super::graph::{Graph, Variant},
	crate::Session,
	std::{
		collections::BTreeMap,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

pub type Paths = BTreeMap<(usize, tg::Reference), tg::graph::data::Edge<tg::object::Id>>;

impl Session {
	pub(super) async fn try_checkin_store_path(
		&self,
		path: &Path,
	) -> tg::Result<Option<tg::checkin::Output>> {
		let checkout_path = self.server.checkout_path();
		let store_path = self.server.store_path();
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
			.checkin_store_path_inner(id, &path)
			.await
			.map_err(|error| tg::error!(!error, "failed to check in the store path"))?;

		Ok(Some(output))
	}

	async fn checkin_store_path_inner(
		&self,
		id: tg::artifact::Id,
		path: &Path,
	) -> tg::Result<tg::checkin::Output> {
		// Recover proofs from the physical checkout and the authenticated origin sandbox.
		let checkout_path = self.server.checkout_path().join(id.to_string());
		let mut tokens = Self::checkin_read_file_tokens(&checkout_path)?;
		let sandbox = self.try_get_checkin_origin_sandbox()?;
		if let Some(sandbox) = &sandbox
			&& let Some(state) = self.server.runner.state().sandboxes().get_by_id(sandbox)
			&& let Some(token) = state.tokens.get(&id)
		{
			tokens.insert_local_authorization(token.clone());
		}

		// Authorize the root and bound both returned tokens by the accepted proof.
		let mut referent = tg::Referent::with_node_and_tokens(id.clone(), tokens);
		let subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let authorization = self
			.authorize_object_read(referent.clone(), true)
			.await?
			.filter(|authorization| authorization.permissions.contains(subtree))
			.ok_or_else(|| tg::error!("unauthorized"))?;
		let now = self.server.clock.unix_timestamp()?;
		let time_to_live = i64::try_from(self.server.config.object.grant_time_to_live.as_secs())
			.map_err(|error| tg::error!(!error, "failed to convert the grant time to live"))?;
		let expires_at = now
			.checked_add(time_to_live)
			.ok_or_else(|| tg::error!("the grant expiration overflowed"))?;
		let expires_at = authorization
			.expires_at
			.map_or(expires_at, |expiration| expiration.min(expires_at));
		let root_token = self.create_token(id.clone().into(), vec![subtree], expires_at)?;
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
				.get_with_handle(self, path)
				.await
				.map_err(|error| tg::error!(!error, "failed to resolve the artifact path"))?;
			let node = artifact
				.store_with_handle(self)
				.await
				.map_err(|error| tg::error!(!error, "failed to store the resolved artifact"))?;
			referent = tg::Referent::with_node(node);
			referent.options.id = Some(id.into());
			referent.options.path = Some(path.to_owned());
		}

		// Return exact tokens for the resolved artifact and its containing root.
		referent.options.tokens.clear();
		if referent.options.id.is_some()
			&& let Some(token) =
				self.create_token(referent.node.clone().into(), vec![subtree], expires_at)?
		{
			referent.options.tokens.insert_local_authorization(token);
		}
		if let Some(token) = root_token {
			referent.options.tokens.insert_local_authorization(token);
		}
		if let Some(sandbox) = &sandbox
			&& let Some(mut state) = self
				.server
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

	fn try_get_checkin_origin_sandbox(&self) -> tg::Result<Option<tg::sandbox::Id>> {
		let Some(sandbox) = self
			.server
			.try_get_request_origin_sandbox(self.context.origin)?
		else {
			return Ok(None);
		};
		let authorized = match &self.context.principal {
			tg::Principal::Process(id) => sandbox.processes.get_by_id(id).is_some(),
			tg::Principal::Sandbox(id) => sandbox.id.as_ref() == Some(id),
			_ => false,
		};
		let id = authorized.then(|| sandbox.id.clone()).flatten();

		Ok(id)
	}

	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn checkin_path_get_edges(
		&self,
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
							let target = self.checkin_path_visit(graph, edge.clone(), get).await?;
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
			tg::graph::data::Edge::Pointer(pointer) if pointer.graph.is_none() => {
				let node = graph.nodes.get(&pointer.index);
				let is_directory = matches!(pointer.kind, tg::artifact::Kind::Directory);
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
			tg::graph::data::Edge::Pointer(pointer) if pointer.graph.is_none() => graph
				.nodes
				.get(&pointer.index)
				.is_some_and(|n| matches!(&n.variant, Variant::Directory(_))),
			tg::graph::data::Edge::Pointer(pointer) => {
				matches!(pointer.kind, tg::artifact::Kind::Directory)
			},
			tg::graph::data::Edge::Object(id) => id.kind() == tg::object::Kind::Directory,
		}
	}

	async fn checkin_path_visit(
		&self,
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
				.checkin_path_get_directory_entry(graph, &current, name)
				.await?;
		}
		Ok(current)
	}

	async fn checkin_path_get_directory_entry(
		&self,
		graph: &Graph,
		edge: &tg::graph::data::Edge<tg::object::Id>,
		name: &str,
	) -> tg::Result<tg::graph::data::Edge<tg::object::Id>> {
		match edge {
			tg::graph::data::Edge::Pointer(pointer) if pointer.graph.is_none() => {
				let node = graph
					.nodes
					.get(&pointer.index)
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
				let graph_id = pointer
					.graph
					.as_ref()
					.ok_or_else(|| tg::error!("expected graph id"))?;
				let data = self.checkin_path_load_graph(graph_id).await?;
				let node = data
					.nodes
					.get(pointer.index)
					.ok_or_else(|| tg::error!("invalid index"))?;
				let directory = node
					.try_unwrap_directory_ref()
					.map_err(|_| tg::error!("expected a directory"))?;
				let leaf = directory
					.try_unwrap_leaf_ref()
					.map_err(|_| tg::error!("expected a leaf directory"))?;
				let entry = leaf
					.entries
					.get(name)
					.ok_or_else(|| tg::error!(%name, "entry not found"))?;
				Ok(Self::checkin_path_convert_entry_with_graph(entry, graph_id))
			},
			tg::graph::data::Edge::Object(id) => {
				let artifact_id: tg::artifact::Id = id
					.clone()
					.try_into()
					.map_err(|_| tg::error!("expected artifact"))?;
				let data = self.checkin_path_load_directory(&artifact_id).await?;
				let leaf = data
					.try_unwrap_leaf_ref()
					.map_err(|_| tg::error!("expected a leaf directory"))?;
				let entry = leaf
					.entries
					.get(name)
					.ok_or_else(|| tg::error!(%name, "entry not found"))?;
				Ok(Self::checkin_path_convert_artifact_edge(entry))
			},
		}
	}

	async fn checkin_path_load_graph(&self, id: &tg::graph::Id) -> tg::Result<tg::graph::Data> {
		let output = self
			.try_get_object_local(&id.clone().into(), false, false, &[])
			.await?
			.ok_or_else(|| tg::error!("graph not found"))?;
		tg::graph::Data::deserialize(output.bytes)
			.map_err(|e| tg::error!(!e, "failed to deserialize graph"))
	}

	async fn checkin_path_load_directory(
		&self,
		id: &tg::artifact::Id,
	) -> tg::Result<tg::graph::data::Directory> {
		let output = self
			.try_get_object_local(&id.clone().into(), false, false, &[])
			.await?
			.ok_or_else(|| tg::error!("directory not found"))?;
		let data = tg::directory::Data::deserialize(output.bytes)
			.map_err(|e| tg::error!(!e, "failed to deserialize"))?;
		match data {
			tg::directory::Data::Node(node) => Ok(node),
			tg::directory::Data::Pointer(pointer) => {
				let graph_id = pointer
					.graph
					.as_ref()
					.ok_or_else(|| tg::error!("expected graph"))?;
				let graph = self.checkin_path_load_graph(graph_id).await?;
				let node = graph
					.nodes
					.get(pointer.index)
					.ok_or_else(|| tg::error!("invalid index"))?;
				node.try_unwrap_directory_ref()
					.cloned()
					.map_err(|_| tg::error!("expected a directory"))
			},
		}
	}

	fn checkin_path_convert_artifact_edge(
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
	) -> tg::graph::data::Edge<tg::object::Id> {
		match edge {
			tg::graph::data::Edge::Pointer(pointer) => {
				tg::graph::data::Edge::Pointer(pointer.clone())
			},
			tg::graph::data::Edge::Object(id) => tg::graph::data::Edge::Object(id.clone().into()),
		}
	}

	fn checkin_path_convert_entry_with_graph(
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
		graph_id: &tg::graph::Id,
	) -> tg::graph::data::Edge<tg::object::Id> {
		match edge {
			tg::graph::data::Edge::Pointer(pointer) => {
				let graph = pointer.graph.clone().or_else(|| Some(graph_id.clone()));
				tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph,
					index: pointer.index,
					kind: pointer.kind,
				})
			},
			tg::graph::data::Edge::Object(id) => tg::graph::data::Edge::Object(id.clone().into()),
		}
	}
}
