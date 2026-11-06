use {
	super::{ArtifactState, Authority, Inner, eio, fallback},
	num::ToPrimitive as _,
	std::{collections::BTreeMap, sync::Arc},
	tangram_client::prelude::*,
};

impl Inner {
	pub(super) fn tokens(
		&self,
		authority: &Arc<Authority>,
		id: &tg::object::Id,
	) -> tg::authorization::Tokens {
		self.authorizations.lock().unwrap().tokens(authority, id)
	}

	pub(super) async fn get_object(
		&self,
		authority: &Arc<Authority>,
		id: &tg::object::Id,
	) -> std::io::Result<tg::object::get::Output> {
		let arg = tg::object::get::Arg {
			tokens: self.tokens(authority, id),
			..Default::default()
		};
		let session = self.client.session(self.client.context());
		let output = session
			.try_get_object(id, arg)
			.await
			.map_err(eio)?
			.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;
		let mut authorizations = self.authorizations.lock().unwrap();
		authorizations.insert(&id.clone().into(), &output.tokens, true);
		for (child, entry) in &output.children {
			authorizations.insert(&child.clone().into(), &entry.tokens, true);
		}
		Ok(output)
	}

	pub(super) async fn object_data(
		&self,
		authority: &Arc<Authority>,
		id: &tg::object::Id,
	) -> std::io::Result<tg::object::Data> {
		let output = self.get_object(authority, id).await?;
		let data = tg::object::Data::deserialize(id.kind(), output.bytes).map_err(eio)?;
		Ok(data)
	}

	pub(super) async fn artifact_data(
		&self,
		artifact: &ArtifactState,
	) -> std::io::Result<tg::artifact::data::Artifact> {
		if let Some(data) = &artifact.data {
			return Ok(data.clone());
		}
		self.object_data(&artifact.authority, &artifact.id.clone().into())
			.await?
			.try_into()
			.map_err(eio)
	}

	pub(super) async fn graph_data(
		&self,
		authority: &Arc<Authority>,
		graph: &tg::graph::Id,
	) -> std::io::Result<tg::graph::Data> {
		self.object_data(authority, &graph.clone().into())
			.await?
			.try_into()
			.map_err(eio)
	}

	pub(super) async fn blob_length(
		&self,
		authority: &Arc<Authority>,
		blob: &tg::blob::Id,
	) -> std::io::Result<u64> {
		let data = self.object_data(authority, &blob.clone().into()).await?;
		match data {
			tg::object::Data::Blob(tg::blob::Data::Branch(branch)) => {
				Ok(branch.children.iter().map(|child| child.length).sum())
			},
			tg::object::Data::Blob(tg::blob::Data::Leaf(leaf)) => {
				Ok(leaf.bytes.len().to_u64().unwrap())
			},
			_ => Err(std::io::Error::other("expected a blob")),
		}
	}
	pub(super) async fn graph_node(
		&self,
		authority: &Arc<Authority>,
		graph: &tg::graph::Id,
		index: usize,
	) -> std::io::Result<Option<tg::graph::data::Node>> {
		let mut current = graph.clone();
		let mut authority = authority.clone();
		let mut offset = index;
		loop {
			authority = Authority::new(current.clone().into(), Some(&authority));
			let data = self.graph_data(&authority, &current).await?;
			let branch = match data {
				tg::graph::Data::Leaf(leaf) => return Ok(leaf.nodes.into_iter().nth(offset)),
				tg::graph::Data::Branch(branch) => branch,
			};
			let mut child_graph = None;
			for child in branch.children {
				let count = child.count.to_usize().unwrap();
				if offset < count {
					child_graph = Some(child.graph);
					break;
				}
				offset -= count;
			}
			let Some(child_graph) = child_graph else {
				return Ok(None);
			};
			current = child_graph;
		}
	}

	pub(super) async fn resolve_graph_node(
		&self,
		authority: &Arc<Authority>,
		pointer: &tg::graph::data::Pointer,
	) -> std::io::Result<(tg::graph::data::Node, tg::graph::Id)> {
		let graph = pointer.graph.clone();
		let node = self
			.graph_node(authority, &graph, pointer.index)
			.await?
			.ok_or_else(|| {
				tracing::error!(graph = %graph, pointer = ?pointer, "invalid graph node");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		if node.kind() != pointer.kind {
			tracing::error!(
				graph = %graph,
				pointer = ?pointer,
				kind = ?node.kind(),
				"invalid pointer kind"
			);
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		}
		Ok((node, graph))
	}

	pub(super) async fn directory_node(
		&self,
		directory: &ArtifactState,
	) -> std::io::Result<(tg::graph::data::Directory, Option<tg::graph::Id>)> {
		let authority = &directory.authority;
		let data = self.artifact_data(directory).await?;
		let tg::artifact::data::Artifact::Directory(directory) = data else {
			tracing::error!("expected directory data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match directory {
			tg::directory::Data::Node(node) => Ok((node, None)),
			tg::directory::Data::Pointer(pointer) => {
				let (node, graph) = self.resolve_graph_node(authority, &pointer).await?;
				let tg::graph::data::Node::Directory(node) = node else {
					tracing::error!(pointer = ?pointer, "expected a directory node in the graph");
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				Ok((node, Some(graph)))
			},
		}
	}

	pub(super) async fn file_node(
		&self,
		file: &ArtifactState,
	) -> std::io::Result<(tg::graph::data::File, Option<tg::graph::Id>)> {
		let authority = &file.authority;
		let data = self.artifact_data(file).await?;
		let tg::artifact::data::Artifact::File(file) = data else {
			tracing::error!("expected file data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match file {
			tg::file::Data::Node(node) => Ok((node, None)),
			tg::file::Data::Pointer(pointer) => {
				let (node, graph) = self.resolve_graph_node(authority, &pointer).await?;
				let tg::graph::data::Node::File(node) = node else {
					tracing::error!(pointer = ?pointer, "expected a file node in the graph");
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				Ok((node, Some(graph)))
			},
		}
	}

	pub(super) async fn symlink_node(
		&self,
		symlink: &ArtifactState,
	) -> std::io::Result<(tg::graph::data::Symlink, Option<tg::graph::Id>)> {
		let authority = &symlink.authority;
		let data = self.artifact_data(symlink).await?;
		let tg::artifact::data::Artifact::Symlink(symlink) = data else {
			tracing::error!("expected symlink data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match symlink {
			tg::symlink::Data::Node(node) => Ok((node, None)),
			tg::symlink::Data::Pointer(pointer) => {
				let (node, graph) = self.resolve_graph_node(authority, &pointer).await?;
				let tg::graph::data::Node::Symlink(node) = node else {
					tracing::error!(pointer = ?pointer, "expected a symlink node in the graph");
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				Ok((node, Some(graph)))
			},
		}
	}

	pub(super) async fn directory_entries(
		&self,
		directory: &ArtifactState,
	) -> std::io::Result<BTreeMap<String, ArtifactState>> {
		let mut entries = BTreeMap::new();
		let mut stack = vec![(directory.clone(), None)];
		while let Some((directory, default_graph)) = stack.pop() {
			let authority = directory.authority.clone();
			let (directory, graph) = self.directory_node(&directory).await?;
			let graph = graph.or(default_graph);
			match directory {
				tg::graph::data::Directory::Branch(branch) => {
					for child in branch.children.into_iter().rev() {
						let artifact = self
							.artifact_from_directory_edge(
								&authority,
								child.directory,
								graph.as_ref(),
							)
							.await?;
						stack.push((artifact, graph.clone()));
					}
				},
				tg::graph::data::Directory::Leaf(leaf) => {
					for (name, edge) in leaf.entries {
						let artifact = self
							.artifact_from_edge(&authority, edge, graph.as_ref())
							.await?;
						entries.insert(name, artifact);
					}
				},
			}
		}
		Ok(entries)
	}

	pub(super) async fn artifact_from_edge(
		&self,
		authority: &Arc<Authority>,
		edge: tg::graph::data::Edge<tg::artifact::Id>,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<ArtifactState> {
		match edge {
			tg::graph::data::Edge::Index(index) => {
				self.artifact_from_index(authority, index, default_graph, None)
					.await
			},
			tg::graph::data::Edge::Object(artifact) => {
				Ok(ArtifactState::new(artifact, Some(authority)))
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				artifact_from_pointer(authority, &pointer, None)
			},
		}
	}

	pub(super) async fn artifact_from_directory_edge(
		&self,
		authority: &Arc<Authority>,
		edge: tg::graph::data::Edge<tg::directory::Id>,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<ArtifactState> {
		match edge {
			tg::graph::data::Edge::Index(index) => {
				self.artifact_from_index(
					authority,
					index,
					default_graph,
					Some(tg::artifact::Kind::Directory),
				)
				.await
			},
			tg::graph::data::Edge::Object(directory) => {
				Ok(ArtifactState::new(directory.into(), Some(authority)))
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				artifact_from_pointer(authority, &pointer, Some(tg::artifact::Kind::Directory))
			},
		}
	}

	pub(super) async fn artifact_from_index(
		&self,
		authority: &Arc<Authority>,
		index: usize,
		graph: Option<&tg::graph::Id>,
		expected_kind: Option<tg::artifact::Kind>,
	) -> std::io::Result<ArtifactState> {
		let graph = graph.ok_or_else(fallback)?;
		let node = self
			.graph_node(authority, graph, index)
			.await?
			.ok_or_else(fallback)?;
		let pointer = tg::graph::data::Pointer {
			graph: graph.clone(),
			index,
			kind: node.kind(),
		};
		artifact_from_pointer(authority, &pointer, expected_kind)
	}

	pub(super) async fn try_get_reference(
		&self,
		reference: &tg::Reference,
	) -> std::io::Result<Option<tg::Referent<tg::get::Node>>> {
		use futures::TryStreamExt as _;
		let session = self.client.session(self.client.context());
		let stream = session
			.try_get(reference, tg::get::Arg::default())
			.await
			.map_err(eio)?;
		let event = stream
			.try_fold(None, |_, event| async { Ok(Some(event)) })
			.await
			.map_err(eio)?;
		let Some(event) = event else {
			return Ok(None);
		};
		let output = event
			.try_unwrap_output()
			.ok()
			.ok_or_else(|| std::io::Error::other("expected the reference output"))?;
		Ok(output.map(|output| output.referent))
	}
}

pub(super) fn artifact_from_pointer(
	authority: &Arc<Authority>,
	pointer: &tg::graph::data::Pointer,
	expected_kind: Option<tg::artifact::Kind>,
) -> std::io::Result<ArtifactState> {
	if expected_kind.is_some_and(|kind| pointer.kind != kind) {
		return Err(std::io::Error::other("invalid pointer kind"));
	}
	let data: tg::artifact::data::Artifact = match pointer.kind {
		tg::artifact::Kind::Directory => tg::directory::Data::Pointer(pointer.clone()).into(),
		tg::artifact::Kind::File => tg::file::Data::Pointer(pointer.clone()).into(),
		tg::artifact::Kind::Symlink => tg::symlink::Data::Pointer(pointer.clone()).into(),
	};
	let bytes = data.serialize().map_err(eio)?;
	let id = tg::artifact::Id::new(pointer.kind, &bytes);
	let graph = Authority::new(pointer.graph.clone().into(), Some(authority));
	let mut artifact = ArtifactState::new(id, Some(&graph));
	artifact.data = Some(data);
	Ok(artifact)
}
