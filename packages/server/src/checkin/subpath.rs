use {
	crate::{Server, checkin::Graph},
	std::path::{Path, PathBuf},
	tangram_client as tg,
};

#[derive(Clone)]
pub struct Subpath {
	pub index: usize,
	pub reference: tg::Reference,
}

impl Server {
	pub(super) async fn checkin_resolve_subpaths(
		&self,
		subpaths: &[Subpath],
		graph: &mut Graph,
	) -> tg::Result<()> {
		let old_graph = graph.clone();
		for subpath in subpaths {
			let path = subpath.reference.options().path.as_ref().unwrap();
			let node = &mut graph.nodes[&subpath.index];
			let file = node
				.variant
				.try_unwrap_file_mut()
				.map_err(|_| tg::error!("expected a file"))?;
			let Some(dependency) = file
				.dependencies
				.get_mut(&subpath.reference)
				.ok_or_else(|| tg::error!("expected a dependency"))?
			else {
				continue;
			};
			let Some(item) = dependency.item.take() else {
				continue;
			};
			let fixed = match item {
				tg::graph::data::Edge::Object(object) => {
					self.checkin_resolve_subpath_with_object(object, path)
						.await?
				},
				tg::graph::data::Edge::Pointer(pointer) => {
					self.checkin_resolve_subpath_with_pointer(pointer, path, &old_graph)
						.await?
				},
			};
			// Should this be allowed?
			let resolved = fixed.ok_or_else(|| tg::error!("expected an item at the path"))?;
			dependency.item.replace(resolved);
			dependency.options.path.replace(path.clone());
			if let Ok(id) = subpath.reference.item().try_unwrap_object_ref()
				&& subpath.reference.options().local.is_none()
				&& dependency.options.id.is_none()
			{
				dependency.options.id.replace(id.clone());
			}
		}
		Ok(())
	}

	async fn checkin_resolve_subpath_with_pointer(
		&self,
		mut pointer: tg::graph::data::Pointer,
		path: &Path,
		graph: &Graph,
	) -> tg::Result<Option<tg::graph::data::Edge<tg::object::Id>>> {
		let mut stack = vec![];
		let mut components = path.components().peekable();
		while let Some(component) = components.next() {
			match component {
				std::path::Component::CurDir => (),
				std::path::Component::ParentDir => {
					let Some((index, kind)) = stack.pop() else {
						return Ok(None);
					};
					pointer.index = index;
					pointer.kind = kind;
				},
				std::path::Component::Normal(component) => {
					let name = component
						.to_str()
						.ok_or_else(|| tg::error!(path = %path.display(), "invalid path"))?;
					let edge = if let Some(graph) = &pointer.graph {
						let graph = tg::Graph::with_id(graph.clone());
						let nodes = graph.nodes(self).await?;
						let node = nodes
							.get(pointer.index)
							.ok_or_else(|| tg::error!("invalid graph pointer"))?;
						let directory = node
							.try_unwrap_directory_ref()
							.map_err(|_| tg::error!("expected a directory"))?;
						let leaf = directory
							.try_unwrap_leaf_ref()
							.map_err(|_| tg::error!("expected a leaf directory"))?;
						let Some(entry) = leaf.entries.get(name) else {
							return Ok(None);
						};
						match entry {
							tg::graph::Edge::Object(object) => {
								let id: tg::object::Id = object.id().into();
								tg::graph::data::Edge::Object(id)
							},
							tg::graph::Edge::Pointer(p) => {
								tg::graph::data::Edge::Pointer(p.to_data())
							},
						}
					} else {
						let directory = graph.nodes[&pointer.index]
							.variant
							.try_unwrap_directory_ref()
							.map_err(|_| tg::error!("expected a directory"))?;
						let Some(edge) = directory.entries.get(name) else {
							return Ok(None);
						};
						edge.clone().into()
					};
					match edge {
						tg::graph::data::Edge::Object(id) => {
							if components.peek().is_none() {
								return Ok(Some(tg::graph::data::Edge::Object(id.clone())));
							}
							let directory = id.clone();
							let path = components.collect::<PathBuf>();
							return self
								.checkin_resolve_subpath_with_object(directory, &path)
								.await;
						},
						tg::graph::data::Edge::Pointer(p) => {
							stack.push((pointer.index, pointer.kind));
							pointer.index = p.index;
							pointer.kind = p.kind;
							if let Some(graph) = &p.graph {
								pointer.graph.replace(graph.clone());
							}
						},
					}
				},
				_ => return Err(tg::error!(path = %path.display(), "invalid path")),
			}
		}
		Ok(Some(tg::graph::data::Edge::Pointer(pointer)))
	}

	async fn checkin_resolve_subpath_with_object(
		&self,
		object: tg::object::Id,
		path: &Path,
	) -> tg::Result<Option<tg::graph::data::Edge<tg::object::Id>>> {
		let directory = object
			.try_unwrap_directory()
			.map_err(|_| tg::error!("expected a directory"))?;
		let directory = tg::Directory::with_id(directory.clone());
		let child = directory.try_get(self, &path).await.map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to resolve path"),
		)?;
		let edge = child.map(|artifact| tg::graph::data::Edge::Object(artifact.id().into()));
		Ok(edge)
	}
}
