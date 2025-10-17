pub use self::{
	data::Object as Data, handle::Object as Handle, id::Id, kind::Kind, metadata::Metadata,
	object::Object, state::State,
};
use crate as tg;
use futures::future;
use std::{collections::HashSet, future::Future};

pub mod data;
pub mod get;
pub mod handle;
pub mod id;
pub mod kind;
pub mod metadata;
#[allow(clippy::module_inception)]
pub mod object;
pub mod put;
pub mod state;
pub mod touch;

/// Visit each object in an object graph once. Note: this is _not_ the same as walking the object tree, since it will also construct objects that point into graph objects to visit each of them.
pub async fn visit<H, V>(
	handle: &H,
	visitor: &mut V,
	objects: impl IntoIterator<Item = tg::Object>,
) -> tg::Result<()>
where
	H: tg::Handle,
	V: tg::object::Visitor<H>,
{
	// Stack for DFS.
	let mut stack = objects.into_iter().collect::<Vec<_>>();

	// Keep track of objects we've visited already.
	let mut visited = HashSet::<_, fnv::FnvBuildHasher>::default();

	// Recursive DFS.
	while let Some(object) = stack.pop() {
		// Don't visit objects more than once.
		if !visited.insert(object.id()) {
			continue;
		}

		// Apply the visitor method.
		match &object {
			tg::Object::Blob(blob) => visitor.visit_blob(handle, blob).await?,
			tg::Object::Directory(directory) => visitor.visit_directory(handle, directory).await?,
			tg::Object::File(file) => visitor.visit_file(handle, file).await?,
			tg::Object::Symlink(symlink) => visitor.visit_symlink(handle, symlink).await?,
			tg::Object::Graph(graph) => visitor.visit_graph(handle, graph).await?,
			tg::Object::Command(command) => visitor.visit_command(handle, command).await?,
		}

		// For graphs we want to visit the objects that point into it as well. The visited set will make sure we don't visit the same graph twice.
		if let tg::Object::Graph(graph) = &object {
			let children =
				graph
					.nodes(handle)
					.await?
					.into_iter()
					.enumerate()
					.map(|(index, node)| match node.kind() {
						tg::artifact::Kind::Directory => {
							tg::Directory::with_graph_and_node(graph.clone(), index).into()
						},
						tg::artifact::Kind::File => {
							tg::File::with_graph_and_node(graph.clone(), index).into()
						},
						tg::artifact::Kind::Symlink => {
							tg::Symlink::with_graph_and_node(graph.clone(), index).into()
						},
					});
			stack.extend(children);
		} else {
			// For everything else just visit the children.
			stack.extend(object.children(handle).await?.into_iter());
		}

		// Unload the object since we probably don't need it anymore.
		object.unload();
	}
	Ok(())
}

#[allow(unused_variables)]
pub trait Visitor<H>
where
	H: tg::Handle,
{
	fn visit_blob(&mut self, handle: &H, blob: &tg::Blob) -> impl Future<Output = tg::Result<()>> {
		future::ok(())
	}

	fn visit_directory(
		&mut self,
		handle: &H,
		directory: &tg::Directory,
	) -> impl Future<Output = tg::Result<()>> {
		future::ok(())
	}

	fn visit_file(&mut self, handle: &H, file: &tg::File) -> impl Future<Output = tg::Result<()>> {
		future::ok(())
	}

	fn visit_symlink(
		&mut self,
		handle: &H,
		symlink: &tg::Symlink,
	) -> impl Future<Output = tg::Result<()>> {
		future::ok(())
	}

	fn visit_graph(
		&mut self,
		handle: &H,
		graph: &tg::Graph,
	) -> impl Future<Output = tg::Result<()>> {
		future::ok(())
	}

	fn visit_command(
		&mut self,
		handle: &H,
		command: &tg::Command,
	) -> impl Future<Output = tg::Result<()>> {
		future::ok(())
	}
}
