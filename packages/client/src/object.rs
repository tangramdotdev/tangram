use {
	crate::prelude::*,
	futures::future,
	std::{collections::HashSet, future::Future},
};

pub use self::{
	data::Object as Data, handle::Object as Handle, id::Id, kind::Kind, metadata::Metadata,
	object::Object, state::State,
};

pub mod data;
pub mod get;
pub mod handle;
pub mod id;
pub mod kind;
pub mod metadata;
#[expect(clippy::module_inception)]
pub mod object;
pub mod put;
pub mod state;
pub mod touch;

/// Visit each object in an object graph once. Note: this is _not_ the same as walking the object tree, since it will also construct objects that point into graph objects to visit each of them.
pub async fn visit<H, V>(
	handle: &H,
	visitor: &mut V,
	object: &tg::Referent<tg::Object>,
) -> tg::Result<()>
where
	H: tg::Handle,
	V: tg::object::Visitor<H>,
{
	// Stack for DFS.
	let mut stack: Vec<crate::Referent<Handle>> = vec![object.clone()];

	// Keep track of objects we've visited already.
	let mut visited = HashSet::new();

	// Recursive DFS.
	while let Some(referent) = stack.pop() {
		// Don't visit referents more than once.
		if !visited.insert(referent.clone().map(|r| r.id())) {
			continue;
		}

		// Apply the visitor method.
		let object = &referent.item;
		let recurse = match &object {
			tg::Object::Blob(blob) => {
				let referent = referent.clone().map(|_| blob);
				visitor.visit_blob(handle, referent).await?
			},
			tg::Object::Directory(directory) => {
				let referent = referent.clone().map(|_| directory);
				visitor.visit_directory(handle, referent).await?
			},
			tg::Object::File(file) => {
				let referent = referent.clone().map(|_| file);
				visitor.visit_file(handle, referent).await?
			},
			tg::Object::Symlink(symlink) => {
				let referent = referent.clone().map(|_| symlink);
				visitor.visit_symlink(handle, referent).await?
			},
			tg::Object::Graph(graph) => {
				let referent = referent.clone().map(|_| graph);
				visitor.visit_graph(handle, referent).await?
			},
			tg::Object::Command(command) => {
				let referent = referent.clone().map(|_| command);
				visitor.visit_command(handle, referent).await?
			},
		};

		if !recurse {
			object.unload();
			continue;
		}

		// Get the children.
		let children = match object {
			tg::Object::Graph(graph) => graph
				.nodes(handle)
				.await?
				.into_iter()
				.enumerate()
				.map(|(index, node)| {
					let object = match node.kind() {
						tg::artifact::Kind::Directory => {
							tg::Directory::with_graph_and_node(graph.clone(), index).into()
						},
						tg::artifact::Kind::File => {
							tg::File::with_graph_and_node(graph.clone(), index).into()
						},
						tg::artifact::Kind::Symlink => {
							tg::Symlink::with_graph_and_node(graph.clone(), index).into()
						},
					};
					let mut child = tg::Referent::with_item(object);
					child.inherit(&referent);
					child
				})
				.collect::<Vec<_>>(),
			tg::Object::Directory(directory) => directory
				.entries(handle)
				.await?
				.into_iter()
				.map(|(name, object)| tg::Referent {
					item: object.into(),
					options: tg::referent::Options {
						path: Some(
							referent
								.path()
								.map_or_else(|| name.clone().into(), |p| p.join(&name)),
						),
						..tg::referent::Options::default()
					},
				})
				.collect::<Vec<_>>(),
			tg::Object::File(file) => file
				.dependencies(handle)
				.await?
				.into_values()
				.filter_map(|child| {
					let mut child = child?;
					child.inherit(&referent);
					Some(child)
				})
				.collect::<Vec<_>>(),
			object => object
				.children(handle)
				.await?
				.into_iter()
				.map(tg::Referent::with_item)
				.collect::<Vec<_>>(),
		};

		stack.extend(children.into_iter().rev());

		// Unload the object since we probably don't need it anymore.
		object.unload();
	}
	Ok(())
}

#[expect(unused_variables)]
pub trait Visitor<H>
where
	H: tg::Handle,
{
	fn visit_blob(
		&mut self,
		handle: &H,
		blob: tg::Referent<&tg::Blob>,
	) -> impl Future<Output = tg::Result<bool>> {
		future::ok(false)
	}

	fn visit_directory(
		&mut self,
		handle: &H,
		directory: tg::Referent<&tg::Directory>,
	) -> impl Future<Output = tg::Result<bool>> {
		future::ok(false)
	}

	fn visit_file(
		&mut self,
		handle: &H,
		file: tg::Referent<&tg::File>,
	) -> impl Future<Output = tg::Result<bool>> {
		future::ok(false)
	}

	fn visit_symlink(
		&mut self,
		handle: &H,
		symlink: tg::Referent<&tg::Symlink>,
	) -> impl Future<Output = tg::Result<bool>> {
		future::ok(false)
	}

	fn visit_graph(
		&mut self,
		handle: &H,
		graph: tg::Referent<&tg::Graph>,
	) -> impl Future<Output = tg::Result<bool>> {
		future::ok(false)
	}

	fn visit_command(
		&mut self,
		handle: &H,
		command: tg::Referent<&tg::Command>,
	) -> impl Future<Output = tg::Result<bool>> {
		future::ok(false)
	}
}
