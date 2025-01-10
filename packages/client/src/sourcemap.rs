use crate as tg;
use std::{collections::BTreeMap, sync::Arc};

pub struct SourceMap {
	objects: BTreeMap<tg::object::Id, tg::Referent<tg::Object>>,
}

impl SourceMap {
	pub async fn new<H>(handle: &H, lockfile: &tg::Lockfile, object: tg::Object) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let edge = tg::Referent {
			item: object.clone(),
			path: Some(".".into()),
			subpath: None,
			tag: None,
		};
		let mut stack = vec![(edge, 0)];
		let mut visited = vec![false; lockfile.nodes.len()];
		let mut objects = BTreeMap::new();
		while let Some((edge, node)) = stack.pop() {
			if visited[node] {
				continue;
			}
			visited[node] = true;
			objects.insert(edge.item.id(handle).await?, edge.clone());

			let edges = get_edges(handle, &edge.item, node, lockfile).await?;
			stack.extend(edges);
		}
		Ok(Self { objects })
	}

	pub fn lookup(&self, object: &tg::object::Id) -> Option<tg::Referent<tg::Object>> {
		self.objects.get(object).cloned()
	}

	pub fn convert_diagnostic(&self, mut diagnostic: tg::Diagnostic) -> tg::Diagnostic {
		diagnostic.location = diagnostic.location.map(|mut location| {
			location.module = self.convert_module(location.module);
			location
		});
		diagnostic
	}

	pub fn convert_error(&self, mut error: tg::Error) -> tg::Error {
		if let Some(location) = error.location.as_mut() {
			if let tg::error::Source::Module(module) = &mut location.source {
				*module = self.convert_module(module.clone());
			}
		}
		if let Some(source) = error.source.as_ref() {
			let source = self.convert_error(source.as_ref().clone());
			error.source.replace(Arc::new(source));
		}
		error
	}

	pub fn convert_module(&self, mut module: tg::Module) -> tg::Module {
		let tg::module::Item::Object(object) = &module.referent.item else {
			return module;
		};
		let Some(referent) = self.lookup(object) else {
			return module;
		};
		module.referent.path = referent.path;
		module.referent.tag = referent.tag;
		module
	}
}

async fn get_edges(
	handle: &impl tg::Handle,
	object: &tg::Object,
	node: usize,
	lockfile: &tg::Lockfile,
) -> tg::Result<Vec<(tg::Referent<tg::Object>, usize)>> {
	match (&lockfile.nodes[node], object) {
		(tg::lockfile::Node::Directory(node), tg::Object::Directory(object)) => {
			let entries = object.entries(handle).await?;
			let edges = node
				.entries
				.iter()
				.filter_map(|(name, value)| {
					let node = value.as_ref().left().copied()?;
					let item = entries.get(name)?.clone().into();
					let path = None;
					let subpath = None;
					let tag = None;
					let referent = tg::Referent {
						item,
						path,
						tag,
						subpath,
					};
					Some((referent, node))
				})
				.collect();
			Ok(edges)
		},
		(tg::lockfile::Node::File(node), tg::Object::File(object)) => {
			let dependencies = object.dependencies(handle).await?;
			let edges = node
				.dependencies
				.iter()
				.filter_map(|(reference, referent)| {
					let node = referent.item.as_ref().left().copied()?;
					let item = dependencies.get(reference)?.item.clone();
					let path = referent.path.clone();
					let subpath = referent.subpath.clone();
					let tag = referent.tag.clone();
					let referent = tg::Referent {
						item,
						path,
						subpath,
						tag,
					};
					Some((referent, node))
				})
				.collect();
			Ok(edges)
		},
		(
			tg::lockfile::Node::Symlink(tg::lockfile::Symlink::Artifact { artifact, .. }),
			tg::Object::Symlink(object),
		) => {
			let Some(node) = artifact.as_ref().left().copied() else {
				return Ok(Vec::new());
			};
			let item = object
				.artifact(handle)
				.await?
				.ok_or_else(|| tg::error!("expected an artifact"))?
				.into();
			let path = None;
			let subpath = None;
			let tag = None;
			let referent = tg::Referent {
				item,
				path,
				tag,
				subpath,
			};
			Ok(vec![(referent, node)])
		},
		_ => Ok(Vec::new()),
	}
}
