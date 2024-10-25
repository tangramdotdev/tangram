use super::Compiler;
use crate::{
	lockfile::{create_artifact_for_lockfile_node, try_get_lockfile_node_for_module_path},
	util::path::Ext,
};
use std::path::{Path, PathBuf};
use tangram_client as tg;
use tangram_either::Either;

impl Compiler {
	/// Resolve an import from a module.
	pub async fn resolve_module(
		&self,
		referrer: Option<&tg::Module>,
		import: &tg::Import,
	) -> tg::Result<tg::Module> {
		let kind = import.kind;

		// Get the referent.
		let referent = match referrer {
			// Handle an import with no referrer.
			None => {
				let object = import
					.reference
					.item()
					.try_unwrap_object_ref()
					.ok()
					.ok_or_else(|| tg::error!("an import with no referrer must specify an object"))?
					.clone();
				let item = tg::module::Item::Object(object);
				let subpath = import
					.reference
					.options()
					.and_then(|query| query.subpath.clone());
				let tag = None;
				tg::Referent { item, subpath, tag }
			},

			// Handle a path referrer.
			Some(tg::Module {
				referent:
					tg::Referent {
						item: tg::module::Item::Path(path),
						subpath,
						..
					},
				..
			}) => {
				// Get the path of the referrer.
				let referrer_path = subpath
					.as_ref()
					.map_or(path.to_owned(), |subpath| path.join(subpath));

				// Get the path of the import, if it exists.
				let import_path = import
					.reference
					.item()
					.try_unwrap_path_ref()
					.ok()
					.or_else(|| import.reference.options()?.path.as_ref());
				if let Some(import_path) = import_path {
					// Canonicalize the import path.
					let module_path = referrer_path.join(import_path);
					let module_path = tokio::fs::canonicalize(&module_path)
						.await
						.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;

					// Check if this is in the same module.
					let subpath = module_path.diff(path).unwrap();
					if subpath.starts_with("..") {
						'a: {
							// Find the root module.
							for ancestor in module_path.ancestors().skip(1) {
								let root_module_path = tg::package::try_get_root_module_file_name(
									&self.server,
									Either::Right(ancestor),
								)
								.await?;
								if root_module_path.is_some() {
									let item = tg::module::Item::Path(ancestor.to_owned());
									let subpath = Some(ancestor.diff(&module_path).unwrap());
									let tag = None;
									break 'a tg::Referent { item, subpath, tag };
								}
							}
							return Err(tg::error!("failed to resolve the module"));
						}
					} else {
						let item = tg::module::Item::Path(path.clone());
						let subpath = Some(subpath);
						let tag = None;
						tg::Referent { item, subpath, tag }
					}
				} else {
					// Find the module within an existing lockfile.
					let (lockfile, node) = try_get_lockfile_node_for_module_path(&referrer_path)
						.await?
						.ok_or_else(
							|| tg::error!(%referrer = referrer_path.display(), "failed to find lockfile for referrer"),
						)?;
					let tg::lockfile::Node::File { dependencies, .. } = &lockfile.nodes[node]
					else {
						return Err(tg::error!("expected a file node"));
					};

					// Resolve using the lockfile.
					let referent = dependencies.get(&import.reference)
						.ok_or_else(|| tg::error!(%reference = import.reference, %referrer = referrer_path.display(), "failed to resolve import"))?;

					let object = match &referent.item {
						Either::Left(node) => {
							create_artifact_for_lockfile_node(&self.server, &lockfile, *node)
								.await?
								.id(&self.server)
								.await?
								.into()
						},
						Either::Right(id) => id.clone(),
					};

					let item = tg::module::Item::Object(object);
					tg::Referent {
						item,
						subpath: referent.subpath.clone(),
						tag: referent.tag.clone(),
					}
				}
			},

			// Handle an object referrer.
			Some(tg::Module {
				referent:
					tg::Referent {
						item: tg::module::Item::Object(object),
						subpath,
						..
					},
				..
			}) => {
				let object = if let Some(subpath) = subpath {
					let tg::object::Id::Directory(directory) = object else {
						return Err(tg::error!("object with subpath must be a directory"));
					};
					let directory = tg::Directory::with_id(directory.clone());
					let artifact = directory.get(&self.server, subpath).await?;
					artifact.id(&self.server).await?.clone().into()
				} else {
					object.clone()
				};
				let file = object
					.try_unwrap_file()
					.ok()
					.ok_or_else(|| tg::error!("the referrer must be a file"))?;
				let file = tg::File::with_id(file.clone());
				let referent = file.get_dependency(&self.server, &import.reference).await?;
				let object = referent.item.id(&self.server).await?.clone();
				let item = tg::module::Item::Object(object);
				let subpath = referent.subpath;
				let tag = referent.tag;
				tg::Referent { item, subpath, tag }
			},
		};

		// If the kind is not known and the referent is a directory with a root module, then use its kind.
		let kind = if kind.is_some() {
			kind
		} else {
			match &referent.item {
				tg::module::Item::Path(path) => {
					let path = if let Some(subpath) = &referent.subpath {
						path.join(subpath)
					} else {
						path.clone()
					};
					let metadata = tokio::fs::metadata(&path)
						.await
						.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
					if metadata.is_dir() {
						let package = Either::Right(path.as_ref());
						if let Some(name) =
							tg::package::try_get_root_module_file_name(&self.server, package)
								.await?
						{
							let name = Path::new(name);
							let extension = name.extension();
							if extension.is_some_and(|extension| extension == "js") {
								Some(tg::module::Kind::Js)
							} else if extension.is_some_and(|extension| extension == "ts") {
								Some(tg::module::Kind::Ts)
							} else {
								None
							}
						} else {
							None
						}
					} else {
						None
					}
				},

				tg::module::Item::Object(object) => {
					let object = if let Some(subpath) = &referent.subpath {
						let object = tg::Object::with_id(object.clone());
						let directory = object
							.try_unwrap_directory_ref()
							.ok()
							.ok_or_else(|| tg::error!("expected a directory"))?;
						let artifact = directory.get(&self.server, subpath).await?;
						let artifact = artifact.id(&self.server).await?.clone();
						artifact.into()
					} else {
						object.clone()
					};
					if object.try_unwrap_directory_ref().is_ok() {
						let object = tg::Object::with_id(object);
						let package = Either::Left(&object);
						if let Some(name) =
							tg::package::try_get_root_module_file_name(&self.server, package)
								.await?
						{
							let name = Path::new(name);
							let extension = name.extension();
							if extension.is_some_and(|extension| extension == "js") {
								Some(tg::module::Kind::Js)
							} else if extension.is_some_and(|extension| extension == "ts") {
								Some(tg::module::Kind::Ts)
							} else {
								None
							}
						} else {
							None
						}
					} else {
						None
					}
				},
			}
		};

		// If the kind is not known, then try to infer it from the path extension.
		let kind = if let Some(kind) = kind {
			Some(kind)
		} else if let Some(subpath) = &referent.subpath {
			let extension = subpath.extension();
			if extension.is_some_and(|extension| extension == "js") {
				Some(tg::module::Kind::Js)
			} else if extension.is_some_and(|extension| extension == "ts") {
				Some(tg::module::Kind::Ts)
			} else {
				None
			}
		} else if let tg::module::Item::Path(path) = &referent.item {
			let extension = path.extension();
			if extension.is_some_and(|extension| extension == "js") {
				Some(tg::module::Kind::Js)
			} else if extension.is_some_and(|extension| extension == "ts") {
				Some(tg::module::Kind::Ts)
			} else {
				None
			}
		} else {
			None
		};

		// Finally, if the kind is not known, then infer it from the object's kind.
		let kind = if let Some(kind) = kind {
			kind
		} else {
			match &referent.item {
				tg::module::Item::Path(path) => {
					let path = if let Some(subpath) = &referent.subpath {
						path.join(subpath)
					} else {
						path.clone()
					};
					let metadata = tokio::fs::symlink_metadata(&path)
						.await
						.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
					if metadata.is_dir() {
						tg::module::Kind::Directory
					} else if metadata.is_file() {
						tg::module::Kind::File
					} else if metadata.is_symlink() {
						tg::module::Kind::Symlink
					} else {
						return Err(tg::error!("expected a directory, file, or symlink"));
					}
				},

				tg::module::Item::Object(object) => {
					let object = if let Some(subpath) = &referent.subpath {
						let object = tg::Object::with_id(object.clone());
						let directory = object
							.try_unwrap_directory_ref()
							.ok()
							.ok_or_else(|| tg::error!("expected a directory"))?;
						let artifact = directory.get(&self.server, subpath).await?;
						let artifact = artifact.id(&self.server).await?.clone();
						artifact.into()
					} else {
						object.clone()
					};
					match &object {
						tg::object::Id::Leaf(_) => tg::module::Kind::Leaf,
						tg::object::Id::Branch(_) => tg::module::Kind::Branch,
						tg::object::Id::Directory(_) => tg::module::Kind::Directory,
						tg::object::Id::File(_) => tg::module::Kind::File,
						tg::object::Id::Symlink(_) => tg::module::Kind::Symlink,
						tg::object::Id::Graph(_) => tg::module::Kind::Graph,
						tg::object::Id::Target(_) => tg::module::Kind::Target,
					}
				},
			}
		};

		// Create the module.
		let module = tg::Module { kind, referent };

		Ok(module)
	}

	async fn resolve_module_at_path(
		&self,
		package: &Path,
		referrer: &Path,
		import: &tg::Import,
	) -> tg::Result<tg::Referent<tg::module::Item>> {
		// Get the referrer within some lockfile.
		let module_path = package.join(referrer);

		// Get the lockfile and its path.
		let (lockfile_path, lockfile) = 'a: {
			for ancestor in module_path.ancestors().skip(1) {
				let lockfile_path = ancestor.join(tg::package::LOCKFILE_FILE_NAME);
				let exists = tokio::fs::try_exists(&lockfile_path).await.map_err(
					|source| tg::error!(!source, %package = ancestor.display(), "failed to check if lockfile exists"),
				)?;
				if exists {
					let contents = tokio::fs::read_to_string(&lockfile_path).await.map_err(
						|source| tg::error!(!source, %path = lockfile_path.display(), "failed to read lockfile"),
					)?;
					let lockfile = serde_json::from_str::<tg::Lockfile>(&contents).map_err(
						|source| tg::error!(!source, %path = lockfile_path.display(), "failed to deserialize lockfile"),
					)?;
					break 'a (lockfile_path, lockfile);
				}
			}
			return Err(tg::error!(%module = module_path.display(), "failed to find lockfile"));
		};

		// Find the referrer in the lockfile.
		let module_index = self
			.find_node_in_lockfile(&lockfile_path, &lockfile, &module_path)
			.await?;
		let tg::lockfile::Node::File { dependencies, .. } = &lockfile.nodes[module_index] else {
			return Err(
				tg::error!(%lockfile = lockfile_path.display(), %node = module_index, "expected a file node"),
			);
		};

		// Get the import path.
		let import_path = import
			.reference
			.item()
			.try_unwrap_path_ref()
			.ok()
			.or_else(|| import.reference.options()?.path.as_ref());

		match dependencies.get(&import.reference) {
			// If this points to another node in the lockfile,
			Some(tg::Referent {
				item: Either::Left(index),
				subpath,
				..
			}) => {
				// let (package, subpath) = self.
				todo!()
			},

			// Resolve objects normally.
			Some(tg::Referent {
				item: Either::Right(object),
				subpath,
				tag,
			}) => Ok(tg::Referent {
				item: tg::module::Item::Object(object.clone()),
				subpath: subpath.clone(),
				tag: tag.clone(),
			}),

			None => {
				// If this is a path dependency but not in the dependencies table for the node, then it is an artifact dependency.
				if let Some(import_path) = import_path {
					let item = module_path.join(import_path);
					return Ok(tg::Referent {
						item: tg::module::Item::Path(item),
						subpath: import
							.reference
							.options()
							.and_then(|query| query.subpath.clone()),
						tag: None,
					});
				};

				// Otherwise we cannot resolve this reference.
				Err(
					tg::error!(%module = module_path.display(), %import = import.reference, "failed to resolve import"),
				)
			},
		}
	}

	async fn find_node_in_lockfile(
		&self,
		lockfile_path: &Path,
		lockfile: &tg::Lockfile,
		path: &Path,
	) -> tg::Result<usize> {
		todo!()
	}

	async fn find_package_path_in_lockfile(
		&self,
		search_node: usize,
		lockfile_path: &Path,
		lockfile: &tg::Lockfile,
	) -> tg::Result<PathBuf> {
		todo!()
	}
}
