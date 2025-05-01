use super::Compiler;
use std::path::Path;
use tangram_client as tg;
use tangram_either::Either;

#[cfg(test)]
mod tests;

impl Compiler {
	/// Resolve an import from a module.
	pub async fn resolve_module(
		&self,
		referrer: &tg::module::Data,
		import: &tg::module::Import,
	) -> tg::Result<tg::module::Data> {
		let kind = import.kind;

		// Get the referent.
		let referent = match referrer {
			// Handle a path referrer.
			tg::module::Data {
				referent:
					tg::Referent {
						item: tg::module::data::Item::Path(path),
						subpath,
						..
					},
				..
			} => {
				self.resolve_with_path_referrer(path, subpath.as_deref(), import)
					.await?
			},

			// Handle an object referrer.
			tg::module::Data {
				referent:
					tg::Referent {
						item: tg::module::data::Item::Object(object),
						subpath,
						..
					},
				..
			} => {
				self.resolve_module_with_object_referrer(object, subpath.as_deref(), import)
					.await?
			},
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
		} else if let tg::module::data::Item::Path(path) = &referent.item {
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
				tg::module::data::Item::Path(path) => {
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

				tg::module::data::Item::Object(object) => {
					let object = if let Some(subpath) = &referent.subpath {
						let object = tg::Object::with_id(object.clone());
						let directory = object
							.try_unwrap_directory_ref()
							.ok()
							.ok_or_else(|| tg::error!("expected a directory"))?;
						let artifact =
							directory
								.get(&self.server, subpath)
								.await
								.map_err(|source| {
									tg::error!(!source, "failed to get the directory entry")
								})?;
						let artifact = artifact.id(&self.server).await?.clone();
						artifact.into()
					} else {
						object.clone()
					};
					match &object {
						tg::object::Id::Blob(_) => tg::module::Kind::Blob,
						tg::object::Id::Directory(_) => tg::module::Kind::Directory,
						tg::object::Id::File(_) => tg::module::Kind::File,
						tg::object::Id::Symlink(_) => tg::module::Kind::Symlink,
						tg::object::Id::Graph(_) => tg::module::Kind::Graph,
						tg::object::Id::Command(_) => tg::module::Kind::Command,
					}
				},
			}
		};

		// Create the module.
		let module = tg::module::Data { kind, referent };

		Ok(module)
	}

	async fn resolve_with_path_referrer(
		&self,
		referrer: &Path,
		subpath: Option<&Path>,
		import: &tg::module::Import,
	) -> tg::Result<tg::Referent<tg::module::data::Item>> {
		// Get the referrer within some lockfile.
		let subpath = subpath.unwrap_or("".as_ref());
		let module_path = referrer.join(subpath);

		// Get the lockfile and its path.
		let (lockfile_path, lockfile) = 'a: {
			// Search the ancestors for a lockfile, if it exists.
			for ancestor in module_path.ancestors().skip(1) {
				// Check if the lockfile exists.
				let lockfile_path = ancestor.join(tg::package::LOCKFILE_FILE_NAME);
				let exists = tokio::fs::try_exists(&lockfile_path).await.map_err(
					|source| tg::error!(!source, %package = ancestor.display(), "failed to check if the lockfile exists"),
				)?;
				if !exists {
					continue;
				}

				// Parse the lockfile.
				let contents = tokio::fs::read_to_string(&lockfile_path).await.map_err(
					|source| tg::error!(!source, %path = lockfile_path.display(), "failed to read the lockfile"),
				)?;
				let lockfile = serde_json::from_str::<tg::Lockfile>(&contents).map_err(
					|source| tg::error!(!source, %path = lockfile_path.display(), "failed to deserialize the lockfile"),
				)?;
				break 'a (lockfile_path, lockfile);
			}

			// Error if no lockfile is found.
			return Err(tg::error!(%module = module_path.display(), "failed to find the lockfile"));
		};

		// Find the referrer in the lockfile.
		let module_index = self
			.server
			.find_node_index_in_lockfile(&module_path, &lockfile_path, &lockfile)
			.await?;

		// The module within the lockfile must be a file for it to have imports.
		let tg::lockfile::Node::File(tg::lockfile::File { dependencies, .. }) =
			&lockfile.nodes[module_index]
		else {
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
			// If this points to another node in the lockfile, find it within the lockfile.
			Some(tg::Referent {
				item: Either::Left(index),
				path,
				subpath,
				tag,
			}) => {
				if let Ok(package_path) = self
					.server
					.find_path_in_lockfile(*index, &lockfile_path, &lockfile)
					.await
				{
					Ok(tg::Referent {
						item: tg::module::data::Item::Path(package_path),
						subpath: subpath.clone(),
						path: path.clone(),
						tag: tag.clone(),
					})
				} else if let Some(id) = lockfile.nodes[*index].id() {
					Ok(tg::Referent {
						item: tg::module::data::Item::Object(id.into()),
						subpath: subpath.clone(),
						path: path.clone(),
						tag: tag.clone(),
					})
				} else {
					Err(
						tg::error!(%referrer = referrer.display(), %reference = import.reference, "failed to resolve import"),
					)
				}
			},

			// Resolve objects normally.
			Some(tg::Referent {
				item: Either::Right(object),
				path,
				subpath,
				tag,
			}) => Ok(tg::Referent {
				item: tg::module::data::Item::Object(object.clone()),
				path: path.clone(),
				subpath: subpath.clone(),
				tag: tag.clone(),
			}),

			None => {
				// If this is a path dependency but not in the dependencies table for the node, then it is an artifact dependency.
				if let Some(import_path) = import_path {
					let Some(module_parent) = module_path.parent() else {
						return Err(
							tg::error!(%path = module_path.display(), "failed to get parent of module_path"),
						);
					};
					let item = module_parent.join(import_path);
					return Ok(tg::Referent {
						item: tg::module::data::Item::Path(item),
						path: None,
						subpath: import
							.reference
							.options()
							.and_then(|options| options.subpath.clone()),
						tag: None,
					});
				}

				// Otherwise we cannot resolve this reference.
				Err(
					tg::error!(%module = module_path.display(), %import = import.reference, "failed to resolve import"),
				)
			},
		}
	}

	async fn resolve_module_with_object_referrer(
		&self,
		object: &tg::object::Id,
		subpath: Option<&Path>,
		import: &tg::module::Import,
	) -> Result<tg::Referent<tg::module::data::Item>, tg::Error> {
		let referrer = if let Some(subpath) = subpath {
			let tg::object::Id::Directory(directory) = object else {
				return Err(tg::error!("object with subpath must be a directory"));
			};
			let artifact = tg::Directory::with_id(directory.clone()).get(&self.server, subpath).await.map_err(|source| tg::error!(!source, %directory, %subpath = subpath.display(), "failed to get directory entry"))?;
			artifact.into()
		} else {
			tg::Object::with_id(object.clone())
		};
		let file = referrer
			.clone()
			.try_unwrap_file()
			.ok()
			.ok_or_else(|| tg::error!(%object, "the referrer must be a file"))?;
		let referent = file.get_dependency(&self.server, &import.reference).await?;
		let object = referent.item.id(&self.server).await?.clone();
		let item = tg::module::data::Item::Object(object);
		let path = referent.path;
		let subpath = referent.subpath;
		let tag = referent.tag;
		Ok(tg::Referent {
			item,
			path,
			subpath,
			tag,
		})
	}
}
