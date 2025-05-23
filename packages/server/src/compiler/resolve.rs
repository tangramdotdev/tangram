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
						item: tg::module::data::Item::Path(item),
						tag,
						path,
						subpath,
					},
				..
			} => {
				let referrer = tg::Referent {
					item: item.as_ref(),
					path: path.clone(),
					subpath: subpath.clone(),
					tag: tag.clone(),
				};
				self.resolve_module_with_path_referrer(&referrer, import)
					.await?
			},

			// Handle an object referrer.
			tg::module::Data {
				referent:
					tg::Referent {
						item: tg::module::data::Item::Object(item),
						tag,
						path,
						subpath,
					},
				..
			} => {
				let referrer = tg::Referent {
					item,
					path: path.clone(),
					subpath: subpath.clone(),
					tag: tag.clone(),
				};
				self.resolve_module_with_object_referrer(referrer, import)
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

		// If the kind is still not known, then infer it from the object's kind.
		let kind = if let Some(kind) = kind {
			kind
		} else {
			match &referent.item {
				tg::module::data::Item::Path(path) => {
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

				tg::module::data::Item::Object(object) => match &object {
					tg::object::Id::Blob(_) => tg::module::Kind::Blob,
					tg::object::Id::Directory(_) => tg::module::Kind::Directory,
					tg::object::Id::File(_) => tg::module::Kind::File,
					tg::object::Id::Symlink(_) => tg::module::Kind::Symlink,
					tg::object::Id::Graph(_) => tg::module::Kind::Graph,
					tg::object::Id::Command(_) => tg::module::Kind::Command,
				},
			}
		};

		// Create the module.
		let module = tg::module::Data { kind, referent };

		Ok(module)
	}

	async fn resolve_module_with_path_referrer(
		&self,
		referrer: &tg::Referent<&Path>,
		import: &tg::module::Import,
	) -> tg::Result<tg::Referent<tg::module::data::Item>> {
		// Get the lockfile and its path.
		let (lockfile_path, lockfile) = 'a: {
			// Search the ancestors for a lockfile, if it exists.
			for ancestor in referrer.item.ancestors().skip(1) {
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
			return Err(
				tg::error!(%module = referrer.item.display(), "failed to find the lockfile"),
			);
		};

		// Find the referrer in the lockfile.
		let module_index = self
			.server
			.find_node_index_in_lockfile(referrer.item, &lockfile_path, &lockfile)
			.await?;

		// The module within the lockfile must be a file for it to have imports.
		let file = &lockfile.nodes[module_index]
			.try_unwrap_file_ref()
			.map_err(|_| tg::error!("expected a file node"))?;

		// Try to resolve the dependency in the file.
		if let Some(referent) = file.dependencies.get(&import.reference) {
			match &referent.item {
				Either::Left(index) => {
					if let Some(id) = lockfile.nodes[*index].id() {
						let path = referent.path.clone().or_else(|| referrer.path.clone());
						let tag = referent.tag.clone().or_else(|| referent.tag.clone());
						return Ok(tg::Referent {
							item: tg::module::data::Item::Object(id.into()),
							path,
							tag,
							subpath: referent.subpath.clone(),
						});
					}
				},
				Either::Right(id) => {
					let path = referent.path.clone().or_else(|| referrer.path.clone());
					let tag = referent.tag.clone().or_else(|| referent.tag.clone());
					return Ok(tg::Referent {
						item: tg::module::data::Item::Object(id.clone()),
						path,
						tag,
						subpath: referent.subpath.clone(),
					});
				},
			}
		}

		// If we reach here, we know the item is a path.
		let import_path = import
			.reference
			.path()
			.ok_or_else(|| tg::error!("failed to resolve dependency"))?;

		// We know the referrer is a module, so get its parent and join with the import path.
		let mut item = referrer.item.parent().unwrap().join(import_path);

		// Lookup the root module if necessary.
		if let Ok(Some(root_module_name)) =
			tg::package::try_get_root_module_file_name(&self.server, Either::Right(&item)).await
		{
			item = item.join(root_module_name);
		}

		Ok(tg::Referent {
			item: tg::module::data::Item::Path(item),
			path: None,
			subpath: None,
			tag: None,
		})
	}

	async fn resolve_module_with_object_referrer(
		&self,
		referrer: tg::Referent<&tg::object::Id>,
		import: &tg::module::Import,
	) -> Result<tg::Referent<tg::module::data::Item>, tg::Error> {
		let referrer_object = tg::Object::with_id(referrer.item.clone());
		let file =
			referrer_object.clone().try_unwrap_file().ok().ok_or_else(
				|| tg::error!(%referrer = referrer.item, "the referrer must be a file"),
			)?;
		let referent = file.get_dependency(&self.server, &import.reference).await?;


		let object = referent.item.id(&self.server).await?.clone();
		let item = tg::module::data::Item::Object(object);
		let subpath = referent.subpath;
		let tag = referent.tag;
		let path = if tag.is_none() {
			referrer
				.subpath
				.as_ref()
				.and_then(|subpath| Some(referrer.path.as_ref()?.join(subpath).parent()?.to_owned()))
				.or_else(|| referrer.path.clone())
				.map(|referrer| {
					if let Some(referent) = &referent.path {
						referrer.join(referent)
					} else {
						referrer
					}
				})
		} else {
			None
		};
		Ok(tg::Referent {
			item,
			path,
			subpath,
			tag,
		})
	}
}
