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
					},
				..
			} => {
				let referrer = tg::Referent {
					item: item.as_ref(),
					path: path.clone(),
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
					},
				..
			} => {
				let referrer = tg::Referent {
					item,
					path: path.clone(),
					tag: tag.clone(),
				};
				self.resolve_module_with_object_referrer(referrer, import)
					.await?
			},
		};

		// If the kind is not known, then try to infer it from the path extension.
		let kind = if let Some(kind) = kind {
			Some(kind)
		} else if let Some(path) = &referent.path {
			let extension = path.extension();
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
		// Resolve path dependencies.
		if let Some(path) = import.reference.path() {
			let path =
				crate::util::fs::canonicalize_parent(&referrer.item.parent().unwrap().join(path))
					.await
					.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
			let metadata = tokio::fs::metadata(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
			if metadata.is_dir()
				&& matches!(
					import.kind,
					None | Some(tg::module::Kind::Js | tg::module::Kind::Ts)
				) {
				if let Some(root_module_name) =
					tg::package::try_get_root_module_file_name(&self.server, Either::Right(&path))
						.await?
				{
					let path = path.join(root_module_name);
					return Ok(tg::Referent::with_item(tg::module::data::Item::Path(path)));
				}
			}
			return Ok(tg::Referent::with_item(tg::module::data::Item::Path(path)));
		}

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
		let referent = file
			.dependencies
			.get(&import.reference)
			.ok_or_else(|| tg::error!("failed to resolve reference"))?;
		let referent = match &referent.item {
			Either::Left(index) => {
				let item = crate::Server::create_object_from_lockfile_node(&lockfile.nodes, *index)
					.map_err(|source| tg::error!(!source, "failed to resolve the dependency"))?;
				tg::Referent {
					item,
					path: referent.path.clone(),
					tag: referent.tag.clone(),
				}
			},
			Either::Right(id) => {
				let path = referent.path.clone().or_else(|| referrer.path.clone());
				let tag = referent.tag.clone().or_else(|| referent.tag.clone());
				tg::Referent {
					item: tg::Object::with_id(id.clone()),
					path,
					tag,
				}
			},
		};

		let referent = self
			.try_resolve_module_with_kind(import.kind, referent)
			.await?
			.map(|item| tg::module::data::Item::Object(item.id()));

		Ok(referent)
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
		let referent = self
			.try_resolve_module_with_kind(
				import.kind,
				file.get_dependency(&self.server, &import.reference).await?,
			)
			.await?;

		let object = referent.item.id();
		let item = tg::module::data::Item::Object(object);
		let tag = referent.tag;
		let referrer_path = referrer.path.as_deref().and_then(|path| path.parent());
		let path = match (referrer_path, &referent.path) {
			(Some(referrer), Some(referent)) => Some(referrer.join(referent)),
			(None, Some(referent)) => Some(referent.clone()),
			(Some(referrer), None) => Some(referrer.to_owned()),
			(None, None) => None,
		};
		Ok(tg::Referent { item, path, tag })
	}

	async fn try_resolve_module_with_kind(
		&self,
		kind: Option<tg::module::Kind>,
		referent: tg::Referent<tg::Object>,
	) -> tg::Result<tg::Referent<tg::Object>> {
		match (kind, &referent.item) {
			(
				None | Some(tg::module::Kind::Js | tg::module::Kind::Ts),
				tg::Object::Directory(directory),
			) => {
				let path = tg::package::try_get_root_module_file_name(
					&self.server,
					Either::Left(&referent.item),
				)
				.await?;
				let (item, path) = if let Some(path) = path {
					let file = directory
						.get(&self.server, path)
						.await?
						.try_unwrap_file_ref()
						.map_err(|_| tg::error!("expected a file"))?
						.clone()
						.into();
					(file, path)
				} else if kind.is_none() {
					return Ok(referent);
				} else {
					return Err(tg::error!("expected a root module"));
				};
				let path = referent.path.map_or_else(|| path.into(), |p| p.join(path));
				Ok(tg::Referent {
					item,
					path: Some(path),
					tag: referent.tag,
				})
			},
			(
				None
				| Some(
					tg::module::Kind::Js
					| tg::module::Kind::Ts
					| tg::module::Kind::Dts
					| tg::module::Kind::File,
				),
				tg::Object::File(_),
			)
			| (Some(tg::module::Kind::Object), _)
			| (Some(tg::module::Kind::Blob), tg::Object::Blob(_))
			| (Some(tg::module::Kind::Directory), tg::Object::Directory(_))
			| (Some(tg::module::Kind::Symlink), tg::Object::Symlink(_))
			| (Some(tg::module::Kind::Graph), tg::Object::Graph(_))
			| (Some(tg::module::Kind::Command), tg::Object::Command(_))
			| (
				Some(tg::module::Kind::Artifact),
				tg::Object::Directory(_) | tg::Object::File(_) | tg::Object::Symlink(_),
			) => Ok(referent),
			(
				None | Some(tg::module::Kind::Js | tg::module::Kind::Ts | tg::module::Kind::Dts),
				_,
			) => Err(tg::error!("expected a file")),
			(Some(tg::module::Kind::Artifact), _) => Err(tg::error!("expected an artifact")),
			(Some(tg::module::Kind::Blob), _) => Err(tg::error!("expected a blob")),
			(Some(tg::module::Kind::Directory), _) => Err(tg::error!("expected a directory")),
			(Some(tg::module::Kind::File), _) => Err(tg::error!("expected a file")),
			(Some(tg::module::Kind::Symlink), _) => Err(tg::error!("expected a symlink")),
			(Some(tg::module::Kind::Graph), _) => Err(tg::error!("expected a graph")),
			(Some(tg::module::Kind::Command), _) => Err(tg::error!("expected a command")),
		}
	}
}
