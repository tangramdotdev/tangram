use super::Compiler;
use std::path::Path;
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
					.path()
					.try_unwrap_object_ref()
					.ok()
					.ok_or_else(|| tg::error!("an import with no referrer must specify an object"))?
					.clone();
				let item = tg::module::Item::Object(object);
				let subpath = import
					.reference
					.query()
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
				todo!()
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
}
