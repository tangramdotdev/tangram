use super::Compiler;
use std::{ffi::OsStr, path::PathBuf};
use tangram_client as tg;
use tangram_either::Either;
use tg::path::Ext as _;

impl Compiler {
	/// Resolve an import from a module.
	pub async fn resolve_module(
		&self,
		referrer: Option<&tg::Module>,
		import: &tg::Import,
	) -> tg::Result<tg::Module> {
		let kind = import.kind;

		// Get the object and path.
		let (object, path) = 'a: {
			if let Some(referrer) = referrer {
				if let Some(Either::Left(tg::object::Id::Directory(referrer_package))) =
					&referrer.object
				{
					let referrer_package = tg::Directory::with_id(referrer_package.clone());
					let referrer_path = referrer
						.path
						.as_ref()
						.ok_or_else(|| tg::error!("the referrer must have a path"))?;

					// Handle a path import.
					if let tg::reference::Path::Path(import_path) = import.reference.path() {
						let path = referrer_path
							.clone()
							.parent()
							.unwrap()
							.join(import_path.clone())
							.normalize();

						// If the import is internal to the package, then use the imported path in the existing package.
						if path.is_internal() {
							let object = Either::Left(referrer_package.clone().into());
							break 'a (object, Some(path));
						}
					}

					// Otherwise, get the dependency from the referrer's file.
					let referrer_file = referrer_package
						.get(&self.server, referrer_path)
						.await?
						.try_unwrap_file()
						.ok()
						.ok_or_else(|| tg::error!("expected a file"))?;
					let object = referrer_file
						.get_dependency(&self.server, &import.reference)
						.await?;
					let object = Either::Left(object);
					let path = None;

					(object, path)
				} else if let Some(Either::Right(referrer_package)) = &referrer.object {
					let referrer_path = referrer
						.path
						.as_ref()
						.ok_or_else(|| tg::error!("the referrer must have a path"))?;

					// Handle a path import.
					let import_path = import
						.reference
						.path()
						.try_unwrap_path_ref()
						.ok()
						.or_else(|| import.reference.query()?.path.as_ref());
					if let Some(import_path) = import_path {
						let path = referrer_path
							.parent()
							.unwrap()
							.join(import_path)
							.normalize();

						// If the import is internal to the package, then use the imported path in the existing package.
						if path.is_internal() {
							let object = referrer_package.clone();
							break 'a (Either::Right(object), Some(path));
						}

						// Otherwise, return the path.
						let object = referrer_package.clone().join(path).normalize();
						break 'a (Either::Right(object), None);
					}

					// Try to find this module in an existing lockfile.
					let Some((lockfile, node)) =
						crate::lockfile::try_get_lockfile_node_for_module_path(
							referrer_path.as_ref(),
						)
						.await?
					else {
						return Err(tg::error!("failed to resolve module"));
					};
					let tg::lockfile::Node::File { dependencies, .. } = &lockfile.nodes[node]
					else {
						return Err(tg::error!("expected a file node"));
					};

					// Try to resolve using the node in the lockfile.
					let Some(object) = dependencies
						.get(&import.reference)
						.map(|dependency| &dependency.object)
					else {
						return Err(tg::error!("failed to resolve module"));
					};

					let object = match object {
						Either::Left(node) => crate::lockfile::create_artifact_for_lockfile_node(
							&self.server,
							&lockfile,
							*node,
						)
						.await?
						.into(),
						Either::Right(object) => tg::Object::with_id(object.clone()),
					};

					(Either::Left(object), None)
				} else {
					return Err(tg::error!("the referrer must have an object"));
				}
			} else {
				let object = tg::Object::with_id(
					import
						.reference
						.path()
						.try_unwrap_object_ref()
						.ok()
						.ok_or_else(|| tg::error!("invalid import"))?
						.clone(),
				);
				let (object, path) = if let tg::Object::Symlink(symlink) = object.clone() {
					let object = tg::Object::with_id(
						symlink
							.artifact(&self.server)
							.await?
							.ok_or_else(|| tg::error!("invalid symlink"))?
							.id(&self.server)
							.await?
							.into(),
					);
					let path = symlink.path(&self.server).await?.map(PathBuf::from);
					(object, path)
				} else {
					(object, None)
				};
				(Either::Left(object), path)
			}
		};

		// If the kind is not known and the object and path refer to a package, then return its root module.
		let (object, path) = if kind.is_some() {
			(object, path)
		} else {
			match object {
				Either::Left(object) => {
					let object_ = if let Some(path) = &path {
						object
							.try_unwrap_directory_ref()
							.ok()
							.ok_or_else(|| tg::error!("expected a directory"))?
							.try_get(&self.server, path)
							.await?
							.ok_or_else(|| {
								let message = "expected the directory to contain the path";
								tg::error!(?object, ?path, "{message}")
							})?
							.into()
					} else {
						object.clone()
					};
					if object_.try_unwrap_directory_ref().is_ok() {
						if let Some(root_module_file_name) =
							tg::package::try_get_root_module_file_name(
								&self.server,
								Either::Left(&object_),
							)
							.await?
						{
							let path = root_module_file_name.into();
							(Either::Left(object_), Some(path))
						} else {
							(Either::Left(object), path)
						}
					} else {
						(Either::Left(object), path)
					}
				},

				Either::Right(object) => {
					let path_ = if let Some(path) = &path {
						object.join(path)
					} else {
						object.clone()
					};
					let metadata = tokio::fs::metadata(&path_)
						.await
						.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
					if metadata.is_dir() {
						if let Some(root_module_file_name) =
							tg::package::try_get_root_module_file_name_for_package_path(&path_)
								.await?
						{
							let path = root_module_file_name.parse().unwrap();
							(Either::Right(object), Some(path))
						} else {
							(Either::Right(object), path)
						}
					} else {
						(Either::Right(object), path)
					}
				},
			}
		};

		// If the kind is not known, then try to infer it from the path extension.
		let kind = if let Some(kind) = kind {
			Some(kind)
		} else if let Some(path) = &path {
			let extension = path.extension().and_then(OsStr::to_str);
			if extension == Some("js") {
				Some(tg::module::Kind::Js)
			} else if extension == Some("ts") {
				Some(tg::module::Kind::Ts)
			} else {
				None
			}
		} else {
			None
		};

		// If the kind is not known, then infer it from the object's kind.
		let kind = if let Some(kind) = kind {
			kind
		} else {
			match &object {
				Either::Left(object) => {
					let object = if let Some(path) = &path {
						object
							.try_unwrap_directory_ref()
							.ok()
							.ok_or_else(|| tg::error!("expected a directory"))?
							.get(&self.server, path)
							.await?
							.into()
					} else {
						object.clone()
					};
					match &object {
						tg::Object::Leaf(_) => tg::module::Kind::Leaf,
						tg::Object::Branch(_) => tg::module::Kind::Branch,
						tg::Object::Directory(_) => tg::module::Kind::Directory,
						tg::Object::File(_) => tg::module::Kind::File,
						tg::Object::Symlink(_) => tg::module::Kind::Symlink,
						tg::Object::Graph(_) => tg::module::Kind::Graph,
						tg::Object::Target(_) => tg::module::Kind::Target,
					}
				},

				Either::Right(object) => {
					let path = if let Some(path) = &path {
						object.join(path)
					} else {
						object.clone()
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
			}
		};

		// Create the module.
		let object = Some(match object {
			Either::Left(object) => Either::Left(object.id(&self.server).await?),
			Either::Right(object) => Either::Right(object),
		});
		let module = tg::Module { kind, object, path };

		Ok(module)
	}
}
