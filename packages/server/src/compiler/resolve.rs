use super::Compiler;
use tangram_client as tg;
use tangram_either::Either;

impl Compiler {
	/// Resolve an import from a module.
	pub async fn resolve_module(
		&self,
		referrer: &tg::Module,
		import: &tg::Import,
	) -> tg::Result<tg::Module> {
		// Get the dependency.
		let (object, path) = 'a: {
			if let Some(Either::Left(tg::object::Id::Directory(package))) = referrer.object() {
				let package = tg::Directory::with_id(package.clone());
				let path = referrer
					.path()
					.ok_or_else(|| tg::error!("the referrer must have a path"))?;

				// Handle a path import.
				if let tg::reference::Path::Path(import_path) = import.reference.path() {
					let object = Either::Left(package.clone().into());
					let path = path.clone().join(import_path.clone());

					// If the import is internal to the package, then use the imported path in the existing package.
					if path.is_internal() {
						break 'a (object, Some(path));
					}
				}

				// Otherwise, get the dependency from the referrer's file.
				let file = package
					.get(&self.server, path)
					.await?
					.try_unwrap_file()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				let object = file.get_dependency(&self.server, &import.reference).await?;
				let object = Either::Left(object);
				let path = None;
				(object, path)
			} else if let Some(Either::Right(package)) = referrer.object() {
				let path = referrer
					.path()
					.ok_or_else(|| tg::error!("the referrer must have a path"))?;

				// Handle a path import.
				if let tg::reference::Path::Path(import_path) = import.reference.path() {
					let object = package.clone();
					let path = path.clone().join(import_path.clone());

					// If the import is internal to the package, then use the imported path in the existing package.
					if path.is_internal() {
						break 'a (Either::Right(object), Some(path));
					}

					// Otherwise, return the path.
					break 'a (Either::Right(object.join(path)), None);
				}

				// Otherwise, get the dependency from the lockfile.
				todo!()
			} else {
				return Err(tg::error!("referrer must have an object"));
			}
		};

		// If the kind was not specified and the object is a package, then return its root module.
		if import.kind.is_none() {
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
					if let tg::Object::Directory(directory) = &object {
						if let Some(module) = tg::Module::try_with_package(
							&self.server,
							Either::Left(directory.clone()),
						)
						.await?
						{
							return Ok(module);
						}
					}
				},

				Either::Right(object) => {
					let path = if let Some(path) = &path {
						object.clone().join(path.clone())
					} else {
						object.clone()
					};
					if let Some(module) =
						tg::Module::try_with_package(&self.server, Either::Right(path)).await?
					{
						return Ok(module);
					}
				},
			}
		}

		// Determine the module's kind.
		let kind = if let Some(kind) = import.kind {
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
						object.clone().join(path.clone())
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
		let module = tg::Module::new(kind, object, path);

		Ok(module)
	}
}
