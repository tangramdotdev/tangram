use super::Server;
use tangram_client as tg;
use tangram_error::{error, Result};

impl Server {
	/// Resolve an import from a module.
	pub async fn resolve_module(
		&self,
		module: &tg::Module,
		import: &tg::Import,
	) -> Result<tg::Module> {
		match (module, import) {
			(tg::Module::Library(module), tg::Import::Module(path)) => {
				let path = module.path.clone().parent().join(path.clone()).normalize();
				Ok(tg::Module::Library(tg::module::Library { path }))
			},

			(tg::Module::Library(_), tg::Import::Dependency(_)) => Err(error!(
				r#"cannot resolve a dependency import from a library module"#
			)),

			(tg::Module::Document(document), tg::Import::Module(path)) => {
				// Resolve the module path.
				let package_path = document.package_path.clone();
				let module_path = document
					.path
					.clone()
					.parent()
					.join(path.clone())
					.normalize();

				// Ensure that the module exists.
				let module_absolute_path = package_path.join(module_path.to_string());
				let exists =
					tokio::fs::try_exists(&module_absolute_path)
						.await
						.map_err(|error| {
							error!(source = error, "failed to determine if the path exists")
						})?;
				if !exists {
					let path = module_absolute_path.display();
					return Err(error!(%path, "could not find a module"));
				}

				// Get or create the document.
				let document = self.get_document(package_path, module_path).await?;

				// Create the module.
				let module = tg::Module::Document(document);

				Ok(module)
			},

			(tg::Module::Document(document), tg::Import::Dependency(dependency))
				if dependency.path.is_some() =>
			{
				// Resolve the package path.
				let dependency_path = document
					.path
					.clone()
					.parent()
					.join(dependency.path.as_ref().unwrap().clone())
					.normalize();
				let package_path = document.package_path.join(dependency_path.to_string());
				let package_path = tokio::fs::canonicalize(package_path)
					.await
					.map_err(|source| error!(!source, "failed to canonicalize the path"))?;

				// Get the package's root module path.
				let module_path = tg::package::get_root_module_path_for_path(&package_path).await?;

				// Get or create the document.
				let document = self.get_document(package_path, module_path).await?;

				// Create the module.
				let module = tg::Module::Document(document);

				Ok(module)
			},

			(tg::Module::Document(document), tg::Import::Dependency(dependency)) => {
				// Make the dependency path relative to the package.
				let mut dependency = dependency.clone();
				if let Some(path) = dependency.path.as_mut() {
					*path = document
						.path
						.clone()
						.parent()
						.join(path.clone())
						.normalize();
				}

				// Get the lock for the document's package.
				let path = document.package_path.clone().try_into()?;
				let dependency_ = tg::Dependency::with_path(path);
				let (_, lock) =
					tg::package::get_with_lock(&self.inner.server, &dependency_).await?;

				// Get the package and lock for the dependency.
				let (package, lock) = lock
					.get(&self.inner.server, &dependency)
					.await?
					.ok_or_else(|| error!(%dependency, "failed to resolve dependency"))?;

				// Create the module.
				let path = tg::package::get_root_module_path(&self.inner.server, &package).await?;
				let lock = lock.id(&self.inner.server).await?.clone();
				let package = package.id(&self.inner.server).await?.clone();
				let module = tg::Module::Normal(tg::module::Normal {
					lock,
					package,
					path,
				});

				Ok(module)
			},

			(tg::Module::Normal(module), tg::Import::Module(path)) => {
				let path = module.path.clone().parent().join(path.clone()).normalize();
				Ok(tg::Module::Normal(tg::module::Normal {
					package: module.package.clone(),
					path,
					lock: module.lock.clone(),
				}))
			},

			(tg::Module::Normal(module), tg::Import::Dependency(dependency)) => {
				// Make the dependency path relative to the package.
				let mut dependency = dependency.clone();
				if let Some(path) = dependency.path.as_mut() {
					*path = module.path.clone().parent().join(path.clone()).normalize();
				}

				// Get this module's lock.
				let lock = tg::Lock::with_id(module.lock.clone());

				// Get the specified package and lock from the dependencies.
				let (package, lock) = lock
					.get(&self.inner.server, &dependency)
					.await?
					.ok_or_else(|| error!(%dependency, "failed to resolve dependency"))?;

				// Create the module.
				let path = tg::package::get_root_module_path(&self.inner.server, &package).await?;
				let package = package.id(&self.inner.server).await?.clone();
				let lock = lock.id(&self.inner.server).await?.clone();
				let module = tg::Module::Normal(tg::module::Normal {
					lock,
					package,
					path,
				});

				Ok(module)
			},
		}
	}
}
