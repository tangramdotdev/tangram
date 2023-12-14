use super::ROOT_MODULE_FILE_NAME;
use crate::{
	document,
	module::{Library, Normal},
	Document, Import, Module,
};
use tangram_client as tg;
use tangram_error::{error, return_error, Result, WrapErr};

impl Module {
	/// Resolve a module.
	#[allow(clippy::too_many_lines)]
	pub async fn resolve(
		&self,
		tg: &dyn tg::Handle,
		document_store: Option<&document::Store>,
		import: &Import,
	) -> Result<Self> {
		match (self, import) {
			(Self::Library(module), Import::Module(path)) => {
				let path = module.path.clone().parent().join(path.clone()).normalize();
				Ok(Self::Library(Library { path }))
			},

			(Self::Library(_), Import::Dependency(_)) => Err(error!(
				r#"Cannot resolve a dependency import from a library module."#
			)),

			(Self::Document(document), Import::Module(path)) => {
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
				let exists = tokio::fs::try_exists(&module_absolute_path)
					.await
					.wrap_err("Failed to determine if the path exists.")?;
				if !exists {
					let path = module_absolute_path.display();
					return_error!(r#"Could not find a module at path "{path}"."#);
				}

				// Create the document.
				let document =
					Document::new(document_store.unwrap(), package_path, module_path).await?;

				// Create the module.
				let module = Self::Document(document);

				Ok(module)
			},

			(Self::Document(document), Import::Dependency(dependency))
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
					.wrap_err("Failed to canonicalize the path.")?;

				// The module path is the root module.
				let module_path = ROOT_MODULE_FILE_NAME.parse().unwrap();

				// Create the document.
				let document =
					Document::new(document_store.unwrap(), package_path, module_path).await?;

				// Create the module.
				let module = Self::Document(document);

				Ok(module)
			},

			(Self::Document(document), Import::Dependency(dependency)) => {
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
				let (_, lock) = tg::package::get_with_lock(tg, &dependency_).await?;

				// Get the lock entry for the dependency.
				let Some(entry) = lock.dependencies(tg).await?.get(&dependency) else {
					return_error!("Could not find the dependency.");
				};

				// Create the module.
				let lock = lock.id(tg).await?.clone();
				let package = entry.package.id(tg).await?.clone();
				let path = ROOT_MODULE_FILE_NAME.parse().unwrap();
				let module = Self::Normal(Normal {
					lock,
					package,
					path,
				});

				Ok(module)
			},

			(Self::Normal(module), Import::Module(path)) => {
				let path = module.path.clone().parent().join(path.clone()).normalize();
				Ok(Self::Normal(Normal {
					package: module.package.clone(),
					path,
					lock: module.lock.clone(),
				}))
			},

			(Self::Normal(module), Import::Dependency(dependency)) => {
				// Make the dependency path relative to the package.
				let mut dependency = dependency.clone();
				if let Some(path) = dependency.path.as_mut() {
					*path = module.path.clone().parent().join(path.clone()).normalize();
				}

				// Get this module's lock.
				let parent_lock = tg::Lock::with_id(module.lock.clone());

				// Get the specified package and lock from the dependencies.
				let dependencies = parent_lock.dependencies(tg).await?;
				let tg::lock::Entry { package, lock } = dependencies
					.get(&dependency)
					.cloned()
					.wrap_err_with(|| format!(r#"Failed to resolve "{dependency}"."#))?;

				// Create the module.
				let module = Module::Normal(Normal {
					package: package.id(tg).await?.clone(),
					path: ROOT_MODULE_FILE_NAME.parse().unwrap(),
					lock: lock.id(tg).await?.clone(),
				});

				Ok(module)
			},
		}
	}
}
