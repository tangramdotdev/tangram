use std::collections::BTreeMap;

use crate::Server;
use tangram_client as tg;
use tangram_error::{Result, WrapErr};

impl Server {
	pub async fn search_packages(&self, arg: tg::package::SearchArg) -> Result<Vec<String>> {
		self.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?
			.search_packages(arg)
			.await
	}

	pub async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> Result<Option<tg::package::GetOutput>> {
		// Attempt to get the package with path dependencies locally.
		let package_with_path_dependencies = 'a: {
			let Some(package_with_path_dependencies) =
				tangram_package::try_get(self, dependency).await?
			else {
				break 'a None;
			};

			Some(package_with_path_dependencies)
		};

		// If the dependency has a name, then attempt to get it from the remote.
		let package_with_path_dependencies = 'a: {
			if let Some(package_with_path_dependencies) = package_with_path_dependencies {
				break 'a Some(package_with_path_dependencies);
			}

			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a None;
			};

			let arg = tg::package::GetArg { lock: false };
			let Some(output) = remote.try_get_package(dependency, arg).await.ok().flatten() else {
				break 'a None;
			};

			let package = tg::Directory::with_id(output.id);

			let package_with_path_dependencies = tangram_package::PackageWithPathDependencies {
				package,
				path_dependencies: BTreeMap::default(),
			};

			Some(package_with_path_dependencies)
		};

		// If the package was not found, then return `None`.
		let Some(package_with_path_dependencies) = package_with_path_dependencies else {
			return Ok(None);
		};

		// Create the lock if requested.
		let lock = if arg.lock {
			let path = dependency.path.as_ref();
			Some(tangram_package::create_lock(self, path, &package_with_path_dependencies).await?)
		} else {
			None
		};

		// Get the package ID.
		let id = package_with_path_dependencies
			.package
			.id(self)
			.await?
			.clone();

		// Get the lock ID.
		let lock = if let Some(lock) = lock {
			Some(lock.id(self).await?.clone())
		} else {
			None
		};

		Ok(Some(tg::package::GetOutput { id, lock }))
	}

	pub async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<Vec<String>>> {
		self.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?
			.try_get_package_versions(dependency)
			.await
	}

	pub async fn try_get_package_metadata(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<tg::package::Metadata>> {
		let Some(package_with_path_dependencies) =
			tangram_package::try_get(self, dependency).await?
		else {
			return Ok(None);
		};
		let package = package_with_path_dependencies.package;
		let metadata = tangram_package::get_metadata(self, &package).await?;
		Ok(Some(metadata))
	}

	pub async fn try_get_package_dependencies(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<Vec<tg::Dependency>>> {
		let Some(package_with_path_dependencies) =
			tangram_package::try_get(self, dependency).await?
		else {
			return Ok(None);
		};
		let package = package_with_path_dependencies.package;
		let dependencies = tangram_package::get_dependencies(self, &package).await?;
		Ok(Some(dependencies))
	}

	pub async fn publish_package(
		&self,
		user: Option<&tg::User>,
		id: &tg::directory::Id,
	) -> Result<()> {
		// Push the package.
		self.push_object(&id.clone().into()).await?;

		// Publish the package.
		let remote = self
			.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?;
		remote.publish_package(user, id).await?;

		Ok(())
	}
}
