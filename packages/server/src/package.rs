use crate::Server;
use tangram_client as tg;
use tangram_error::{Result, WrapErr};
use tg::Handle;

impl Server {
	pub async fn search_packages(&self, query: &str) -> Result<Vec<String>> {
		self.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?
			.search_packages(query)
			.await
	}

	pub async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<tg::directory::Id>> {
		// If the dependency has an ID, then return it.
		if let Some(id) = dependency.id.as_ref() {
			return Ok(Some(id.clone()));
		}

		// If the dependency has a path, then attempt to get it from the path.
		if dependency.path.is_some() {
			if let Some(package) = tangram_package::try_get_package(self, dependency).await? {
				let package = package.id(self).await?.clone();
				return Ok(Some(package));
			}
		}

		// If the dependency has a name, then attempt to get it from the remote.
		if dependency.name.is_some() {
			if let Some(remote) = self.inner.remote.as_ref() {
				if let Some(package) = remote.try_get_package(dependency).await.ok().flatten() {
					return Ok(Some(package));
				}
			}
		}

		Ok(None)
	}

	pub async fn try_get_package_and_lock(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<(tg::directory::Id, tg::lock::Id)>> {
		if let Some((package, lock)) =
			tangram_package::try_get_package_and_lock(self, dependency).await?
		{
			let package = package.id(self).await?.clone();
			let lock = lock.id(self).await?.clone();
			return Ok(Some((package, lock)));
		};

		Ok(None)
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
		let package = tg::Directory::with_id(self.get_package(dependency).await?);
		let metadata = tangram_package::metadata(self, &package).await?;
		Ok(Some(metadata))
	}

	pub async fn try_get_package_dependencies(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<Vec<tg::Dependency>>> {
		let package = tg::Directory::with_id(self.get_package(dependency).await?);
		let dependencies = tangram_package::dependencies(self, &package).await?;
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
