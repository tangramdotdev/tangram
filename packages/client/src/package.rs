use crate::{Dependency, Directory, Handle, Lock};
use async_trait::async_trait;
use tangram_error::Result;

/// The file name of the root module in a package.
pub const ROOT_MODULE_FILE_NAME: &str = "tangram.tg";

/// The file name of the lockfile in a package.
pub const LOCKFILE_FILE_NAME: &str = "tangram.lock";

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Metadata {
	pub name: Option<String>,
	pub version: Option<String>,
	pub description: Option<String>,
}

#[async_trait]
pub trait Ext {
	async fn metadata(&self, tg: &dyn Handle) -> Result<Metadata>;
	async fn dependencies(&self, tg: &dyn Handle) -> Result<Vec<Dependency>>;
}

pub async fn get(tg: &dyn Handle, dependency: &Dependency) -> Result<Directory> {
	let package = tg.get_package(dependency).await?;
	let package = Directory::with_id(package);
	Ok(package)
}

pub async fn get_with_lock(tg: &dyn Handle, dependency: &Dependency) -> Result<(Directory, Lock)> {
	let (package, lock) = tg.get_package_and_lock(dependency).await?;
	let package = Directory::with_id(package);
	let lock = Lock::with_id(lock);
	Ok((package, lock))
}

#[async_trait]
impl Ext for Directory {
	async fn metadata(&self, tg: &dyn Handle) -> Result<Metadata> {
		let id = self.id(tg).await?;
		let dependency = Dependency::with_id(id.clone());
		let dependencies = tg.get_package_metadata(&dependency).await?;
		Ok(dependencies)
	}

	async fn dependencies(&self, tg: &dyn Handle) -> Result<Vec<Dependency>> {
		let id = self.id(tg).await?;
		let dependency = Dependency::with_id(id.clone());
		let dependencies = tg.get_package_dependencies(&dependency).await?;
		Ok(dependencies)
	}
}
