use crate::{directory, lock, Dependency, Directory, Handle, Lock};
use async_trait::async_trait;
use std::path::Path;
use tangram_error::{error, Result, WrapErr};

/// The possible file names of the root module in a package.
pub const ROOT_MODULE_FILE_NAMES: &[&str] =
	&["tangram.js", "tangram.tg.js", "tangram.tg.ts", "tangram.ts"];

/// The file name of the lockfile in a package.
pub const LOCKFILE_FILE_NAME: &str = "tangram.lock";

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Metadata {
	pub name: Option<String>,
	pub version: Option<String>,
	pub description: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct SearchArg {
	pub query: String,
}

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct GetArg {
	pub lock: bool,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct GetOutput {
	pub id: directory::Id,
	pub lock: Option<lock::Id>,
}

#[async_trait]
pub trait Ext {
	async fn root_module_path(&self, tg: &dyn Handle) -> Result<crate::Path>;
	async fn metadata(&self, tg: &dyn Handle) -> Result<Metadata>;
	async fn dependencies(&self, tg: &dyn Handle) -> Result<Vec<Dependency>>;
}

pub async fn get_root_module_path(tg: &dyn Handle, package: &Directory) -> Result<crate::Path> {
	try_get_root_module_path(tg, package)
		.await?
		.wrap_err("Failed to find the package's root module.")
}

pub async fn try_get_root_module_path(
	tg: &dyn Handle,
	package: &Directory,
) -> Result<Option<crate::Path>> {
	let mut root_module_path = None;
	for module_file_name in ROOT_MODULE_FILE_NAMES {
		if package
			.try_get(tg, &module_file_name.parse().unwrap())
			.await?
			.is_some()
		{
			if root_module_path.is_some() {
				return Err(error!("Found multiple root modules."));
			}
			root_module_path = Some(module_file_name.parse().unwrap());
		}
	}
	Ok(root_module_path)
}

pub async fn get_root_module_path_for_path(path: &Path) -> Result<crate::Path> {
	try_get_root_module_path_for_path(path)
		.await?
		.wrap_err("Failed to find the package's root module.")
}

pub async fn try_get_root_module_path_for_path(path: &Path) -> Result<Option<crate::Path>> {
	let mut root_module_path = None;
	for module_file_name in ROOT_MODULE_FILE_NAMES {
		if tokio::fs::try_exists(path.join(module_file_name))
			.await
			.wrap_err("Failed to get the metadata.")?
		{
			if root_module_path.is_some() {
				return Err(error!("Found multiple root modules."));
			}
			root_module_path = Some(module_file_name.parse().unwrap());
		}
	}
	Ok(root_module_path)
}

pub async fn get(tg: &dyn Handle, dependency: &Dependency) -> Result<Directory> {
	let arg = GetArg { lock: false };
	let output = tg.get_package(dependency, arg).await?;
	let package = Directory::with_id(output.id);
	Ok(package)
}

pub async fn try_get(tg: &dyn Handle, dependency: &Dependency) -> Result<Option<Directory>> {
	let arg = GetArg { lock: false };
	let output = tg.try_get_package(dependency, arg).await?;
	let package = output.map(|output| Directory::with_id(output.id));
	Ok(package)
}

pub async fn get_with_lock(tg: &dyn Handle, dependency: &Dependency) -> Result<(Directory, Lock)> {
	let arg = GetArg { lock: true };
	let output = tg.get_package(dependency, arg).await?;
	let package = Directory::with_id(output.id);
	let lock = output.lock.wrap_err("Expected the lock to be set.")?;
	let lock = Lock::with_id(lock);
	Ok((package, lock))
}

#[async_trait]
impl Ext for Directory {
	async fn root_module_path(&self, tg: &dyn Handle) -> Result<crate::Path> {
		get_root_module_path(tg, self).await
	}

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
