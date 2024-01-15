use crate::{directory, lock, Dependency, Directory, Handle, Lock};
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

pub type SearchOutput = Vec<String>;

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct GetArg {
	pub dependencies: bool,
	pub lock: bool,
	pub metadata: bool,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct GetOutput {
	pub dependencies: Option<Vec<Dependency>>,
	pub id: directory::Id,
	pub lock: Option<lock::Id>,
	pub metadata: Option<Metadata>,
}

pub async fn get(tg: &dyn Handle, dependency: &Dependency) -> Result<Directory> {
	try_get(tg, dependency)
		.await?
		.wrap_err("Failed to find the package.")
}

pub async fn try_get(tg: &dyn Handle, dependency: &Dependency) -> Result<Option<Directory>> {
	let arg = GetArg::default();
	let output = tg.try_get_package(dependency, arg).await?;
	let package = output.map(|output| Directory::with_id(output.id));
	Ok(package)
}

pub async fn get_with_lock(tg: &dyn Handle, dependency: &Dependency) -> Result<(Directory, Lock)> {
	try_get_with_lock(tg, dependency)
		.await?
		.wrap_err("Failed to find the package.")
}

pub async fn try_get_with_lock(
	tg: &dyn Handle,
	dependency: &Dependency,
) -> Result<Option<(Directory, Lock)>> {
	let arg = GetArg {
		lock: true,
		..Default::default()
	};
	let Some(output) = tg.try_get_package(dependency, arg).await? else {
		return Ok(None);
	};
	let package = Directory::with_id(output.id);
	let lock = output.lock.wrap_err("Expected the lock to be set.")?;
	let lock = Lock::with_id(lock);
	Ok(Some((package, lock)))
}

pub async fn get_dependencies(tg: &dyn Handle, package: &Directory) -> Result<Vec<Dependency>> {
	try_get_dependencies(tg, package)
		.await?
		.wrap_err("Failed to find the package.")
}

pub async fn try_get_dependencies(
	tg: &dyn Handle,
	package: &Directory,
) -> Result<Option<Vec<Dependency>>> {
	let id = package.id(tg).await?.clone();
	let dependency = Dependency::with_id(id);
	let arg = GetArg {
		dependencies: true,
		..Default::default()
	};
	let Some(output) = tg.try_get_package(&dependency, arg).await? else {
		return Ok(None);
	};
	let dependencies = output
		.dependencies
		.wrap_err("Expected the dependencies to be set.")?;
	Ok(Some(dependencies))
}

pub async fn get_metadata(tg: &dyn Handle, package: &Directory) -> Result<Metadata> {
	try_get_metadata(tg, package)
		.await?
		.wrap_err("Failed to find the package.")
}

pub async fn try_get_metadata(tg: &dyn Handle, package: &Directory) -> Result<Option<Metadata>> {
	let id = package.id(tg).await?.clone();
	let dependency = Dependency::with_id(id);
	let arg = GetArg {
		metadata: true,
		..Default::default()
	};
	let Some(output) = tg.try_get_package(&dependency, arg).await? else {
		return Ok(None);
	};
	let metadata = output
		.metadata
		.wrap_err("Expected the metadata to be set.")?;
	Ok(Some(metadata))
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
