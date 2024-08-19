use crate as tg;
use std::path::Path;

/// The possible file names for the root module in a package.
pub const ROOT_MODULE_FILE_NAMES: &[&str] =
	&["tangram.js", "tangram.tg.js", "tangram.tg.ts", "tangram.ts"];

/// The file name of the lockfile in a package.
pub const LOCKFILE_FILE_NAME: &str = "tangram.lock";

pub async fn get_root_module_path<H>(handle: &H, artifact: &tg::Artifact) -> tg::Result<tg::Path>
where
	H: tg::Handle,
{
	try_get_root_module_path(handle, artifact)
		.await?
		.ok_or_else(|| tg::error!("failed to find the package's root module"))
}

pub async fn try_get_root_module_path<H>(
	handle: &H,
	artifact: &tg::Artifact,
) -> tg::Result<Option<tg::Path>>
where
	H: tg::Handle,
{
	let Ok(artifact) = artifact.try_unwrap_directory_ref() else {
		return Ok(None);
	};
	let mut root_module_path = None;
	for module_file_name in ROOT_MODULE_FILE_NAMES {
		if artifact
			.try_get(handle, &module_file_name.parse().unwrap())
			.await?
			.is_some()
		{
			if root_module_path.is_some() {
				return Err(tg::error!("found multiple root modules"));
			}
			root_module_path = Some(module_file_name.parse().unwrap());
		}
	}
	Ok(root_module_path)
}

pub async fn get_root_module_path_for_path(path: &Path) -> tg::Result<tg::Path> {
	try_get_root_module_path_for_path(path).await?.ok_or_else(
		|| tg::error!(%path = path.display(), "failed to find the package's root module"),
	)
}

pub async fn try_get_root_module_path_for_path(path: &Path) -> tg::Result<Option<tg::Path>> {
	let mut root_module_path = None;
	for module_file_name in ROOT_MODULE_FILE_NAMES {
		if tokio::fs::try_exists(path.join(module_file_name))
			.await
			.map_err(|source| tg::error!(!source, "failed to get the metadata"))?
		{
			if root_module_path.is_some() {
				return Err(tg::error!("found multiple root modules"));
			}
			root_module_path = Some(module_file_name.parse().unwrap());
		}
	}
	Ok(root_module_path)
}

pub fn is_root_module_path(path: &Path) -> bool {
	let Some(last) = path.components().last() else {
		return false;
	};
	let last = last.as_os_str().to_string_lossy();
	ROOT_MODULE_FILE_NAMES.iter().any(|name| &last == *name)
}

pub fn is_module_path(path: &Path) -> bool {
	let Some(last) = path.components().last() else {
		return false;
	};

	let last = last.as_os_str().to_string_lossy();
	ROOT_MODULE_FILE_NAMES.iter().any(|name| &last == *name)
		|| last.ends_with(".tg.js")
		|| last.ends_with(".tg.ts")
}

pub async fn try_get_lock_path_for_path(path: &Path) -> tg::Result<Option<tg::Path>> {
	// Canonicalize the path.
	let path = tokio::fs::canonicalize(path).await.map_err(
		|source| tg::error!(!source, %path = path.display(), "failed to canonicalize the path"),
	)?;

	// Check if this is a module path.
	if !is_module_path(path.as_ref()) {
		return Ok(None);
	}

	// Walk up to find a lockfile.
	let mut path_: &Path = path.as_ref();
	while let Some(parent) = path_.parent() {
		let lock = parent.join(LOCKFILE_FILE_NAME);
		if tokio::fs::try_exists(&lock).await.map_err(
			|source| tg::error!(!source, %path = lock.display(), "failed to check if file exists"),
		)? {
			let path = path
				.try_into()
				.map_err(|source| tg::error!(!source, "invalid path"))?;
			return Ok(Some(path));
		}
		path_ = parent;
	}

	// If no existing lockfile was found, compute the path to the new lockfile.
	let path = path
		.parent()
		.unwrap()
		.join(LOCKFILE_FILE_NAME)
		.try_into()
		.map_err(|source| tg::error!(!source, "invalid path"))?;
	Ok(Some(path))
}
