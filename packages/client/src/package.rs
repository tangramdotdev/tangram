use crate as tg;
use std::path::Path;
use tangram_either::Either;

pub mod check;
pub mod document;
pub mod format;

/// The possible file names for the root module in a package.
pub const ROOT_MODULE_FILE_NAMES: &[&str] = &["tangram.js", "tangram.ts"];

/// The file name of the lockfile in a package.
pub const LOCKFILE_FILE_NAME: &str = "tangram.lock";

#[must_use]
pub fn is_root_module_path(path: &Path) -> bool {
	let Some(name) = path.file_name() else {
		return false;
	};
	let Some(name) = name.to_str() else {
		return false;
	};
	tg::package::ROOT_MODULE_FILE_NAMES
		.iter()
		.any(|n| name == *n)
}

#[must_use]
pub fn is_module_path(path: &Path) -> bool {
	let Some(name) = path.file_name() else {
		return false;
	};
	let Some(name) = name.to_str() else {
		return false;
	};
	tg::package::ROOT_MODULE_FILE_NAMES
		.iter()
		.any(|n| name == *n)
		|| name.ends_with(".tg.js")
		|| name.ends_with(".tg.ts")
}

pub async fn is<H>(handle: &H, package: Either<&tg::Object, &Path>) -> tg::Result<bool>
where
	H: tg::Handle,
{
	try_get_root_module_file_name(handle, package)
		.await
		.map(|option| option.is_some())
}

pub async fn try_get_root_module_file_name<H>(
	handle: &H,
	package: Either<&tg::Object, &Path>,
) -> tg::Result<Option<&'static str>>
where
	H: tg::Handle,
{
	let mut name = None;
	for name_ in tg::package::ROOT_MODULE_FILE_NAMES {
		let exists = match package {
			Either::Left(object) => object
				.try_unwrap_directory_ref()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?
				.try_get_entry(handle, name_)
				.await?
				.is_some(),
			Either::Right(path) => tokio::fs::try_exists(path.join(*name_))
				.await
				.map_err(|source| tg::error!(!source, "failed to get the metadata"))?,
		};
		if exists {
			if name.is_some() {
				return Err(tg::error!("package contains multiple root modules"));
			}
			name = Some(*name_);
		}
	}
	Ok(name)
}

pub async fn try_get_root_module_file_name_for_package_path(
	path: &Path,
) -> tg::Result<Option<&'static str>> {
	let mut name = None;
	for name_ in tg::package::ROOT_MODULE_FILE_NAMES {
		let exists = tokio::fs::try_exists(path.join(name_))
			.await
			.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
		if exists {
			if name.is_some() {
				return Err(tg::error!("found multiple root modules"));
			}
			name = Some(*name_);
		}
	}
	Ok(name)
}

pub async fn try_get_nearest_package_path_for_path(path: &Path) -> tg::Result<Option<&Path>> {
	for path in path.ancestors() {
		let metadata = tokio::fs::metadata(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
		if metadata.is_dir()
			&& try_get_root_module_file_name_for_package_path(path)
				.await?
				.is_some()
		{
			return Ok(Some(path));
		}
	}
	Ok(None)
}
