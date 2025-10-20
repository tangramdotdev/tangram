use {crate as tg, std::path::Path, tangram_either::Either};

/// The possible file names for the root module in a package.
pub const ROOT_MODULE_FILE_NAMES: &[&str] = &["tangram.js", "tangram.ts"];

/// The file name of a lockfile.
pub const LOCKFILE_FILE_NAME: &str = "tangram.lock";

pub fn module_kind_for_path(path: impl AsRef<Path>) -> tg::Result<tg::module::Kind> {
	let path = path.as_ref();
	if path.ends_with(".d.ts") {
		Ok(tg::module::Kind::Dts)
	} else if path.extension().is_some_and(|ext| ext == "ts") {
		Ok(tg::module::Kind::Ts)
	} else if path.extension().is_some_and(|ext| ext == "js") {
		Ok(tg::module::Kind::Js)
	} else {
		Err(tg::error!(%path = path.display(), "unknown or missing file extension"))
	}
}

#[must_use]
pub fn is_module_path(path: &Path) -> bool {
	let Some(name) = path.file_name() else {
		return false;
	};
	let Some(name) = name.to_str() else {
		return false;
	};
	tg::package::ROOT_MODULE_FILE_NAMES.contains(&name)
		|| name.ends_with(".tg.js")
		|| name.ends_with(".tg.ts")
}

#[must_use]
pub fn is_root_module_path(path: &Path) -> bool {
	let Some(name) = path.file_name() else {
		return false;
	};
	let Some(name) = name.to_str() else {
		return false;
	};
	tg::package::ROOT_MODULE_FILE_NAMES.contains(&name)
}

#[must_use]
pub fn is_non_root_module_path(path: &Path) -> bool {
	let Some(name) = path.file_name() else {
		return false;
	};
	let Some(name) = name.to_str() else {
		return false;
	};
	!tg::package::ROOT_MODULE_FILE_NAMES.contains(&name)
		&& (name.ends_with(".tg.js") || name.ends_with(".tg.ts"))
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
			Either::Right(path) => tokio::fs::try_exists(path.join(*name_)).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to get the metadata"),
			)?,
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

pub fn try_get_root_module_file_name_for_package_path(
	path: &Path,
) -> tg::Result<Option<&'static str>> {
	let mut name = None;
	for name_ in tg::package::ROOT_MODULE_FILE_NAMES {
		let exists = path.join(name_).try_exists().map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to get the metadata"),
		)?;
		if exists {
			if name.is_some() {
				return Err(tg::error!("found multiple root modules"));
			}
			name = Some(*name_);
		}
	}
	Ok(name)
}

pub fn try_get_nearest_package_path_for_path(path: &Path) -> tg::Result<Option<&Path>> {
	for path in path.ancestors() {
		let metadata = std::fs::symlink_metadata(path).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to get the metadata"),
		)?;
		if metadata.is_dir() && try_get_root_module_file_name_for_package_path(path)?.is_some() {
			return Ok(Some(path));
		}
	}
	Ok(None)
}
