use {
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

#[cfg(feature = "foundationdb")]
const RUNTIME_LIBRARIES: &[&str] = &["libfdb_c"];

#[cfg(not(feature = "foundationdb"))]
const RUNTIME_LIBRARIES: &[&str] = &[];

pub(crate) fn resolve(_tangram_path: &Path) -> tg::Result<Vec<PathBuf>> {
	let mut libraries = Vec::new();
	for stem in RUNTIME_LIBRARIES {
		let path = resolve_library(stem)?;
		libraries.push(path);
	}
	Ok(libraries)
}

pub(crate) fn stage(target_dir: &Path, sources: &[PathBuf]) -> tg::Result<()> {
	if sources.is_empty() {
		return Ok(());
	}
	std::fs::create_dir_all(target_dir)
		.map_err(|error| tg::error!(!error, "failed to create the sandbox libraries directory"))?;
	#[cfg(target_os = "linux")]
	let permissions = <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o755);
	for source in sources {
		let name = source.file_name().ok_or_else(|| {
			tg::error!(
				path = %source.display(),
				"failed to get the dynamic library file name"
			)
		})?;
		let target = target_dir.join(name);
		if target.exists() {
			continue;
		}
		if std::fs::hard_link(source, &target).is_err() {
			std::fs::copy(source, &target).map_err(|error| {
				tg::error!(
					!error,
					source = %source.display(),
					target = %target.display(),
					"failed to stage the dynamic library"
				)
			})?;
		}
		#[cfg(target_os = "linux")]
		std::fs::set_permissions(&target, permissions.clone()).map_err(|error| {
			tg::error!(
				!error,
				path = %target.display(),
				"failed to set sandbox file permissions"
			)
		})?;
	}
	Ok(())
}

fn resolve_library(stem: &str) -> tg::Result<PathBuf> {
	let file_name = library_file_name(stem);
	if let Some(path) = resolve_from_fdb_lib_path(&file_name) {
		return Ok(path);
	}
	if let Some(path) = resolve_from_library_path_env(&file_name) {
		return Ok(path);
	}
	for directory in default_library_directories() {
		let path = PathBuf::from(directory).join(&file_name);
		if path.exists() {
			return Ok(path);
		}
	}
	Err(tg::error!(
		library = %stem,
		"failed to find a runtime library; set FDB_LIB_PATH or install the FoundationDB client libraries"
	))
}

fn library_file_name(stem: &str) -> String {
	#[cfg(target_os = "macos")]
	{
		format!("{stem}.dylib")
	}
	#[cfg(target_os = "linux")]
	{
		format!("{stem}.so")
	}
}

fn resolve_from_fdb_lib_path(file_name: &str) -> Option<PathBuf> {
	let fdb_lib_path = std::env::var_os("FDB_LIB_PATH")?;
	let path = PathBuf::from(fdb_lib_path);
	if path.is_file() {
		return path.exists().then_some(path);
	}
	let candidate = path.join(file_name);
	candidate.exists().then_some(candidate)
}

fn resolve_from_library_path_env(file_name: &str) -> Option<PathBuf> {
	#[cfg(target_os = "macos")]
	let variables = ["DYLD_LIBRARY_PATH"];
	#[cfg(target_os = "linux")]
	let variables = ["LD_LIBRARY_PATH"];
	for variable in variables {
		let Some(value) = std::env::var_os(variable) else {
			continue;
		};
		for directory in std::env::split_paths(&value) {
			let candidate = directory.join(file_name);
			if candidate.exists() {
				return Some(candidate);
			}
		}
	}
	None
}

fn default_library_directories() -> &'static [&'static str] {
	&[
		"/usr/local/lib",
		#[cfg(target_os = "linux")]
		"/usr/lib",
		#[cfg(target_os = "linux")]
		"/lib",
	]
}
