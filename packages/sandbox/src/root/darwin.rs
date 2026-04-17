use {
	super::Arg,
	std::{
		os::unix::ffi::OsStrExt as _,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

pub fn prepare_runtime_libraries(arg: &Arg) -> tg::Result<()> {
	std::fs::remove_dir_all(&arg.path).ok();
	std::fs::create_dir_all(&arg.path)
		.map_err(|source| tg::error!(!source, "failed to create the sandbox directory"))?;
	let libraries = collect_dynamic_libraries(&arg.tangram_path)?;
	let libraries_path = arg.path.join("lib");
	if !libraries.is_empty() {
		std::fs::create_dir_all(&libraries_path).map_err(|source| {
			tg::error!(!source, "failed to create the sandbox libraries directory")
		})?;
		for source in libraries {
			let name = source.file_name().ok_or_else(|| {
				tg::error!(
					path = %source.display(),
					"failed to get the dynamic library file name"
				)
			})?;
			let target = libraries_path.join(name);
			if target.exists() {
				continue;
			}
			if std::fs::hard_link(&source, &target).is_err() {
				std::fs::copy(&source, &target).map_err(|error| {
					tg::error!(
						!error,
						source = %source.display(),
						target = %target.display(),
						"failed to stage the dynamic library"
					)
				})?;
			}
		}
	}
	prepare_tangram_wrapper(arg, &libraries_path)?;
	Ok(())
}

fn prepare_tangram_wrapper(arg: &Arg, libraries_path: &Path) -> tg::Result<()> {
	let bin_path = arg.path.join("bin");
	std::fs::create_dir_all(&bin_path)
		.map_err(|source| tg::error!(!source, "failed to create the sandbox bin directory"))?;
	let tangram_wrapper_path = bin_path.join("tangram");
	let tangram_path = shell_quote(&arg.tangram_path);
	let libraries_path = shell_quote(libraries_path);
	let wrapper = format!(
		r#"#!/bin/sh
if [ -n "${{DYLD_LIBRARY_PATH:-}}" ]; then
	export DYLD_LIBRARY_PATH='{libraries_path}':"$DYLD_LIBRARY_PATH"
else
	export DYLD_LIBRARY_PATH='{libraries_path}'
fi
if [ -n "${{DYLD_FALLBACK_LIBRARY_PATH:-}}" ]; then
	export DYLD_FALLBACK_LIBRARY_PATH='{libraries_path}':"$DYLD_FALLBACK_LIBRARY_PATH"
else
	export DYLD_FALLBACK_LIBRARY_PATH='{libraries_path}'
fi
exec '{tangram_path}' "$@"
"#,
	);
	std::fs::write(&tangram_wrapper_path, wrapper)
		.map_err(|source| tg::error!(!source, "failed to write the tangram wrapper"))?;
	let permissions = std::os::unix::fs::PermissionsExt::from_mode(0o755);
	std::fs::set_permissions(&tangram_wrapper_path, permissions)
		.map_err(|source| tg::error!(!source, "failed to set the tangram wrapper permissions"))?;
	let tg_wrapper_path = bin_path.join("tg");
	std::fs::remove_file(&tg_wrapper_path).ok();
	std::os::unix::fs::symlink("tangram", &tg_wrapper_path)
		.map_err(|source| tg::error!(!source, "failed to create the tg wrapper symlink"))?;
	Ok(())
}

fn shell_quote(path: &Path) -> String {
	path.to_string_lossy().replace('\'', r"'\''")
}

fn collect_dynamic_libraries(executable: &Path) -> tg::Result<Vec<PathBuf>> {
	let executable = std::fs::canonicalize(executable).map_err(|source| {
		tg::error!(
			!source,
			path = %executable.display(),
			"failed to canonicalize the executable path"
		)
	})?;
	let loaded_images = loaded_images();
	let mut queue = std::collections::VecDeque::from([executable]);
	let mut visited = std::collections::BTreeSet::new();
	let mut libraries = std::collections::BTreeSet::new();
	while let Some(path) = queue.pop_front() {
		if !visited.insert(path.clone()) {
			continue;
		}
		let rpaths = otool_rpaths(&path)?;
		let dependencies = otool_dependencies(&path)?;
		for dependency in dependencies {
			if path
				.extension()
				.is_some_and(|extension| extension == "dylib")
				&& Path::new(&dependency).file_name() == path.file_name()
			{
				continue;
			}
			let Some(dependency) =
				resolve_dynamic_library(&path, &rpaths, &loaded_images, &dependency)?
			else {
				continue;
			};
			if libraries.insert(dependency.clone()) {
				queue.push_back(dependency);
			}
		}
	}
	Ok(libraries.into_iter().collect())
}

fn otool_dependencies(path: &Path) -> tg::Result<Vec<String>> {
	let output = std::process::Command::new("otool")
		.args(["-L"])
		.arg(path)
		.output()
		.map_err(|source| {
			tg::error!(
				!source,
				path = %path.display(),
				"failed to execute `otool -L`"
			)
		})?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		let stdout = String::from_utf8_lossy(&output.stdout);
		return Err(tg::error!(
			status = %output.status,
			path = %path.display(),
			stderr = %stderr.trim(),
			stdout = %stdout.trim(),
			"`otool -L` failed"
		));
	}
	let stdout = String::from_utf8(output.stdout)
		.map_err(|source| tg::error!(!source, "failed to parse the `otool -L` output"))?;
	let mut dependencies = Vec::new();
	for line in stdout.lines().skip(1) {
		let line = line.trim();
		if line.is_empty() {
			continue;
		}
		let dependency = line
			.split_once(" (")
			.map_or(line, |(dependency, _)| dependency);
		dependencies.push(dependency.to_owned());
	}
	Ok(dependencies)
}

fn otool_rpaths(path: &Path) -> tg::Result<Vec<PathBuf>> {
	let output = std::process::Command::new("otool")
		.args(["-l"])
		.arg(path)
		.output()
		.map_err(|source| {
			tg::error!(
				!source,
				path = %path.display(),
				"failed to execute `otool -l`"
			)
		})?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		let stdout = String::from_utf8_lossy(&output.stdout);
		return Err(tg::error!(
			status = %output.status,
			path = %path.display(),
			stderr = %stderr.trim(),
			stdout = %stdout.trim(),
			"`otool -l` failed"
		));
	}
	let stdout = String::from_utf8(output.stdout)
		.map_err(|source| tg::error!(!source, "failed to parse the `otool -l` output"))?;
	let mut rpaths = Vec::new();
	let mut in_rpath = false;
	for line in stdout.lines() {
		let line = line.trim();
		if in_rpath && line.starts_with("path ") {
			let rpath = line.strip_prefix("path ").unwrap();
			let rpath = rpath.split_once(" (").map_or(rpath, |(rpath, _)| rpath);
			rpaths.push(resolve_dyld_path(path, rpath));
			in_rpath = false;
			continue;
		}
		in_rpath = line == "cmd LC_RPATH";
	}
	Ok(rpaths)
}

fn resolve_dynamic_library(
	binary: &Path,
	rpaths: &[PathBuf],
	loaded_images: &[PathBuf],
	dependency: &str,
) -> tg::Result<Option<PathBuf>> {
	let resolved = if let Some(suffix) = dependency.strip_prefix("@rpath/") {
		let path =
			resolve_rpath_dynamic_library(rpaths, loaded_images, suffix).ok_or_else(|| {
				tg::error!(
					binary = %binary.display(),
					%dependency,
					"failed to resolve the dynamic library"
				)
			})?;
		std::fs::canonicalize(&path).map_err(|source| {
			tg::error!(
				!source,
				path = %path.display(),
				"failed to canonicalize the dynamic library path"
			)
		})?
	} else {
		let path = resolve_dyld_path(binary, dependency);
		if is_system_library_path(&path) {
			return Ok(None);
		}
		std::fs::canonicalize(&path).map_err(|source| {
			tg::error!(
				!source,
				path = %path.display(),
				"failed to canonicalize the dynamic library path"
			)
		})?
	};
	if is_system_library_path(&resolved) {
		return Ok(None);
	}
	Ok(Some(resolved))
}

fn resolve_rpath_dynamic_library(
	rpaths: &[PathBuf],
	loaded_images: &[PathBuf],
	suffix: &str,
) -> Option<PathBuf> {
	resolve_dynamic_library_in_directories(rpaths, suffix)
		.or_else(|| resolve_dynamic_library_in_dyld_environment(suffix))
		.or_else(|| resolve_dynamic_library_in_loaded_images(loaded_images, suffix))
}

fn resolve_dynamic_library_in_directories(
	directories: &[PathBuf],
	suffix: &str,
) -> Option<PathBuf> {
	directories
		.iter()
		.map(|directory| directory.join(suffix))
		.find(|path| path.exists())
}

fn resolve_dynamic_library_in_dyld_environment(suffix: &str) -> Option<PathBuf> {
	let file_name = Path::new(suffix).file_name()?;
	[
		"DYLD_LIBRARY_PATH",
		"DYLD_FALLBACK_LIBRARY_PATH",
		"FDB_LIB_PATH",
	]
	.into_iter()
	.filter_map(std::env::var_os)
	.flat_map(|value| std::env::split_paths(&value).collect::<Vec<_>>())
	.map(|directory| directory.join(file_name))
	.find(|path| path.exists())
}

fn loaded_images() -> Vec<PathBuf> {
	unsafe extern "C" {
		fn _dyld_image_count() -> u32;
		fn _dyld_get_image_name(index: u32) -> *const libc::c_char;
	}
	let image_count = unsafe { _dyld_image_count() };
	(0..image_count)
		.filter_map(|index| {
			let path = unsafe { _dyld_get_image_name(index) };
			if path.is_null() {
				return None;
			}
			let path = unsafe { std::ffi::CStr::from_ptr(path) };
			let path = std::ffi::OsStr::from_bytes(path.to_bytes());
			let path = PathBuf::from(path);
			path.exists().then_some(path)
		})
		.collect()
}

fn resolve_dynamic_library_in_loaded_images(
	loaded_images: &[PathBuf],
	suffix: &str,
) -> Option<PathBuf> {
	let file_name = Path::new(suffix).file_name()?;
	loaded_images
		.iter()
		.find(|path| path.file_name() == Some(file_name))
		.cloned()
}

fn resolve_dyld_path(binary: &Path, path: &str) -> PathBuf {
	let parent = binary.parent().unwrap();
	if path == "@executable_path" || path == "@loader_path" {
		return parent.to_owned();
	}
	if let Some(path) = path.strip_prefix("@executable_path/") {
		return parent.join(path);
	}
	if let Some(path) = path.strip_prefix("@loader_path/") {
		return parent.join(path);
	}
	path.into()
}

fn is_system_library_path(path: &Path) -> bool {
	path.starts_with("/Library/Apple/System/")
		|| path.starts_with("/System/")
		|| path.starts_with("/System/iOSSupport/")
		|| path.starts_with("/usr/lib/")
		|| path.starts_with("/System/Volumes/Preboot/Cryptexes/OS/usr/lib/")
}
