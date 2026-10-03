use {
	std::{
		collections::{BTreeMap, btree_map::Entry},
		ffi::OsString,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "macos")]
use self::darwin as platform;
#[cfg(target_os = "linux")]
use self::linux as platform;

#[derive(Clone, Debug)]
pub(crate) struct Library {
	pub name: OsString,
	pub source: PathBuf,
}

pub(crate) fn resolve() -> tg::Result<Vec<Library>> {
	let sources = platform::resolve()?;
	let mut libraries = BTreeMap::<OsString, PathBuf>::new();
	for source in sources {
		let name = library_name(&source)?;
		let source = std::fs::canonicalize(&source).map_err(|error| {
			tg::error!(
				!error,
				path = %source.display(),
				"failed to resolve the dynamic library path"
			)
		})?;
		match libraries.entry(name.clone()) {
			Entry::Occupied(entry) if entry.get() != &source => {
				return Err(tg::error!(
					name = %Path::new(&name).display(),
					path_a = %entry.get().display(),
					path_b = %source.display(),
					"found conflicting dynamic libraries"
				));
			},
			Entry::Occupied(_) => (),
			Entry::Vacant(entry) => {
				entry.insert(source);
			},
		}
	}
	let libraries = libraries
		.into_iter()
		.map(|(name, source)| Library { name, source })
		.collect();
	Ok(libraries)
}

pub(crate) fn stage(target_dir: &Path, libraries: &[Library]) -> tg::Result<()> {
	if libraries.is_empty() {
		return Ok(());
	}
	std::fs::create_dir_all(target_dir)
		.map_err(|error| tg::error!(!error, "failed to create the sandbox libraries directory"))?;
	#[cfg(target_os = "linux")]
	let permissions = <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o755);
	for library in libraries {
		let target = target_dir.join(&library.name);
		if target.exists() {
			continue;
		}
		std::fs::create_dir_all(target.parent().unwrap()).map_err(|error| {
			tg::error!(!error, "failed to create the runtime library directory")
		})?;
		if std::fs::hard_link(&library.source, &target).is_err() {
			std::fs::copy(&library.source, &target).map_err(|error| {
				tg::error!(
					!error,
					source = %library.source.display(),
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

fn library_name(source: &Path) -> tg::Result<OsString> {
	// Preserve framework paths so dyld can resolve staged framework dependencies.
	#[cfg(target_os = "macos")]
	if let Some(index) = source.components().position(|component| {
		component
			.as_os_str()
			.to_string_lossy()
			.ends_with(".framework")
	}) {
		let path: PathBuf = source.components().skip(index).collect();
		return Ok(path.into_os_string());
	}
	let name = source
		.file_name()
		.ok_or_else(
			|| tg::error!(path = %source.display(), "failed to get the dynamic library file name"),
		)?
		.to_owned();
	Ok(name)
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
	use super::*;

	#[test]
	fn framework_library_names_preserve_the_version_path() {
		let path = Path::new("/opt/python/Python.framework/Versions/3.14/Python");
		assert_eq!(
			library_name(path).unwrap(),
			"Python.framework/Versions/3.14/Python"
		);
		assert_eq!(
			library_name(Path::new("/opt/lib/libexample.dylib")).unwrap(),
			"libexample.dylib"
		);
	}
}
