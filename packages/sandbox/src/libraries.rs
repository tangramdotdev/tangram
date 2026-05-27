use {
	std::path::{Path, PathBuf},
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

pub(crate) fn resolve(tangram_path: &Path) -> tg::Result<Vec<PathBuf>> {
	platform::resolve(tangram_path)
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
		let source_is_symlink = std::fs::symlink_metadata(source)
			.is_ok_and(|metadata| metadata.file_type().is_symlink());
		if source_is_symlink || std::fs::hard_link(source, &target).is_err() {
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
