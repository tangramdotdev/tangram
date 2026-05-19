use {
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Arg {
	pub image_path: PathBuf,
	pub path: PathBuf,
	pub tangram_path: PathBuf,
}

pub fn ensure(arg: &Arg) -> tg::Result<bool> {
	if !image_needs_create(&arg.image_path, &arg.tangram_path)? {
		return Ok(false);
	}
	build_image(&arg.path, &arg.tangram_path, &arg.image_path)?;
	Ok(true)
}

fn build_image(rootfs_path: &Path, tangram_path: &Path, image_path: &Path) -> tg::Result<()> {
	restore_runtime_library_links(rootfs_path)?;

	let libexec_path = rootfs_path.join("opt/tangram/libexec/tangram");
	std::fs::copy(tangram_path, &libexec_path).map_err(|error| {
		tg::error!(
			!error,
			src = %tangram_path.display(),
			dst = %libexec_path.display(),
			"failed to stage the tangram binary into the rootfs",
		)
	})?;

	if let Some(parent) = image_path.parent() {
		std::fs::create_dir_all(parent).map_err(|error| {
			tg::error!(!error, path = %parent.display(), "failed to create the image parent directory")
		})?;
	}
	let temp_image_path = image_path.with_extension("squashfs.tmp");
	std::fs::remove_file(&temp_image_path).ok();

	let status = std::process::Command::new("mksquashfs")
		.arg(rootfs_path)
		.arg(&temp_image_path)
		.arg("-comp")
		.arg("zstd")
		.arg("-all-root")
		.arg("-noappend")
		.arg("-no-progress")
		.arg("-quiet")
		.status()
		.map_err(|error| tg::error!(!error, "failed to invoke mksquashfs"))?;
	if !status.success() {
		return Err(tg::error!(%status, "mksquashfs failed"));
	}
	std::fs::rename(&temp_image_path, image_path).map_err(|error| {
		tg::error!(
			!error,
			src = %temp_image_path.display(),
			dst = %image_path.display(),
			"failed to move the image into place",
		)
	})?;
	Ok(())
}

fn image_needs_create(image_path: &Path, tangram_path: &Path) -> tg::Result<bool> {
	let image_metadata = match std::fs::metadata(image_path) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
		Err(error) => {
			return Err(tg::error!(
				!error,
				path = %image_path.display(),
				"failed to stat the vm image"
			));
		},
	};
	let image_modified = image_metadata.modified().map_err(|error| {
		tg::error!(
			!error,
			path = %image_path.display(),
			"failed to get the vm image modification time"
		)
	})?;
	let tangram_modified = std::fs::metadata(tangram_path)
		.and_then(|metadata| metadata.modified())
		.map_err(|error| {
			tg::error!(
				!error,
				path = %tangram_path.display(),
				"failed to get the tangram executable modification time"
			)
		})?;
	Ok(tangram_modified > image_modified)
}

fn restore_runtime_library_links(rootfs_path: &Path) -> tg::Result<()> {
	let lib64_path = rootfs_path.join("lib64");
	std::fs::remove_file(&lib64_path).ok();
	std::fs::remove_dir_all(&lib64_path).ok();
	std::os::unix::fs::symlink("/opt/tangram/lib", &lib64_path)
		.map_err(|error| tg::error!(!error, "failed to restore the lib64 symlink"))?;

	let usr_path = rootfs_path.join("usr");
	std::fs::create_dir_all(&usr_path).map_err(
		|error| tg::error!(!error, path = %usr_path.display(), "failed to create the usr directory"),
	)?;
	let usr_lib_path = usr_path.join("lib");
	std::fs::remove_file(&usr_lib_path).ok();
	std::fs::remove_dir_all(&usr_lib_path).ok();
	std::os::unix::fs::symlink("/opt/tangram/lib", &usr_lib_path)
		.map_err(|error| tg::error!(!error, "failed to restore the usr lib symlink"))?;

	Ok(())
}
