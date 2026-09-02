use {
	std::{
		path::{Path, PathBuf},
		time::SystemTime,
	},
	tangram_client::prelude::*,
};

const BUILD_ATTEMPTS: usize = 3;

#[derive(Clone, Debug)]
pub struct Arg {
	pub image_path: PathBuf,
	pub path: PathBuf,
}

pub fn ensure(arg: &Arg) -> tg::Result<bool> {
	let mut root_modified = path_modified(&arg.path)?;
	if !image_needs_create(&arg.image_path, root_modified)? {
		return Ok(false);
	}
	let temp_image_path = arg.image_path.with_extension("squashfs.tmp");
	for _ in 0..BUILD_ATTEMPTS {
		let result = build_image(&arg.path, &temp_image_path);
		if let Err(error) = result {
			std::fs::remove_file(&temp_image_path).ok();
			return Err(error);
		}

		let next_root_modified = match path_modified(&arg.path) {
			Ok(modified) => modified,
			Err(error) => {
				std::fs::remove_file(&temp_image_path).ok();
				return Err(error);
			},
		};
		if root_modified != next_root_modified {
			root_modified = next_root_modified;
			continue;
		}

		if let Err(error) = install_image(&temp_image_path, &arg.image_path) {
			std::fs::remove_file(&temp_image_path).ok();
			return Err(error);
		}
		return Ok(true);
	}
	std::fs::remove_file(&temp_image_path).ok();
	let error = tg::error!(
		attempts = BUILD_ATTEMPTS,
		"the sandbox root did not stabilize while building the VM image"
	);
	Err(error)
}

fn image_needs_create(image_path: &Path, input_modified: SystemTime) -> tg::Result<bool> {
	let image_metadata = match std::fs::metadata(image_path) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
		Err(error) => {
			return Err(tg::error!(
				!error,
				path = %image_path.display(),
				"failed to stat the VM image"
			));
		},
	};
	let image_modified = image_metadata.modified().map_err(|error| {
		tg::error!(
			!error,
			path = %image_path.display(),
			"failed to get the VM image modification time"
		)
	})?;
	let needs_create = image_modified <= input_modified;
	Ok(needs_create)
}

fn build_image(rootfs_path: &Path, temp_image_path: &Path) -> tg::Result<()> {
	if let Some(parent) = temp_image_path.parent() {
		std::fs::create_dir_all(parent).map_err(|error| {
			tg::error!(!error, path = %parent.display(), "failed to create the image parent directory")
		})?;
	}
	std::fs::remove_file(temp_image_path).ok();

	let status = std::process::Command::new("mksquashfs")
		.arg(rootfs_path)
		.arg(temp_image_path)
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
	let modified = SystemTime::now();
	tangram_util::fs::set_modified_sync(temp_image_path, modified).map_err(|error| {
		tg::error!(
			!error,
			path = %temp_image_path.display(),
			"failed to set the VM image modification time"
		)
	})?;
	Ok(())
}

fn install_image(temp_image_path: &Path, image_path: &Path) -> tg::Result<()> {
	std::fs::rename(temp_image_path, image_path).map_err(|error| {
		tg::error!(
			!error,
			src = %temp_image_path.display(),
			dst = %image_path.display(),
			"failed to move the image into place",
		)
	})?;
	if let Some(parent) = image_path.parent() {
		sync_file(parent)?;
	}
	Ok(())
}

fn path_modified(path: &Path) -> tg::Result<SystemTime> {
	let metadata = std::fs::metadata(path).map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to stat a VM image input"),
	)?;
	let modified = metadata.modified().map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to get a VM image input modification time"
		)
	})?;
	Ok(modified)
}

fn sync_file(path: &Path) -> tg::Result<()> {
	let file = std::fs::File::open(path).map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to open a VM image path"),
	)?;
	file.sync_all().map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to sync a VM image path"),
	)?;
	Ok(())
}
