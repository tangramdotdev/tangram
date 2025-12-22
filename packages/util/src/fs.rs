use {
	futures::FutureExt as _,
	std::{
		os::unix::fs::{MetadataExt, PermissionsExt as _},
		path::{Path, PathBuf},
	},
};

pub async fn canonicalize_parent(path: impl AsRef<Path>) -> std::io::Result<PathBuf> {
	let path = std::path::absolute(path)?;
	let Some(parent) = path.parent() else {
		return Ok(path);
	};
	let last = path.components().next_back().unwrap();
	let mut path = tokio::fs::canonicalize(parent).await?;
	match last {
		std::path::Component::Prefix(_) | std::path::Component::RootDir => {
			return Err(std::io::Error::other("invalid last component"));
		},
		std::path::Component::CurDir => (),
		std::path::Component::ParentDir => {
			if path != Path::new("/") {
				path = path.parent().unwrap().to_owned();
			}
		},
		std::path::Component::Normal(component) => {
			path.push(component);
		},
	}
	Ok(path)
}

pub fn canonicalize_parent_sync(path: impl AsRef<Path>) -> std::io::Result<PathBuf> {
	let path = std::path::absolute(path)?;
	let Some(parent) = path.parent() else {
		return Ok(path);
	};
	let last = path.components().next_back().unwrap();
	let mut path = std::fs::canonicalize(parent)?;
	match last {
		std::path::Component::Prefix(_) | std::path::Component::RootDir => {
			return Err(std::io::Error::other("invalid last component"));
		},
		std::path::Component::CurDir => (),
		std::path::Component::ParentDir => {
			if path != Path::new("/") {
				path = path.parent().unwrap().to_owned();
			}
		},
		std::path::Component::Normal(component) => {
			path.push(component);
		},
	}
	Ok(path)
}

pub async fn remove(path: impl AsRef<Path>) -> std::io::Result<()> {
	let path = path.as_ref().to_owned();
	tokio::task::spawn_blocking(move || remove_sync(&path))
		.map(|result| match result {
			Err(error) if error.is_cancelled() => {
				tracing::error!(?error);
				Ok(Ok(()))
			},
			_ => result,
		})
		.await
		.unwrap()
}

pub fn remove_sync(path: impl AsRef<Path>) -> std::io::Result<()> {
	fn inner(path: &Path) -> std::io::Result<()> {
		// Get the metadata.
		let metadata = std::fs::symlink_metadata(path)?;

		if !metadata.is_symlink() {
			// Set permissions +rw.
			let mode = metadata.mode();
			let permissions = std::fs::Permissions::from_mode(mode | 0o666);
			std::fs::set_permissions(path, permissions)?;
		}

		// Recurse if necessary.
		if metadata.is_dir() {
			for entry in std::fs::read_dir(path)? {
				inner(&entry?.path())?;
			}
			std::fs::remove_dir(path)?;
		} else {
			std::fs::remove_file(path)?;
		}

		Ok(())
	}
	inner(path.as_ref())
}

/// Rename a file or directory atomically, failing if the destination already exists.
///
/// Uses `renameatx_np` with `RENAME_EXCL` on macOS and `renameat2` with `RENAME_NOREPLACE` on Linux.
pub fn rename_noreplace_sync(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> std::io::Result<()> {
	// #[cfg(target_os = "macos")]
	// {
	// 	let src = std::ffi::CString::new(src.as_ref().as_os_str().as_encoded_bytes())
	// 		.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
	// 	let dst = std::ffi::CString::new(dst.as_ref().as_os_str().as_encoded_bytes())
	// 		.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
	// 	const RENAME_EXCL: libc::c_uint = 1;
	// 	let result = unsafe {
	// 		libc::renameatx_np(
	// 			libc::AT_FDCWD,
	// 			src.as_ptr(),
	// 			libc::AT_FDCWD,
	// 			dst.as_ptr(),
	// 			RENAME_EXCL,
	// 		)
	// 	};
	// 	if result != 0 {
	// 		let err = std::io::Error::last_os_error();
	// 		return Err(err);
	// 	}
	// }

	#[cfg(target_os = "macos")]
	{
		std::fs::rename(src, dst)?;
	}

	#[cfg(target_os = "linux")]
	{
		rustix::fs::renameat_with(
			rustix::fs::CWD,
			src.as_ref(),
			rustix::fs::CWD,
			dst.as_ref(),
			rustix::fs::RenameFlags::NOREPLACE,
		)
		.map_err(std::io::Error::from)?;
	}

	Ok(())
}

/// Rename a file or directory atomically, failing if the destination already exists.
///
/// Uses `renameatx_np` with `RENAME_EXCL` on macOS and `renameat2` with `RENAME_NOREPLACE` on Linux.
pub async fn rename_noreplace(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> std::io::Result<()> {
	let src = src.as_ref().to_owned();
	let dst = dst.as_ref().to_owned();
	tokio::task::spawn_blocking(move || rename_noreplace_sync(&src, &dst))
		.await
		.map_err(std::io::Error::other)?
}
