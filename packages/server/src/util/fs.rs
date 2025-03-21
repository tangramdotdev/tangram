use futures::TryFutureExt as _;
use std::{
	os::unix::fs::PermissionsExt,
	path::{Path, PathBuf},
};

pub async fn canonicalize_parent(path: impl AsRef<Path>) -> std::io::Result<PathBuf> {
	let path = path.as_ref();
	if !path.is_absolute() {
		return Err(std::io::Error::other("path must be absolute"));
	}
	let parent = path.parent().unwrap();
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

pub async fn remove(path: impl AsRef<Path>) -> std::io::Result<()> {
	fn remove_sync(path: &PathBuf) -> std::io::Result<()> {
		let metadata = std::fs::symlink_metadata(path)?;
		if metadata.permissions().readonly() {
			let mut permissions = metadata.permissions().clone();
			permissions.set_readonly(false);
			std::fs::set_permissions(&path, permissions).ok();
		}
		if metadata.is_dir() {
			for entry in std::fs::read_dir(path)? {
				remove_sync(&entry?.path())?;
			}
			return std::fs::remove_dir(path);
		}
		std::fs::remove_file(path)
	}

	let path = path.as_ref().to_owned();
	tokio::task::spawn_blocking(move || remove_sync(&path))
		.await
		.unwrap()
}
