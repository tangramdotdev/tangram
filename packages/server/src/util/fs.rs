use futures::TryFutureExt as _;
use std::path::{Path, PathBuf};

pub async fn canonicalize_parent(path: impl AsRef<Path>) -> std::io::Result<PathBuf> {
	let path = path.as_ref();
	if !path.is_absolute() {
		return Err(std::io::Error::other("path must be absolute"));
	}
	let parent = path.parent().unwrap();
	let last = path.components().last().unwrap();
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
	};
	Ok(path)
}

pub async fn remove(path: impl AsRef<Path>) -> std::io::Result<()> {
	let path = path.as_ref();
	tokio::fs::remove_file(path)
		.or_else(|_| tokio::fs::remove_dir_all(path))
		.await
}

#[cfg(test)]
pub async fn cleanup(temp: tangram_temp::Temp, server: crate::Server) {
	server.stop();
	server.wait().await;
	temp.remove().await.ok();
}
