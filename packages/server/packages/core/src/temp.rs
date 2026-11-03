use {
	crate::Server,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
	tangram_util::fs::remove,
};

pub struct Temp {
	#[cfg(target_os = "linux")]
	filesystem_project_id: Option<tangram_server_runner_core::project::Id>,
	path: PathBuf,
	preserve: bool,
	server: Server,
}

impl Temp {
	#[must_use]
	pub fn new(server: &Server) -> Self {
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		let id = uuid::Uuid::now_v7();
		let id = ENCODING.encode(&id.into_bytes());
		let path = server.temp_path().join(id);
		let preserve = server.config.advanced.preserve_temp_directories;
		let server = server.clone();
		Self {
			#[cfg(target_os = "linux")]
			filesystem_project_id: None,
			path,
			preserve,
			server,
		}
	}

	#[must_use]
	pub fn path(&self) -> &Path {
		&self.path
	}

	#[cfg(target_os = "linux")]
	pub fn set_filesystem_project_id(
		&mut self,
		project_id: tangram_server_runner_core::project::Id,
	) {
		assert!(self.filesystem_project_id.replace(project_id).is_none());
	}

	/// Move the temp to a path that outlives it, returning its filesystem project ID so that the caller keeps it reserved.
	#[cfg(target_os = "linux")]
	pub async fn persist(
		mut self,
		path: &Path,
	) -> std::io::Result<Option<tangram_server_runner_core::project::Id>> {
		if let Some(parent) = path.parent() {
			tokio::fs::create_dir_all(parent).await?;
		}
		if self.path != path {
			tokio::fs::rename(&self.path, path).await?;
		}
		self.preserve = true;
		self.server.temps.remove(&self.path);
		let project_id = self.filesystem_project_id.take();
		Ok(project_id)
	}

	/// Move the temp to a path that outlives it.
	#[cfg(not(target_os = "linux"))]
	pub async fn persist(mut self, path: &Path) -> std::io::Result<Option<()>> {
		if let Some(parent) = path.parent() {
			tokio::fs::create_dir_all(parent).await?;
		}
		if self.path != path {
			tokio::fs::rename(&self.path, path).await?;
		}
		self.preserve = true;
		self.server.temps.remove(&self.path);
		Ok(None)
	}

	pub async fn remove(&mut self) -> std::io::Result<()> {
		match remove(&self.path).await {
			Ok(()) => {},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
			Err(error) => return Err(error),
		}
		crate::checkpoint!(self.server, "temp.removed", path = %self.path.display()).await;
		self.preserve = true;
		#[cfg(target_os = "linux")]
		if let Some(project_id) = self.filesystem_project_id.take() {
			let path = self.path.parent().unwrap().to_owned();
			tokio::spawn(Self::release_filesystem_project_id(path, project_id));
		}

		Ok(())
	}

	#[cfg(target_os = "linux")]
	async fn release_filesystem_project_id(
		path: PathBuf,
		project_id: tangram_server_runner_core::project::Id,
	) {
		loop {
			match tangram_sandbox::filesystem_project_usage(&path, project_id.value()) {
				Ok(0) => {
					project_id.release();
					return;
				},
				Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
				Err(error) => {
					tracing::error!(%error, "failed to release the filesystem project ID");
					return;
				},
			}
		}
	}
}

impl AsRef<Path> for Temp {
	fn as_ref(&self) -> &Path {
		self.path()
	}
}

impl Drop for Temp {
	fn drop(&mut self) {
		if !self.preserve {
			#[cfg(target_os = "linux")]
			let filesystem_project_id = self.filesystem_project_id.take();
			tokio::spawn({
				let server = self.server.clone();
				let path = self.path.clone();
				async move {
					#[cfg(target_os = "linux")]
					let removed = match remove(&path).await {
						Ok(()) => true,
						Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
						Err(_) => false,
					};
					#[cfg(not(target_os = "linux"))]
					remove(&path).await.ok();
					server.temps.remove(&path);
					#[cfg(target_os = "linux")]
					if removed && let Some(project_id) = filesystem_project_id {
						let parent = path.parent().unwrap().to_owned();
						tokio::spawn(Self::release_filesystem_project_id(parent, project_id));
					}
					crate::checkpoint!(server, "temp.removed", path = %path.display()).await;
				}
			});
		}
	}
}
