use {
	crate::Server,
	std::path::{Path, PathBuf},
	tangram_util::fs::remove,
};

pub struct Temp {
	#[cfg(target_os = "linux")]
	filesystem_project_id: Option<crate::runner::project::Id>,
	path: PathBuf,
	preserve: bool,
	server: Server,
}

impl Temp {
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

	pub fn path(&self) -> &Path {
		&self.path
	}

	#[cfg(target_os = "linux")]
	pub(crate) fn set_filesystem_project_id(&mut self, project_id: crate::runner::project::Id) {
		assert!(self.filesystem_project_id.replace(project_id).is_none());
	}

	pub async fn remove(&mut self) -> std::io::Result<()> {
		match remove(&self.path).await {
			Ok(()) => {},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
			Err(error) => return Err(error),
		}
		self.preserve = true;
		#[cfg(target_os = "linux")]
		if let Some(project_id) = self.filesystem_project_id.take() {
			project_id.release();
		}

		Ok(())
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
						project_id.release();
					}
				}
			});
		}
	}
}
