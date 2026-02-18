use {std::path::PathBuf, std::sync::Arc, tangram_client::prelude::*};

#[derive(Clone, Debug, Default)]
pub struct Context {
	pub process: Option<Arc<Process>>,
	pub token: Option<String>,
	pub untrusted: bool,
}

#[derive(Clone, Debug)]
pub struct Process {
	pub id: tg::process::Id,
	pub paths: Option<Paths>,
	pub remote: Option<String>,
	pub retry: bool,
}

#[derive(Clone, Debug)]
pub struct Paths {
	pub server_guest: PathBuf,
	pub server_host: PathBuf,
	pub output_guest: PathBuf,
	pub output_host: PathBuf,
	pub root_host: PathBuf,
}

impl Process {
	pub fn host_path_for_guest_path(&self, path: PathBuf) -> PathBuf {
		let Some(path_map) = &self.paths else {
			return path;
		};
		if let Ok(path) = path.strip_prefix(&path_map.output_guest) {
			path_map.output_host.join(path)
		} else if let Ok(path) = path.strip_prefix(&path_map.server_guest) {
			path_map.server_host.join(path)
		} else {
			path_map
				.root_host
				.join(path.strip_prefix("/").unwrap_or(path.as_ref()))
		}
	}

	pub fn guest_path_for_host_path(&self, path: PathBuf) -> tg::Result<PathBuf> {
		let Some(paths) = &self.paths else {
			return Ok(path);
		};
		let path = if path.starts_with(&paths.output_host) {
			let suffix = path.strip_prefix(&paths.output_host).unwrap();
			paths.output_guest.join(suffix)
		} else if path.starts_with(&paths.server_host) {
			let suffix = path.strip_prefix(&paths.server_host).unwrap();
			paths.server_guest.join(suffix)
		} else {
			let suffix = path.strip_prefix(&paths.root_host).map_err(|error| {
				tg::error!(source = error, "cannot map path outside of host root")
			})?;
			PathBuf::from("/").join(suffix)
		};
		Ok(path)
	}
}
