use {
	std::path::{Path, PathBuf},
	std::sync::Arc,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, Default)]
pub struct Context {
	pub process: Option<Arc<Process>>,
	pub token: Option<String>,
	pub untrusted: bool,
}

#[derive(Clone, Debug)]
pub struct Process {
	pub id: tg::process::Id,
	pub path_maps: Option<Vec<PathMap>>,
	pub remote: Option<String>,
	pub retry: bool,
}

#[derive(Clone, Debug)]
pub struct PathMap {
	pub host: PathBuf,
	pub guest: PathBuf,
}

impl Process {
	pub fn host_path_for_guest_path(&self, path: &Path) -> Option<PathBuf> {
		let Some(path_maps) = &self.path_maps else {
			return Some(path.to_owned());
		};
		let path_maps = path_maps
			.iter()
			.map(|path_map| (path_map.guest.as_path(), path_map.host.as_path()));
		Self::map_path(path, path_maps)
	}

	pub fn guest_path_for_host_path(&self, path: &Path) -> Option<PathBuf> {
		let Some(path_maps) = &self.path_maps else {
			return Some(path.to_owned());
		};
		let path_maps = path_maps
			.iter()
			.map(|path_map| (path_map.host.as_path(), path_map.guest.as_path()));
		Self::map_path(path, path_maps)
	}

	fn map_path<'a>(
		path: &Path,
		path_maps: impl Iterator<Item = (&'a Path, &'a Path)>,
	) -> Option<PathBuf> {
		let mut best: Option<(&Path, &Path, usize)> = None;
		for (from, to) in path_maps {
			if !path.starts_with(from) {
				continue;
			}
			let len = from.components().count();
			if best.is_none_or(|(_, _, best_len)| len > best_len) {
				best = Some((from, to, len));
			}
		}
		let (from, to, _) = best?;
		let suffix = path.strip_prefix(from).unwrap();
		Some(to.join(suffix))
	}
}
