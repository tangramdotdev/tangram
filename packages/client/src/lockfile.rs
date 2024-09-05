use crate::{self as tg, util::serde::is_false};
use std::collections::BTreeMap;
use tangram_either::Either;
use tokio::io::AsyncWriteExt;
pub const TANGRAM_LOCKFILE_FILE_NAME: &str = "tangram.lock";

#[derive(Default, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Lockfile {
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub paths: BTreeMap<tg::Path, usize>,
	pub nodes: Vec<Node>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Node {
	Directory {
		entries: BTreeMap<String, Entry>,
	},

	File {
		#[serde(default, skip_serializing_if = "Option::is_none")]
		contents: Option<tg::blob::Id>,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		dependencies: Option<BTreeMap<tg::Reference, Entry>>,

		#[serde(default, skip_serializing_if = "is_false")]
		executable: bool,
	},

	Symlink {
		#[serde(default, skip_serializing_if = "Option::is_none")]
		artifact: Option<Entry>,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		path: Option<tg::Path>,
	},
}

pub type Entry = Option<Either<usize, tg::object::Id>>;

impl Lockfile {
	pub async fn try_read(path: &tg::Path) -> tg::Result<Option<Self>> {
		let contents = match tokio::fs::read_to_string(&path).await {
			Ok(contents) => contents,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
			Err(source) => return Err(tg::error!(!source, %path, "failed to read lockfile"))?,
		};
		let lockfile = serde_json::from_str(&contents)
			.map_err(|source| tg::error!(!source, %path, "failed to deserialize lockfile"))?;
		Ok(Some(lockfile))
	}

	// pub async fn write(&self, path: &tg::Path) -> tg::Result<()> {
	// 	let root_module_path = get_root_module_path_for_path(path.as_ref()).await?;
	// 	let lockfile_path = root_module_path.parent().normalize();
	// 	let contents = serde_json::to_vec_pretty(self)
	// 		.map_err(|source| tg::error!(!source, "failed to serialize lockfile"))?;
	// 	tokio::fs::File::options()
	// 		.create(true)
	// 		.append(false)
	// 		.truncate(true)
	// 		.open(&lockfile_path)
	// 		.await
	// 		.map_err(
	// 			|source| tg::error!(!source, %path = lockfile_path, "failed to open lockfile"),
	// 		)?
	// 		.write_all(&contents)
	// 		.await
	// 		.map_err(
	// 			|source| tg::error!(!source, %path = lockfile_path, "failed to write lockfile"),
	// 		)?;
	// 	Ok(())
	// }
}

impl Node {
	#[must_use]
	pub fn children(&self) -> Vec<Either<usize, tg::object::Id>> {
		match self {
			Self::Directory { entries } => entries.values().flatten().cloned().collect(),
			Self::File { dependencies, .. } => dependencies
				.iter()
				.flatten()
				.filter_map(|(_, e)| e.clone())
				.collect(),
			Self::Symlink { artifact, .. } => {
				let Some(Some(artifact)) = artifact else {
					return Vec::new();
				};
				vec![artifact.clone()]
			},
		}
	}
}
