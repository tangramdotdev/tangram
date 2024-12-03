use crate::{self as tg, util::serde::is_false};
use std::{
	collections::BTreeMap,
	path::{Path, PathBuf},
};
use tangram_either::Either;

#[derive(Default, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Lockfile {
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

		#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
		dependencies: BTreeMap<tg::Reference, tg::Referent<Entry>>,

		#[serde(default, skip_serializing_if = "is_false")]
		executable: bool,
	},

	Symlink(Symlink),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Symlink {
	Target {
		target: PathBuf,
	},

	Artifact {
		artifact: Entry,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		subpath: Option<PathBuf>,
	},
}

pub type Entry = Either<usize, tg::object::Id>;

impl Lockfile {
	pub async fn try_read(path: &Path) -> tg::Result<Option<Self>> {
		let contents = match tokio::fs::read_to_string(&path).await {
			Ok(contents) => contents,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
			Err(source) => {
				return Err(tg::error!(!source, %path = path.display(), "failed to read lockfile"))?
			},
		};
		let lockfile = serde_json::from_str(&contents).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to deserialize lockfile"),
		)?;
		Ok(Some(lockfile))
	}
}
