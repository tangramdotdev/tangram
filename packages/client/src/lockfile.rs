use crate::{self as tg, util::serde::is_false};
use std::{
	collections::BTreeMap,
	path::{Path, PathBuf},
};
use tangram_either::Either;

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Lockfile {
	pub nodes: Vec<Node>,
}

#[derive(
	Clone,
	Debug,
	Eq,
	PartialEq,
	derive_more::From,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	serde::Serialize,
	serde::Deserialize,
)]
#[try_unwrap(ref)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Node {
	Directory(Directory),
	File(File),
	Symlink(Symlink),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Directory {
	pub entries: BTreeMap<String, Entry>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct File {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub contents: Option<tg::blob::Id>,

	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub dependencies: BTreeMap<tg::Reference, tg::Referent<Entry>>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub executable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
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
				return Err(
					tg::error!(!source, %path = path.display(), "failed to read lockfile"),
				)?;
			},
		};
		let lockfile = serde_json::from_str(&contents).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to deserialize lockfile"),
		)?;
		Ok(Some(lockfile))
	}
}
