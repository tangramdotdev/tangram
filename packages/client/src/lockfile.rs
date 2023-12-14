use crate::{directory, Dependency};
use std::collections::BTreeMap;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Lockfile {
	pub root: usize,
	pub locks: Vec<Lock>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Lock {
	pub dependencies: BTreeMap<Dependency, Entry>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
pub struct Entry {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub package: Option<directory::Id>,
	pub lock: usize,
}
