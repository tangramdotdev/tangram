use crate::{directory, Dependency};
use serde_with::{serde_as, DisplayFromStr};
use std::collections::BTreeMap;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Lockfile {
	pub root: usize,
	pub locks: Vec<Lock>,
}

#[serde_as]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Lock {
	#[serde_as(as = "BTreeMap<DisplayFromStr, _>")]
	pub dependencies: BTreeMap<Dependency, Entry>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
pub struct Entry {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub package: Option<directory::Id>,
	pub lock: usize,
}
