use crate as tg;
use std::path::PathBuf;

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Referent<T> {
	pub item: T,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub subpath: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tag: Option<tg::Tag>,
}
