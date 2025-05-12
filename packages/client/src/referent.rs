use crate as tg;
use std::path::PathBuf;

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Referent<T> {
	pub item: T,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tag: Option<tg::Tag>,
}

impl<T> Referent<T> {
	pub fn with_item(item: T) -> Self {
		Self {
			item,
			path: None,
			tag: None,
		}
	}

	pub fn map<U>(self, f: impl FnOnce(T) -> U) -> tg::Referent<U> {
		tg::Referent {
			item: f(self.item),
			path: self.path,
			tag: self.tag,
		}
	}
}
