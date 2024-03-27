use super::{Module, Range};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Location {
	pub module: Module,
	pub range: Range,
}
