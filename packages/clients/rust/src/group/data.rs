use crate::prelude::*;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub id: tg::group::Id,
	pub name: String,
	pub parent: Option<tg::Id>,
	pub specifier: tg::Specifier,
}
