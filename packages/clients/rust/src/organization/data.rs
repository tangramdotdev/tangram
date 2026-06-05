use crate::prelude::*;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub id: tg::organization::Id,
	pub name: String,
	pub specifier: tg::Specifier,
}
