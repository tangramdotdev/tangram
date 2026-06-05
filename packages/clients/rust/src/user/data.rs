use crate::prelude::*;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub emails: Vec<String>,
	pub id: tg::user::Id,
	pub name: String,
	pub specifier: tg::Specifier,
}
