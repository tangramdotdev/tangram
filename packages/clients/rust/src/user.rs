use crate::prelude::*;

pub mod current;
pub mod data;
pub mod get;
pub mod id;
pub mod login;
pub mod selector;

pub use self::{data::Data, id::Id, selector::Selector};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct User {
	pub emails: Vec<String>,

	pub id: Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::Location>,

	pub name: String,

	pub specifier: tg::Specifier,
}
