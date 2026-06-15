use crate::prelude::*;

pub mod create;
pub mod data;
pub mod delete;
pub mod get;
pub mod id;
pub mod member;
pub mod members;
pub mod selector;

pub use self::{data::Data, id::Id, member::Member, selector::Selector};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Group {
	pub id: Id,

	pub name: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::Id>,

	pub specifier: tg::Specifier,
}
