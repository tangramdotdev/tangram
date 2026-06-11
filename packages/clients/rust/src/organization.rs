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

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Organization {
	pub id: Id,
	pub name: String,
	pub specifier: tg::Specifier,
}
