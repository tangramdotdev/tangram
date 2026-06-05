use crate::prelude::*;

pub mod batch;
pub mod data;
pub mod delete;
pub mod get;
pub mod grants;
pub mod id;
pub mod put;
pub mod selector;

pub use self::{data::Data, id::Id, selector::Selector};

#[derive(Clone, Debug)]
pub struct Tag {
	pub id: tg::tag::Id,
	pub item: tg::tag::Item,
	pub name: String,
	pub parent: Option<tg::Id>,
	pub specifier: tg::Specifier,
}

#[derive(Clone, Debug)]
pub enum Item {
	Object(tg::Object),
	Process(tg::Process),
}

impl From<tg::Either<tg::object::Id, tg::process::Id>> for tg::tag::Item {
	fn from(value: tg::Either<tg::object::Id, tg::process::Id>) -> Self {
		match value {
			tg::Either::Left(id) => id.into(),
			tg::Either::Right(id) => id.into(),
		}
	}
}

impl From<tg::object::Id> for tg::tag::Item {
	fn from(value: tg::object::Id) -> Self {
		Self::Object(tg::Object::with_id(value))
	}
}

impl From<tg::process::Id> for tg::tag::Item {
	fn from(value: tg::process::Id) -> Self {
		Self::Process(tg::Process::new(value, None, None, None, None, None))
	}
}

impl std::fmt::Display for Tag {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.specifier)
	}
}

impl std::fmt::Display for tg::tag::Data {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.specifier)
	}
}
