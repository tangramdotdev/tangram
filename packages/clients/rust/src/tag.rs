use crate::prelude::*;

pub mod batch;
pub mod data;
pub mod delete;
pub mod get;
pub mod id;
pub mod put;
pub mod selector;

pub use self::{data::Data, id::Id, selector::Selector};

#[derive(Clone, Debug)]
pub struct Tag {
	pub id: tg::tag::Id,
	pub item: tg::tag::Item,
	pub location: Option<tg::Location>,
	pub name: String,
	pub parent: Option<tg::Id>,
	pub permissions: Vec<tg::grant::Permission>,
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
		Self::Process(tg::Process::new(value, tg::process::Options::default()))
	}
}

impl From<tg::tag::get::Output> for Tag {
	fn from(value: tg::tag::get::Output) -> Self {
		let tg::tag::get::Output { data, location } = value;
		let tg::tag::Data {
			id,
			item,
			name,
			parent,
			permissions,
			specifier,
		} = data;
		let item = match item {
			tg::tag::data::Item::Object(id) => tg::tag::Item::Object(tg::Object::with_id(id)),
			tg::tag::data::Item::Process(id) => tg::tag::Item::Process(tg::Process::new(
				id,
				tg::process::Options {
					location: location.clone().map(Into::into),
					..tg::process::Options::default()
				},
			)),
		};

		Self {
			id,
			item,
			location,
			name,
			parent,
			permissions,
			specifier,
		}
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
