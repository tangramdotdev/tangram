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
	pub target: tg::tag::Target,
	pub location: Option<tg::Location>,
	pub name: String,
	pub parent: Option<tg::Id>,
	pub permissions: Vec<tg::grant::Permission>,
	pub specifier: tg::Specifier,
	pub tokens: tg::authorization::Tokens,
}

#[derive(Clone, Debug)]
pub enum Target {
	Object(tg::Object),
	Process(tg::Process),
}

impl From<tg::Either<tg::object::Id, tg::process::Id>> for tg::tag::Target {
	fn from(value: tg::Either<tg::object::Id, tg::process::Id>) -> Self {
		match value {
			tg::Either::Left(id) => id.into(),
			tg::Either::Right(id) => id.into(),
		}
	}
}

impl From<tg::object::Id> for tg::tag::Target {
	fn from(value: tg::object::Id) -> Self {
		Self::Object(tg::Object::with_id(value))
	}
}

impl From<tg::process::Id> for tg::tag::Target {
	fn from(value: tg::process::Id) -> Self {
		Self::Process(tg::Process::new(value, tg::process::Options::default()))
	}
}

impl From<tg::tag::get::Output> for Tag {
	fn from(value: tg::tag::get::Output) -> Self {
		let tg::tag::get::Output {
			data,
			location,
			tokens,
		} = value;
		let tg::tag::Data {
			id,
			target,
			name,
			parent,
			permissions,
			specifier,
		} = data;
		let target = match target {
			tg::tag::data::Target::Object(id) => tg::tag::Target::Object(tg::Object::with_id(id)),
			tg::tag::data::Target::Process(id) => tg::tag::Target::Process(tg::Process::new(
				id,
				tg::process::Options {
					location: location.clone().map(Into::into),
					..tg::process::Options::default()
				},
			)),
		};

		Self {
			id,
			target,
			location,
			name,
			parent,
			permissions,
			specifier,
			tokens,
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
