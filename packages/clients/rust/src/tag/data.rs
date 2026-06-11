use crate::prelude::*;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub id: tg::tag::Id,
	pub item: Item,
	pub name: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::Id>,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub permissions: Vec<tg::grant::Permission>,
	pub specifier: tg::Specifier,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum Item {
	Object(tg::object::Id),
	Process(tg::process::Id),
}

impl From<tg::Either<tg::object::Id, tg::process::Id>> for Item {
	fn from(value: tg::Either<tg::object::Id, tg::process::Id>) -> Self {
		match value {
			tg::Either::Left(id) => id.into(),
			tg::Either::Right(id) => id.into(),
		}
	}
}

impl From<tg::object::Id> for Item {
	fn from(value: tg::object::Id) -> Self {
		Self::Object(value)
	}
}

impl From<tg::process::Id> for Item {
	fn from(value: tg::process::Id) -> Self {
		Self::Process(value)
	}
}
