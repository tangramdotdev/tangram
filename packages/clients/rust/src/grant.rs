use crate::prelude::*;

pub mod create;
pub mod delete;
pub mod list;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Grant {
	pub created_at: i64,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub creator: Option<tg::Principal>,
	pub permissions: tg::authorization::permission::Set,
	pub resource: tg::Id,
	pub subject: tg::authorization::Subject,
}
