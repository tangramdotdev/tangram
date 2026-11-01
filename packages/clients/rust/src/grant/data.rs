use crate::prelude::*;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub created_at: i64,

	#[serde(default, skip_serializing_if = "tg::Principal::is_root")]
	pub creator: tg::Principal,

	pub id: tg::grant::Id,

	pub permissions: tg::authorization::permission::Set,

	pub resource: tg::Id,

	pub subject: tg::authorization::Subject,
}
