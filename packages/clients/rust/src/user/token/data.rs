use crate::prelude::*;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub created_at: i64,

	pub id: tg::token::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,
}
