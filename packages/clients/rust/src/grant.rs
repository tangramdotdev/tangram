use crate::prelude::*;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Grant {
	pub namespace: tg::Namespace,
	pub principal: tg::Principal,
	pub permission: tg::Permission,
	pub created_at: i64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub created_by: Option<tg::user::Id>,
}
