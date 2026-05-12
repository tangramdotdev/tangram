use crate::prelude::*;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Grant {
	pub namespace: tg::Namespace,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub user: Option<tg::user::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub group: Option<tg::group::Id>,

	pub permission: tg::Permission,
	pub created_at: i64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub created_by: Option<tg::user::Id>,
}
