use crate::prelude::*;

pub use {permission::Permission, principal::Principal, resource::Resource};

pub mod create;
pub mod delete;
pub mod permission;
pub mod principal;
pub mod resource;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Grant {
	pub created_at: i64,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub created_by: Option<tg::user::Id>,
	pub permission: tg::grant::Permission,
	pub principal: tg::grant::Principal,
	pub resource: tg::Id,
}
