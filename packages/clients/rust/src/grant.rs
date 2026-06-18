use crate::prelude::*;

pub use {
	permission::Permission,
	principal::Principal,
	resource::Resource,
	token::{Algorithm, Body, MaybeWithToken, Metadata, PrivateKey, PublicKey, Token, WithToken},
};

pub mod create;
pub mod delete;
pub mod list;
pub mod permission;
pub mod principal;
pub mod resource;
pub mod token;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Grant {
	pub created_at: i64,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub creator: Option<tg::Principal>,
	pub permissions: tg::grant::permission::Set,
	pub principal: tg::grant::Principal,
	pub resource: tg::Id,
}
