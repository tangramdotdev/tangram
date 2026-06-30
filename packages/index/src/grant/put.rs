use tangram_client::prelude::*;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub created_at: i64,
	pub creator: Option<tg::Principal>,
	pub expires_at: Option<i64>,
	pub permissions: tg::grant::permission::Set,
	pub principal: tg::grant::Principal,
	pub resource: tg::Id,
}
