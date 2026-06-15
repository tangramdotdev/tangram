use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub expires_at: Option<i64>,
	pub permission: tg::grant::Permission,
	pub principal: tg::grant::Principal,
	pub resource: tg::Id,
}
