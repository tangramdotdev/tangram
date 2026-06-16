use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub creator: Option<tg::Principal>,
	pub expires_at: Option<i64>,
	pub permissions: tg::grant::Set,
	pub principal: tg::grant::Principal,
	pub resource: tg::Id,
}
