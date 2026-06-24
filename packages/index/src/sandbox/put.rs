use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::sandbox::Id,
	pub owner: Option<tg::Principal>,
}
