use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub group: tg::group::Id,
	pub member: tg::group::Member,
}
