use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub member: tg::organization::Member,
	pub organization: tg::organization::Id,
}
