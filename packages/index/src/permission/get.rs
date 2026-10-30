use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub resource: tg::Id,
	pub subject: tg::authorization::Subject,
}
