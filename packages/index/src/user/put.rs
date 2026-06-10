use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::user::Id,
	pub specifier: tg::Specifier,
}
