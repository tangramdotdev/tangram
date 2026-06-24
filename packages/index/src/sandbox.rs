use tangram_client::prelude::*;

pub mod put;

#[derive(Clone, Debug)]
pub struct Sandbox {
	pub owner: Option<tg::Principal>,
}
