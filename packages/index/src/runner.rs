use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Sandbox {
	pub attempt: String,
	pub id: tg::sandbox::Id,
}
