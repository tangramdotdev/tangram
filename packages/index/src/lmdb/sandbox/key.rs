use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Sandbox(tg::sandbox::Id),
}
