use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	User(tg::user::Id),
}
