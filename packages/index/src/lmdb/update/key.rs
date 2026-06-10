use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Update {
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
	UpdateVersion {
		version: u64,
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
}
