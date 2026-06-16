use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Update {
		id: tg::Either<tg::object::Id, tg::process::Id>,
		kind: Kind,
	},
	UpdateVersion {
		id: tg::Either<tg::object::Id, tg::process::Id>,
		kind: Kind,
		version: u64,
	},
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Kind {
	Item,
	Grants(tg::grant::Principal),
}
