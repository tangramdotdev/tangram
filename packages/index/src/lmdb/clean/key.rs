use tangram_client::prelude::*;

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
pub enum ItemKind {
	CacheEntry = 0,
	Object = 1,
	Process = 2,
}

#[derive(Clone, Debug)]
pub enum Key {
	Clean {
		touched_at: i64,
		kind: ItemKind,
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
}
