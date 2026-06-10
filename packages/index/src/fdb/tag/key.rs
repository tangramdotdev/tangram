use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Tag(tg::tag::Id),
	ItemTag {
		item: Vec<u8>,
		tag: tg::tag::Id,
	},
	ParentTag {
		parent: Option<tg::Id>,
		name: String,
		tag: tg::tag::Id,
	},
	TagParent {
		tag: tg::tag::Id,
		parent: Option<tg::Id>,
		name: String,
	},
}
