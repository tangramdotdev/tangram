use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Object(tg::object::Id),
	ObjectChild {
		object: tg::object::Id,
		child: tg::object::Id,
	},
	ChildObject {
		child: tg::object::Id,
		object: tg::object::Id,
	},
	ObjectCacheEntry {
		object: tg::object::Id,
		cache_entry: tg::artifact::Id,
	},
	CacheEntryObject {
		cache_entry: tg::artifact::Id,
		object: tg::object::Id,
	},
	ObjectProcess {
		object: tg::object::Id,
		kind: crate::process::object::Kind,
		process: tg::process::Id,
	},
}
