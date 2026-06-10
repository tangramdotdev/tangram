use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	CacheEntry(tg::artifact::Id),
	CacheEntryDependency {
		cache_entry: tg::artifact::Id,
		dependency: tg::artifact::Id,
	},
	DependencyCacheEntry {
		dependency: tg::artifact::Id,
		cache_entry: tg::artifact::Id,
	},
}
