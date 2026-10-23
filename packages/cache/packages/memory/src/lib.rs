use {
	std::{
		collections::{BTreeMap, BTreeSet, HashMap},
		sync::{Arc, Mutex, MutexGuard},
	},
	tangram_cache::object as cache_object,
	tangram_client::prelude::*,
};

mod archive;
mod flush;
mod index;
mod log;
mod object;
#[cfg(test)]
mod tests;

#[derive(Clone, Debug, Default)]
pub struct Config {}

pub struct Cache {
	state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct Log {
	end: Option<tg::process::log::End>,
	entries: BTreeMap<u64, tangram_cache::log::read::Entry<'static>>,
	stream_positions: BTreeMap<(tg::process::stdio::Stream, u64), u64>,
}

#[derive(Default)]
struct State {
	archive_queue: BTreeMap<(tg::indexer::Id, u64), tangram_cache::archive::queue::Entry>,
	index_queue: BTreeMap<(tg::indexer::Id, u64), tangram_cache::index::queue::Fragment>,
	log_cache: BTreeSet<(u64, i64, tg::process::Id)>,
	logs: Logs,
	object_cache: BTreeMap<(u64, [u8; 16]), (tg::object::Id, [u8; 16])>,
	objects: Objects,
}

#[derive(Clone)]
struct Object {
	object: cache_object::Object<'static>,
}

type Logs = HashMap<tg::process::Id, Log, tg::id::BuildHasher>;
type Objects = HashMap<tg::object::Id, Object, tg::id::BuildHasher>;

impl Cache {
	#[must_use]
	pub fn new() -> Self {
		let state = Arc::new(Mutex::new(State::default()));
		Self { state }
	}

	fn state(&self) -> MutexGuard<'_, State> {
		self.state
			.lock()
			.expect("failed to lock the memory cache state")
	}
}

impl Default for Cache {
	fn default() -> Self {
		Self::new()
	}
}

impl tangram_cache::Cache for Cache {
	async fn flush(&self) -> tg::Result<()> {
		self.flush();
		Ok(())
	}

	async fn try_get_capacity(&self) -> tg::Result<Option<tangram_cache::capacity::Capacity>> {
		Ok(None)
	}
}
