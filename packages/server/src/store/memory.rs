use bytes::Bytes;
use dashmap::DashMap;
use tangram_client as tg;

pub struct Memory(DashMap<tg::object::Id, Bytes, fnv::FnvBuildHasher>);

impl Memory {
	pub fn new() -> Self {
		Self(DashMap::default())
	}

	pub fn try_get(&self, id: &tg::object::Id) -> Option<Bytes> {
		self.0.get(id).map(|value| value.clone())
	}

	pub fn put(&self, id: &tg::object::Id, bytes: Bytes) {
		self.0.insert(id.clone(), bytes);
	}

	pub fn put_batch(&self, items: &[(tg::object::Id, Bytes)]) {
		for (id, bytes) in items {
			self.put(id, bytes.clone());
		}
	}
}
