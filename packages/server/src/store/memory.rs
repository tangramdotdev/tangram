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

	pub fn put(&self, arg: super::PutArg) {
		self.0.insert(arg.id, arg.bytes);
	}

	pub fn put_batch(&self, arg: super::PutBatchArg) {
		for (id, bytes) in arg.objects {
			self.put(super::PutArg {
				id,
				bytes,
				touched_at: arg.touched_at,
			});
		}
	}

	pub fn delete_batch(&self, arg: super::DeleteBatchArg) {
		for id in arg.ids {
			self.0.remove(&id);
		}
	}
}
