use super::CacheReference;
use bytes::Bytes;
use dashmap::DashMap;
use num::ToPrimitive as _;
use tangram_client as tg;

pub struct Memory(DashMap<tg::object::Id, Entry, fnv::FnvBuildHasher>);

struct Entry {
	bytes: Option<Bytes>,
	cache_reference: Option<CacheReference>,
	touched_at: i64,
}

impl Memory {
	pub fn new() -> Self {
		Self(DashMap::default())
	}

	pub fn try_get(&self, id: &tg::object::Id) -> Option<Bytes> {
		let entry = self.0.get(id)?;
		let bytes = entry.bytes.as_ref()?;
		Some(bytes.clone())
	}

	pub fn try_get_object_data(&self, id: &tg::object::Id) -> tg::Result<Option<tg::object::Data>> {
		let Some(entry) = self.0.get(id) else {
			return Ok(None);
		};
		let Some(bytes) = &entry.bytes else {
			return Ok(None);
		};
		let data = tg::object::Data::deserialize(id.kind(), bytes.as_ref())?;
		Ok(Some(data))
	}

	pub fn try_get_cache_reference(&self, id: &tg::object::Id) -> Option<CacheReference> {
		let entry = self.0.get(id)?;
		let cache_reference = entry.cache_reference.clone()?;
		Some(cache_reference)
	}

	pub fn put(&self, arg: super::PutArg) {
		let entry = Entry {
			bytes: arg.bytes,
			cache_reference: arg.cache_reference,
			touched_at: arg.touched_at,
		};
		self.0.insert(arg.id, entry);
	}

	pub fn put_batch(&self, arg: super::PutBatchArg) {
		for (id, bytes, cache_reference) in arg.objects {
			self.put(super::PutArg {
				id,
				bytes,
				touched_at: arg.touched_at,
				cache_reference,
			});
		}
	}

	pub fn delete_batch(&self, arg: super::DeleteBatchArg) {
		for id in arg.ids {
			self.0.remove_if(&id, |_, entry| {
				entry.touched_at >= arg.now - arg.ttl.to_i64().unwrap()
			});
		}
	}

	pub fn touch(&self, id: &tg::object::Id, touched_at: i64) {
		if let Some(mut entry) = self.0.get_mut(id) {
			entry.touched_at = touched_at;
		}
	}
}
