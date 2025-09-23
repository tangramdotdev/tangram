use {
	super::{CacheReference, DeleteArg, PutArg},
	bytes::Bytes,
	dashmap::DashMap,
	num::ToPrimitive as _,
	tangram_client as tg,
};

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

	pub fn try_get_batch(&self, ids: &[tg::object::Id]) -> Vec<Option<Bytes>> {
		ids.iter().map(|id| self.try_get(id)).collect()
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

	pub fn put_batch(&self, args: Vec<PutArg>) {
		for arg in args {
			self.put(arg);
		}
	}

	#[allow(clippy::needless_pass_by_value)]
	pub fn delete(&self, arg: DeleteArg) {
		self.0.remove_if(&arg.id, |_, entry| {
			entry.touched_at >= arg.now - arg.ttl.to_i64().unwrap()
		});
	}

	pub fn delete_batch(&self, args: Vec<DeleteArg>) {
		for arg in args {
			self.delete(arg);
		}
	}
}
