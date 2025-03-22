use bytes::Bytes;
use dashmap::DashMap;
use tangram_client as tg;

use super::Reference;

pub struct Memory(DashMap<tg::object::Id, Bytes, fnv::FnvBuildHasher>);

impl Memory {
	pub fn new() -> Self {
		Self(DashMap::default())
	}

	pub fn try_get(&self, id: &tg::object::Id) -> Option<Bytes> {
		self.0.get(id).map(|value| value.clone())
	}

	pub fn try_get_cache_reference(&self, _: &tg::object::Id) -> Option<Reference> {
		todo!()
	}

	pub fn put(&self, arg: super::PutArg) {
		self.0.insert(arg.id, arg.bytes);
	}

	pub fn put_batch(&self, arg: super::PutBatchArg) {
		for (id, bytes, reference) in arg.objects {
			self.put(super::PutArg {
				id,
				bytes,
				touched_at: arg.touched_at,
				reference,
			});
		}
	}

	pub fn delete_batch(&self, arg: super::DeleteBatchArg) {
		for id in arg.ids {
			self.0.remove(&id);
		}
	}

	pub fn try_get_object_data(&self, id: &tg::object::Id) -> tg::Result<Option<tg::object::Data>> {
		let Some(bytes) = self.0.get(id) else {
			return Ok(None);
		};
		let data = tg::object::Data::deserialize(id.kind(), bytes.value().clone())?;
		Ok(Some(data))
	}
}
