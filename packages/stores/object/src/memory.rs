use {
	crate::{DeleteArg, Object, PutArg},
	dashmap::DashMap,
	num::ToPrimitive as _,
	std::borrow::Cow,
	tangram_client::prelude::*,
};

pub struct Store {
	objects: DashMap<tg::object::Id, Object<'static>, tg::id::BuildHasher>,
}

impl Store {
	#[must_use]
	pub fn new() -> Self {
		Self {
			objects: DashMap::default(),
		}
	}

	#[must_use]
	pub fn try_get(&self, id: &tg::object::Id) -> Option<Object<'static>> {
		self.objects.get(id).map(|entry| entry.clone())
	}

	#[must_use]
	pub fn try_get_batch(&self, ids: &[tg::object::Id]) -> Vec<Option<Object<'static>>> {
		ids.iter().map(|id| self.try_get(id)).collect()
	}

	pub fn try_get_object_data(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let Some(entry) = self.objects.get(id) else {
			return Ok(None);
		};
		let Some(bytes) = &entry.bytes else {
			return Ok(None);
		};
		let size = bytes.len().to_u64().unwrap();
		let data = tg::object::Data::deserialize(id.kind(), bytes.as_ref())?;
		Ok(Some((size, data)))
	}

	pub fn put(&self, arg: PutArg) {
		let object = Object {
			bytes: arg.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			cache_pointer: arg.cache_pointer,
			touched_at: arg.touched_at,
		};
		self.objects.insert(arg.id, object);
	}

	pub fn put_batch(&self, args: Vec<PutArg>) {
		for arg in args {
			self.put(arg);
		}
	}

	#[expect(clippy::needless_pass_by_value)]
	pub fn delete(&self, arg: DeleteArg) {
		self.objects.remove_if(&arg.id, |_, entry| {
			entry.touched_at >= arg.now - arg.ttl.to_i64().unwrap()
		});
	}

	pub fn delete_batch(&self, args: Vec<DeleteArg>) {
		for arg in args {
			self.delete(arg);
		}
	}

	pub fn flush(&self) {}
}

impl Default for Store {
	fn default() -> Self {
		Self::new()
	}
}

impl crate::Store for Store {
	async fn try_get(&self, id: &tg::object::Id) -> tg::Result<Option<Object<'static>>> {
		Ok(self.try_get(id))
	}

	async fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<Object<'static>>>> {
		Ok(self.try_get_batch(ids))
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		self.put(arg);
		Ok(())
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		self.put_batch(args);
		Ok(())
	}

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		self.delete(arg);
		Ok(())
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		self.delete_batch(args);
		Ok(())
	}

	async fn flush(&self) -> tg::Result<()> {
		self.flush();
		Ok(())
	}
}
