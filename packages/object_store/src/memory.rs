use {
	crate::{DeleteObjectArg, Object, PutObjectArg},
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
	pub fn try_get_object(&self, id: &tg::object::Id) -> Option<Object<'static>> {
		self.objects.get(id).map(|entry| entry.clone())
	}

	#[must_use]
	pub fn try_get_object_batch(&self, ids: &[tg::object::Id]) -> Vec<Option<Object<'static>>> {
		ids.iter().map(|id| self.try_get_object(id)).collect()
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

	pub fn put_object(&self, arg: PutObjectArg) {
		let object = Object {
			bytes: arg.bytes.map(|bytes| Cow::Owned(bytes.to_vec())),
			cache_pointer: arg.cache_pointer,
			touched_at: arg.touched_at,
		};
		self.objects.insert(arg.id, object);
	}

	pub fn put_object_batch(&self, args: Vec<PutObjectArg>) {
		for arg in args {
			self.put_object(arg);
		}
	}

	#[expect(clippy::needless_pass_by_value)]
	pub fn delete_object(&self, arg: DeleteObjectArg) {
		self.objects.remove_if(&arg.id, |_, entry| {
			entry.touched_at >= arg.now - arg.ttl.to_i64().unwrap()
		});
	}

	pub fn delete_object_batch(&self, args: Vec<DeleteObjectArg>) {
		for arg in args {
			self.delete_object(arg);
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
	async fn try_get_object(&self, id: &tg::object::Id) -> tg::Result<Option<Object<'static>>> {
		Ok(self.try_get_object(id))
	}

	async fn try_get_object_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<Object<'static>>>> {
		Ok(self.try_get_object_batch(ids))
	}

	async fn put_object(&self, arg: PutObjectArg) -> tg::Result<()> {
		self.put_object(arg);
		Ok(())
	}

	async fn put_object_batch(&self, args: Vec<PutObjectArg>) -> tg::Result<()> {
		self.put_object_batch(args);
		Ok(())
	}

	async fn delete_object(&self, arg: DeleteObjectArg) -> tg::Result<()> {
		self.delete_object(arg);
		Ok(())
	}

	async fn delete_object_batch(&self, args: Vec<DeleteObjectArg>) -> tg::Result<()> {
		self.delete_object_batch(args);
		Ok(())
	}

	async fn flush(&self) -> tg::Result<()> {
		self.flush();
		Ok(())
	}
}
