use {
	crate::{CacheReference, DeleteArg, PutArg},
	bytes::Bytes,
	dashmap::DashMap,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

pub struct Store(DashMap<tg::object::Id, Entry, tg::id::BuildHasher>);

struct Entry {
	bytes: Option<Bytes>,
	cache_reference: Option<CacheReference>,
	touched_at: i64,
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub enum Error {
	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl Store {
	#[must_use]
	pub fn new() -> Self {
		Self(DashMap::default())
	}

	#[must_use]
	pub fn try_get(&self, id: &tg::object::Id) -> Option<Bytes> {
		let entry = self.0.get(id)?;
		let bytes = entry.bytes.as_ref()?;
		Some(bytes.clone())
	}

	#[must_use]
	pub fn try_get_batch(&self, ids: &[tg::object::Id]) -> Vec<Option<Bytes>> {
		ids.iter().map(|id| self.try_get(id)).collect()
	}

	#[must_use]
	pub fn try_get_cache_reference(&self, id: &tg::object::Id) -> Option<CacheReference> {
		let entry = self.0.get(id)?;
		let cache_reference = entry.cache_reference.clone()?;
		Some(cache_reference)
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
}

impl Default for Store {
	fn default() -> Self {
		Self::new()
	}
}

impl crate::Store for Store {
	type Error = Error;

	async fn try_get(&self, id: &tg::object::Id) -> Result<Option<Bytes>, Self::Error> {
		Ok(self.try_get(id))
	}

	async fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
	) -> Result<Vec<Option<Bytes>>, Self::Error> {
		Ok(self.try_get_batch(ids))
	}

	async fn try_get_cache_reference(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<CacheReference>, Self::Error> {
		Ok(self.try_get_cache_reference(id))
	}

	async fn put(&self, arg: PutArg) -> Result<(), Self::Error> {
		let entry = Entry {
			bytes: arg.bytes,
			cache_reference: arg.cache_reference,
			touched_at: arg.touched_at,
		};
		self.0.insert(arg.id, entry);
		Ok(())
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> Result<(), Self::Error> {
		for arg in args {
			self.put(arg).await?;
		}
		Ok(())
	}

	async fn delete(&self, arg: DeleteArg) -> Result<(), Self::Error> {
		self.0.remove_if(&arg.id, |_, entry| {
			entry.touched_at >= arg.now - arg.ttl.to_i64().unwrap()
		});
		Ok(())
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> Result<(), Self::Error> {
		for arg in args {
			self.delete(arg).await?;
		}
		Ok(())
	}

	async fn flush(&self) -> Result<(), Self::Error> {
		Ok(())
	}
}

impl crate::Error for Error {
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}
