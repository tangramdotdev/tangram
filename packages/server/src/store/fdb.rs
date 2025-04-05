use super::CacheReference;
use bytes::Bytes;
use foundationdb::{self as fdb, FdbBindingError};
use foundationdb_tuple::TuplePack as _;
use futures::{TryStreamExt as _, future};
use num::ToPrimitive;
use std::pin::pin;
use tangram_client as tg;

/// The maximum size of a value.
const VALUE_SIZE_LIMIT: usize = 10_240;

pub struct Fdb {
	database: fdb::Database,
}

impl Fdb {
	pub fn new(config: &crate::config::FdbStore) -> tg::Result<Self> {
		let path = config
			.path
			.as_ref()
			.map(|path| path.as_os_str().to_str().unwrap());
		let database = fdb::Database::new(path)
			.map_err(|source| tg::error!(!source, "failed to open the database"))?;
		Ok(Self { database })
	}

	pub async fn try_get(&self, id: &tangram_client::object::Id) -> tg::Result<Option<Bytes>> {
		let bytes = self
			.database
			.run(|transaction, _| async move {
				let subspace = fdb::tuple::Subspace::all().subspace(&(0, id.to_bytes(), 0));
				let mut range = fdb::RangeOption::from(subspace.range());
				range.mode = fdb::options::StreamingMode::WantAll;
				let stream = transaction.get_ranges(range, false);
				let mut stream = pin!(stream);
				let mut empty = true;
				let mut bytes = Vec::new();
				while let Some(entries) = stream.try_next().await? {
					empty = false;
					for entry in entries {
						bytes.extend_from_slice(entry.value());
					}
				}
				if empty {
					return Ok(None);
				}
				Ok(Some(bytes.into()))
			})
			.await
			.map_err(|source| tg::error!(!source, "the transaction failed"))?;
		Ok(bytes)
	}

	pub async fn try_get_cache_reference(
		&self,
		id: &tangram_client::object::Id,
	) -> tg::Result<Option<CacheReference>> {
		let reference = self
			.database
			.run(|transaction, _| async move {
				let key = (0, id.to_bytes(), 2);
				let Some(bytes) = transaction.get(&key.pack_to_vec(), false).await? else {
					return Ok(None);
				};
				let reference = serde_json::from_slice(&bytes).map_err(|_| {
					FdbBindingError::new_custom_error("failed to deserialize the reference".into())
				})?;
				Ok(Some(reference))
			})
			.await
			.map_err(|source| tg::error!(!source, "the transaction failed"))?;
		Ok(reference)
	}

	pub async fn put(&self, arg: super::PutArg) -> tg::Result<()> {
		let arg = &arg;
		self.database
			.run(|transaction, _| async move {
				let subspace = fdb::tuple::Subspace::all().subspace(&(0, arg.id.to_bytes(), 0));
				if let Some(bytes) = &arg.bytes {
					if bytes.is_empty() {
						transaction.set(&subspace.pack(&0), &[]);
					} else {
						let mut start = 0;
						for chunk in bytes.chunks(VALUE_SIZE_LIMIT) {
							let key = subspace.pack(&start);
							transaction.set(&key, chunk);
							start += chunk.len();
						}
					}
				}
				let key = (0, arg.id.to_bytes(), 1);
				let value = arg.touched_at.to_le_bytes();
				transaction.set(&key.pack_to_vec(), &value);
				if let Some(reference) = &arg.cache_reference {
					let key = (0, arg.id.to_bytes(), 2);
					let value = serde_json::to_vec(reference).unwrap();
					transaction.set(&key.pack_to_vec(), &value);
				}
				Ok(())
			})
			.await
			.map_err(|source| tg::error!(!source, "the transaction failed"))?;
		Ok(())
	}

	pub async fn touch(&self, id: &tg::object::Id, touched_at: i64) -> tg::Result<()> {
		self.database
			.run(|transaction, _| async move {
				let key = (0, id.to_bytes(), 1);
				let value = touched_at.to_le_bytes();
				transaction.set(&key.pack_to_vec(), &value);
				Ok(())
			})
			.await
			.map_err(|source| tg::error!(!source, "the transaction failed"))?;
		Ok(())
	}

	pub async fn put_batch(&self, arg: super::PutBatchArg) -> tg::Result<()> {
		let arg = &arg;
		self.database
			.run(|transaction, _| async move {
				for (id, bytes, reference) in &arg.objects {
					let subspace = fdb::tuple::Subspace::all().subspace(&(0, id.to_bytes(), 0));
					if let Some(bytes) = bytes {
						if bytes.is_empty() {
							transaction.set(&subspace.pack(&0), &[]);
						} else {
							let mut start = 0;
							for chunk in bytes.chunks(VALUE_SIZE_LIMIT) {
								transaction.set(&subspace.pack(&start), chunk);
								start += chunk.len();
							}
						}
					}
					let key = (0, id.to_bytes(), 1);
					let value = arg.touched_at.to_le_bytes();
					transaction.set(&key.pack_to_vec(), &value);
					if let Some(reference) = reference {
						let key = (0, id.to_bytes(), 2);
						let value = serde_json::to_vec(reference).unwrap();
						transaction.set(&key.pack_to_vec(), &value);
					}
				}
				Ok(())
			})
			.await
			.map_err(|source| tg::error!(!source, "the transaction failed"))?;
		Ok(())
	}

	pub async fn delete_batch(&self, arg: super::DeleteBatchArg) -> tg::Result<()> {
		let arg = &arg;
		self.database
			.run(|transaction, _| async move {
				future::try_join_all(arg.ids.iter().map(|id| async {
					let Some(touched_at) = transaction
						.get(&(0, id.to_bytes(), 1).pack_to_vec(), false)
						.await?
					else {
						return Ok::<_, fdb::FdbBindingError>(());
					};
					let touched_at = touched_at.as_ref().try_into().map_err(|_| {
						fdb::FdbBindingError::new_custom_error("invalid touch time".into())
					})?;
					let touched_at = i64::from_le_bytes(touched_at);
					if arg.now - touched_at >= arg.ttl.to_i64().unwrap() {
						let subspace = fdb::tuple::Subspace::all().subspace(&(0, id.to_bytes()));
						transaction.clear_subspace_range(&subspace);
					}
					Ok(())
				}))
				.await?;
				Ok(())
			})
			.await
			.map_err(|source| tg::error!(!source, "the transaction failed"))?;
		Ok(())
	}
}
