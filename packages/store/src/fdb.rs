use {
	crate::{CacheReference, DeleteArg, PutArg},
	bytes::Bytes,
	foundationdb::{self as fdb, FdbBindingError},
	foundationdb_tuple::TuplePack as _,
	futures::{TryStreamExt as _, future, stream::FuturesOrdered},
	num::ToPrimitive as _,
	std::pin::pin,
	tangram_client as tg,
};

/// The maximum size of a value.
const VALUE_SIZE_LIMIT: usize = 10_240;

#[derive(Clone, Debug)]
pub struct Config {
	pub path: Option<std::path::PathBuf>,
}

pub struct Store {
	database: fdb::Database,
}

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Fdb(fdb::FdbError),
	FdbBinding(fdb::FdbBindingError),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl Store {
	pub fn new(config: &Config) -> Result<Self, Error> {
		let path = config
			.path
			.as_ref()
			.map(|path| path.as_os_str().to_str().unwrap());
		let database = fdb::Database::new(path)?;
		Ok(Self { database })
	}
}

impl crate::Store for Store {
	type Error = Error;

	async fn try_get(&self, id: &tg::object::Id) -> Result<Option<Bytes>, Self::Error> {
		let bytes = self
			.database
			.run(|transaction, _| async move {
				let id = id.to_bytes();
				let subspace = (0, id.as_ref(), 0);
				let subspace = fdb::tuple::Subspace::all().subspace(&subspace);
				let mut range = fdb::RangeOption::from(subspace.range());
				range.mode = fdb::options::StreamingMode::WantAll;
				let stream = transaction.get_ranges(range, false);
				let mut stream = pin!(stream);
				let mut empty = true;
				let mut bytes = Vec::new();
				while let Some(entries) = stream.try_next().await? {
					for entry in entries {
						empty = false;
						bytes.extend_from_slice(entry.value());
					}
				}
				if empty {
					return Ok(None);
				}
				Ok(Some(bytes.into()))
			})
			.await?;
		Ok(bytes)
	}

	async fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
	) -> Result<Vec<Option<Bytes>>, Self::Error> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let batch = self
			.database
			.run(|transaction, _| async move {
				let result = ids
					.iter()
					.map(|id| {
						let transaction = transaction.clone();
						async move {
							let id = id.to_bytes();
							let subspace = (0, id.as_ref(), 0);
							let subspace = fdb::tuple::Subspace::all().subspace(&subspace);
							let mut range = fdb::RangeOption::from(subspace.range());
							range.mode = fdb::options::StreamingMode::WantAll;
							let stream = transaction.get_ranges(range, false);
							let mut stream = pin!(stream);
							let mut empty = true;
							let mut bytes = Vec::new();
							while let Some(entries) = stream.try_next().await? {
								for entry in entries {
									empty = false;
									bytes.extend_from_slice(entry.value());
								}
							}
							if empty {
								return Ok::<_, fdb::FdbBindingError>(None);
							}
							Ok(Some(bytes.into()))
						}
					})
					.collect::<FuturesOrdered<_>>()
					.try_collect::<Vec<_>>()
					.await?;
				Ok(result)
			})
			.await?;
		Ok(batch)
	}

	async fn try_get_cache_reference(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<CacheReference>, Self::Error> {
		let reference = self
			.database
			.run(|transaction, _| async move {
				let id = id.to_bytes();
				let key = (0, id.as_ref(), 2);
				let Some(bytes) = transaction.get(&key.pack_to_vec(), false).await? else {
					return Ok(None);
				};
				let reference = CacheReference::deserialize(&*bytes).map_err(|_| {
					FdbBindingError::new_custom_error("failed to deserialize the reference".into())
				})?;
				Ok(Some(reference))
			})
			.await?;
		Ok(reference)
	}

	async fn put(&self, arg: PutArg) -> Result<(), Self::Error> {
		let arg = &arg;
		self.database
			.run(|transaction, _| async move {
				let id = arg.id.to_bytes();
				let subspace = (0, id.as_ref(), 0);
				let subspace = fdb::tuple::Subspace::all().subspace(&subspace);
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
				let id = arg.id.to_bytes();
				let key = (0, id.as_ref(), 1);
				let value = arg.touched_at.to_le_bytes();
				transaction.set(&key.pack_to_vec(), &value);
				if let Some(reference) = &arg.cache_reference {
					let id = arg.id.to_bytes();
					let key = (0, id.as_ref(), 2);
					let value = reference.serialize().unwrap();
					transaction.set(&key.pack_to_vec(), &value);
				}
				Ok(())
			})
			.await?;
		Ok(())
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> Result<(), Self::Error> {
		if args.is_empty() {
			return Ok(());
		}
		let args = &args;
		self.database
			.run(|transaction, _| async move {
				for arg in args {
					let id = arg.id.to_bytes();
					let subspace = (0, id.as_ref(), 0);
					let subspace = fdb::tuple::Subspace::all().subspace(&subspace);
					if let Some(bytes) = &arg.bytes {
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
					let id = arg.id.to_bytes();
					let key = (0, id.as_ref(), 1);
					let value = arg.touched_at.to_le_bytes();
					transaction.set(&key.pack_to_vec(), &value);
					if let Some(cache_reference) = &arg.cache_reference {
						let id = arg.id.to_bytes();
						let key = (0, id.as_ref(), 2);
						let value = cache_reference.serialize().unwrap();
						transaction.set(&key.pack_to_vec(), &value);
					}
				}
				Ok(())
			})
			.await?;
		Ok(())
	}

	async fn delete(&self, arg: DeleteArg) -> Result<(), Self::Error> {
		let arg = &arg;
		self.database
			.run(|transaction, _| async move {
				let id = arg.id.to_bytes();
				let key = (0, id.as_ref(), 1);
				let Some(touched_at) = transaction.get(&key.pack_to_vec(), false).await? else {
					return Ok::<_, fdb::FdbBindingError>(());
				};
				let touched_at = touched_at.as_ref().try_into().map_err(|_| {
					fdb::FdbBindingError::new_custom_error("invalid touch time".into())
				})?;
				let touched_at = i64::from_le_bytes(touched_at);
				if arg.now - touched_at >= arg.ttl.to_i64().unwrap() {
					let id = arg.id.to_bytes();
					let subspace = (0, id.as_ref());
					let subspace = fdb::tuple::Subspace::all().subspace(&subspace);
					transaction.clear_subspace_range(&subspace);
				}
				Ok(())
			})
			.await?;
		Ok(())
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> Result<(), Self::Error> {
		if args.is_empty() {
			return Ok(());
		}
		let args = &args;
		self.database
			.run(|transaction, _| async move {
				future::try_join_all(args.iter().map(|arg| async {
					let id = arg.id.to_bytes();
					let Some(touched_at) = transaction
						.get(&(0, id.as_ref(), 1).pack_to_vec(), false)
						.await?
					else {
						return Ok::<_, fdb::FdbBindingError>(());
					};
					let touched_at = touched_at.as_ref().try_into().map_err(|_| {
						fdb::FdbBindingError::new_custom_error("invalid touch time".into())
					})?;
					let touched_at = i64::from_le_bytes(touched_at);
					if arg.now - touched_at >= arg.ttl.to_i64().unwrap() {
						let id = arg.id.to_bytes();
						let subspace = (0, id.as_ref());
						let subspace = fdb::tuple::Subspace::all().subspace(&subspace);
						transaction.clear_subspace_range(&subspace);
					}
					Ok(())
				}))
				.await?;
				Ok(())
			})
			.await?;
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
