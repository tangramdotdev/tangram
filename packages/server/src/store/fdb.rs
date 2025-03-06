use bytes::Bytes;
use foundationdb as fdb;
use foundationdb_tuple::TuplePack as _;
use futures::future;
use num::ToPrimitive;
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
				let mut range_option = fdb::RangeOption::from(subspace.range());
				range_option.mode = fdb::options::StreamingMode::WantAll;
				let entries = transaction.get_range(&range_option, 0, false).await?;
				if entries.is_empty() {
					return Ok(None);
				}
				let mut bytes = Vec::new();
				for entry in entries {
					bytes.extend_from_slice(entry.value());
				}
				Ok(Some(bytes.into()))
			})
			.await
			.map_err(|source| tg::error!(!source, "the transaction failed"))?;
		Ok(bytes)
	}

	pub async fn put(&self, arg: super::PutArg) -> tg::Result<()> {
		let arg = &arg;
		self.database
			.run(|transaction, _| async move {
				let subspace = fdb::tuple::Subspace::all().subspace(&(0, arg.id.to_bytes(), 0));
				if arg.bytes.is_empty() {
					transaction.set(&subspace.pack(&0), &[]);
				} else {
					let mut start = 0;
					for chunk in arg.bytes.chunks(VALUE_SIZE_LIMIT) {
						transaction.set(&subspace.pack(&start), chunk);
						start += chunk.len();
					}
				}
				let touched_at = arg.touched_at.to_le_bytes();
				transaction.set(&(0, arg.id.to_bytes(), 1).pack_to_vec(), &touched_at);
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
				for (id, bytes) in &arg.objects {
					let subspace = fdb::tuple::Subspace::all().subspace(&(0, id.to_bytes(), 0));
					if bytes.is_empty() {
						transaction.set(&subspace.pack(&0), &[]);
					} else {
						let mut start = 0;
						for chunk in bytes.chunks(VALUE_SIZE_LIMIT) {
							transaction.set(&subspace.pack(&start), chunk);
							start += chunk.len();
						}
					}
					let touched_at = arg.touched_at.to_le_bytes();
					transaction.set(&(0, id.to_bytes(), 1).pack_to_vec(), &touched_at);
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
					if arg.now - touched_at > arg.ttl.to_i64().unwrap() {
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
