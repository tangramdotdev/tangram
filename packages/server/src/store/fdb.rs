use bytes::Bytes;
use foundationdb as fdb;
use std::time::Instant;
use tangram_client as tg;

/// The maximum size of a transaction.
const TRANSACTION_SIZE_LIMIT: usize = 1_048_576;

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
		let start = Instant::now();
		let bytes = self
			.database
			.run(|transaction, _| async move {
				let subspace = fdb::tuple::Subspace::all().subspace(&id.to_string());
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
		let elapsed = start.elapsed();
		tracing::debug!(?elapsed, "get");
		Ok(bytes)
	}

	pub async fn put(&self, id: &tangram_client::object::Id, bytes: Bytes) -> tg::Result<()> {
		let start = Instant::now();
		self.database
			.run(|transaction, _| {
				let bytes = &bytes;
				async move {
					let subspace = fdb::tuple::Subspace::all().subspace(&id.to_string());
					if bytes.is_empty() {
						transaction.set(&subspace.pack(&0), &[]);
					} else {
						let mut start = 0;
						for chunk in bytes.chunks(VALUE_SIZE_LIMIT) {
							transaction.set(&subspace.pack(&start), chunk);
							start += chunk.len();
						}
					}
					Ok(())
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the transaction failed"))?;
		let elapsed = start.elapsed();
		tracing::debug!(?elapsed, "put");
		Ok(())
	}

	pub async fn put_batch(&self, items: &[(tg::object::Id, Bytes)]) -> tg::Result<()> {
		let start = Instant::now();
		self.database
			.run(|transaction, _| async move {
				for (id, bytes) in items {
					let subspace = fdb::tuple::Subspace::all().subspace(&id.to_string());
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
				Ok(())
			})
			.await
			.map_err(|source| tg::error!(!source, "the transaction failed"))?;
		let elapsed = start.elapsed();
		tracing::debug!(?elapsed, "put_batch");
		Ok(())
	}
}
