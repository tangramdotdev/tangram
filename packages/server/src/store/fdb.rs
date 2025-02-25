use bytes::Bytes;
use foundationdb as fdb;
use std::time::Instant;
use tangram_client as tg;

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
				let bytes = transaction
					.get(id.to_string().as_bytes(), false)
					.await?
					.map(|bytes| Bytes::copy_from_slice(&bytes));
				Ok(bytes)
			})
			.await
			.map_err(|source| tg::error!(!source, "the transaction failed"))?;
		let elapsed = start.elapsed();
		tracing::debug!(?elapsed, "get");
		Ok(bytes)
	}

	pub async fn put(&self, id: &tangram_client::object::Id, bytes: &Bytes) -> tg::Result<()> {
		let start = Instant::now();
		self.database
			.run(|transaction, _| async move {
				transaction.set(id.to_string().as_bytes(), bytes);
				Ok(())
			})
			.await
			.map_err(|source| tg::error!(!source, "the transaction failed"))?;
		let elapsed = start.elapsed();
		tracing::debug!(?elapsed, "put");
		Ok(())
	}
}
