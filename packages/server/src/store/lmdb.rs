use bytes::Bytes;
use heed as lmdb;
use tangram_client as tg;

pub struct Lmdb {
	db: lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>,
	env: lmdb::Env,
}

impl Lmdb {
	pub fn new(config: &crate::config::LmdbStore) -> tg::Result<Self> {
		if !config.path.exists() {
			std::fs::File::create(&config.path)
				.map_err(|source| tg::error!(!source, "failed to create the database file"))?;
		}
		let env = unsafe {
			heed::EnvOpenOptions::new()
				.map_size(1_099_511_627_776)
				.max_dbs(3)
				.flags(heed::EnvFlags::NO_SUB_DIR)
				.open(&config.path)
				.map_err(|source| tg::error!(!source, "failed to open the database"))?
		};
		let mut transaction = env.write_txn().unwrap();
		let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = env
			.create_database(&mut transaction, None)
			.map_err(|source| tg::error!(!source, "failed to open the database"))?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(Self { db, env })
	}

	pub fn try_get(
		&self,
		id: &tangram_client::object::Id,
	) -> Result<Option<Bytes>, tangram_client::Error> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let Some(bytes) = self
			.db
			.get(&transaction, id.to_string().as_bytes())
			.map_err(|source| tg::error!(!source, "failed to get the value"))?
		else {
			return Ok(None);
		};
		let bytes = Bytes::copy_from_slice(bytes);
		drop(transaction);
		Ok(Some(bytes))
	}

	pub fn put(&self, id: &tangram_client::object::Id, bytes: Bytes) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		self.db
			.put(&mut transaction, id.to_string().as_bytes(), &bytes)
			.map_err(|source| tg::error!(!source, "failed to put the value"))?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn put_batch(&self, items: &[(tg::object::Id, Bytes)]) -> tg::Result<()> {
		todo!()
	}
}
