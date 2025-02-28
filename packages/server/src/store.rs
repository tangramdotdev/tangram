use bytes::Bytes;
use tangram_client as tg;

#[cfg(feature = "foundationdb")]
mod fdb;
mod lmdb;
mod memory;
mod s3;

#[cfg(feature = "foundationdb")]
pub use self::fdb::Fdb;
pub use self::{lmdb::Lmdb, memory::Memory, s3::S3};

pub enum Store {
	#[cfg(feature = "foundationdb")]
	Fdb(Fdb),
	Lmdb(Lmdb),
	Memory(Memory),
	S3(S3),
}

impl Store {
	#[cfg(feature = "foundationdb")]
	pub fn new_fdb(config: &crate::config::FdbStore) -> tg::Result<Self> {
		let fdb = Fdb::new(config)?;
		Ok(Self::Fdb(fdb))
	}

	pub fn new_lmdb(config: &crate::config::LmdbStore) -> tg::Result<Self> {
		let lmdb = Lmdb::new(config)?;
		Ok(Self::Lmdb(lmdb))
	}

	pub fn new_memory() -> Self {
		Self::Memory(Memory::new())
	}

	pub fn new_s3(config: &crate::config::S3Store) -> Self {
		Self::S3(S3::new(config))
	}

	pub async fn try_get(&self, id: &tg::object::Id) -> tg::Result<Option<Bytes>> {
		match self {
			Self::Memory(memory) => Ok(memory.try_get(id)),
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.try_get(id).await,
			Self::Lmdb(lmdb) => lmdb.try_get(id),
			Self::S3(s3) => s3.try_get(id).await,
		}
	}

	pub async fn put(&self, id: &tg::object::Id, bytes: Bytes) -> tg::Result<()> {
		match self {
			Self::Memory(memory) => {
				memory.put(id, bytes);
			},
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => {
				fdb.put(id, bytes).await?;
			},
			Self::Lmdb(lmdb) => {
				lmdb.put(id, bytes)?;
			},
			Self::S3(s3) => {
				s3.put(id, bytes).await?;
			},
		}
		Ok(())
	}

	pub async fn put_batch(&self, items: &[(tg::object::Id, Bytes)]) -> tg::Result<()> {
		match self {
			Self::Memory(memory) => {
				memory.put_batch(items);
			},
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => {
				fdb.put_batch(items).await?;
			},
			Self::Lmdb(lmdb) => {
				lmdb.put_batch(items)?;
			},
			Self::S3(s3) => {
				s3.put_batch(items).await?;
			},
		}
		Ok(())
	}
}
