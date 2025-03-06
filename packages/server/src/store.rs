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

pub struct PutArg {
	pub id: tg::object::Id,
	pub bytes: Bytes,
	pub touched_at: i64,
}

pub struct PutBatchArg {
	pub objects: Vec<(tg::object::Id, Bytes)>,
	pub touched_at: i64,
}

pub struct DeleteArg {
	pub id: tg::object::Id,
	pub now: i64,
	pub ttl: u64,
}

pub struct DeleteBatchArg {
	pub ids: Vec<tg::object::Id>,
	pub now: i64,
	pub ttl: u64,
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
			Self::Lmdb(lmdb) => lmdb.try_get(id).await,
			Self::S3(s3) => s3.try_get(id).await,
		}
	}

	pub async fn put(&self, arg: PutArg) -> tg::Result<()> {
		match self {
			Self::Memory(memory) => {
				memory.put(arg);
			},
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => {
				fdb.put(arg).await?;
			},
			Self::Lmdb(lmdb) => {
				lmdb.put(arg).await?;
			},
			Self::S3(s3) => {
				s3.put(arg).await?;
			},
		}
		Ok(())
	}

	pub async fn put_batch(&self, arg: PutBatchArg) -> tg::Result<()> {
		match self {
			Self::Memory(memory) => {
				memory.put_batch(arg);
			},
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => {
				fdb.put_batch(arg).await?;
			},
			Self::Lmdb(lmdb) => {
				lmdb.put_batch(arg).await?;
			},
			Self::S3(s3) => {
				s3.put_batch(arg).await?;
			},
		}
		Ok(())
	}

	pub async fn delete_batch(&self, arg: DeleteBatchArg) -> tg::Result<()> {
		match self {
			Self::Memory(memory) => {
				memory.delete_batch(arg);
			},
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => {
				fdb.delete_batch(arg).await?;
			},
			Self::Lmdb(lmdb) => {
				lmdb.delete_batch(arg).await?;
			},
			Self::S3(s3) => {
				s3.delete_batch(arg).await?;
			},
		}
		Ok(())
	}
}
