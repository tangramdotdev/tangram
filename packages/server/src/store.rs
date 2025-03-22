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

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Store {
	#[cfg(feature = "foundationdb")]
	Fdb(Fdb),
	Lmdb(Lmdb),
	Memory(Memory),
	S3(S3),
}

pub struct PutArg {
	pub bytes: Option<Bytes>,
	pub cache_reference: Option<CacheReference>,
	pub id: tg::object::Id,
	pub touched_at: i64,
}

pub struct PutBatchArg {
	pub objects: Vec<(tg::object::Id, Option<Bytes>, Option<CacheReference>)>,
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CacheReference {
	pub artifact: tg::artifact::Id,
	pub position: u64,
	pub length: u64,
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
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.try_get(id).await,
			Self::Lmdb(lmdb) => lmdb.try_get(id).await,
			Self::Memory(memory) => Ok(memory.try_get(id)),
			Self::S3(s3) => s3.try_get(id).await,
		}
	}

	pub async fn try_get_cache_reference(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<CacheReference>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.try_get_cache_reference(id).await,
			Self::Lmdb(lmdb) => lmdb.try_get_cache_reference(id).await,
			Self::Memory(memory) => Ok(memory.try_get_cache_reference(id)),
			Self::S3(s3) => s3.try_get_cache_reference(id).await,
		}
	}

	pub async fn put(&self, arg: PutArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => {
				fdb.put(arg).await?;
			},
			Self::Lmdb(lmdb) => {
				lmdb.put(arg).await?;
			},
			Self::Memory(memory) => {
				memory.put(arg);
			},
			Self::S3(s3) => {
				s3.put(arg).await?;
			},
		}
		Ok(())
	}

	pub async fn put_batch(&self, arg: PutBatchArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => {
				fdb.put_batch(arg).await?;
			},
			Self::Lmdb(lmdb) => {
				lmdb.put_batch(arg).await?;
			},
			Self::Memory(memory) => {
				memory.put_batch(arg);
			},
			Self::S3(s3) => {
				s3.put_batch(arg).await?;
			},
		}
		Ok(())
	}

	pub async fn delete_batch(&self, arg: DeleteBatchArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => {
				fdb.delete_batch(arg).await?;
			},
			Self::Lmdb(lmdb) => {
				lmdb.delete_batch(arg).await?;
			},
			Self::Memory(memory) => {
				memory.delete_batch(arg);
			},
			Self::S3(s3) => {
				s3.delete_batch(arg).await?;
			},
		}
		Ok(())
	}

	pub async fn touch(&self, id: &tg::object::Id, touched_at: i64) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => {
				fdb.touch(id, touched_at).await?;
			},
			Self::Lmdb(lmdb) => {
				lmdb.touch(id, touched_at).await?;
			},
			Self::Memory(memory) => {
				memory.touch(id, touched_at);
			},
			Self::S3(s3) => {
				s3.touch(id, touched_at).await?;
			},
		}
		Ok(())
	}
}
