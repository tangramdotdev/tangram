use {byteorder::ReadBytesExt as _, bytes::Bytes, std::path::PathBuf, tangram_client as tg};

#[cfg(feature = "foundationdb")]
mod fdb;
mod lmdb;
mod memory;
mod s3;
#[cfg(feature = "scylla")]
mod scylla;

#[cfg(feature = "foundationdb")]
pub use self::fdb::Fdb;
#[cfg(feature = "scylla")]
pub use self::scylla::Scylla;
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
	#[cfg(feature = "scylla")]
	Scylla(Scylla),
}

#[derive(Clone, Debug)]
pub struct PutArg {
	pub bytes: Option<Bytes>,
	pub cache_reference: Option<CacheReference>,
	pub id: tg::object::Id,
	pub touched_at: i64,
}

#[derive(Clone, Debug)]
pub struct DeleteArg {
	pub id: tg::object::Id,
	pub now: i64,
	pub ttl: u64,
}

#[derive(
	Clone,
	Debug,
	serde::Serialize,
	serde::Deserialize,
	tangram_serialize::Serialize,
	tangram_serialize::Deserialize,
)]
pub struct CacheReference {
	#[tangram_serialize(id = 0)]
	pub artifact: tg::artifact::Id,

	#[tangram_serialize(id = 1)]
	pub length: u64,

	#[tangram_serialize(id = 2)]
	pub position: u64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
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

	#[cfg(feature = "scylla")]
	pub async fn new_scylla(config: &crate::config::ScyllaStore) -> tg::Result<Self> {
		let scylla = Scylla::new(config).await?;
		Ok(Self::Scylla(scylla))
	}

	pub async fn try_get(&self, id: &tg::object::Id) -> tg::Result<Option<Bytes>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.try_get(id).await,
			Self::Lmdb(lmdb) => lmdb.try_get(id).await,
			Self::Memory(memory) => Ok(memory.try_get(id)),
			Self::S3(s3) => s3.try_get(id).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_get(id).await,
		}
	}

	pub async fn try_get_batch(&self, ids: &[tg::object::Id]) -> tg::Result<Vec<Option<Bytes>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.try_get_batch(ids).await,
			Self::Lmdb(lmdb) => lmdb.try_get_batch(ids).await,
			Self::Memory(memory) => Ok(memory.try_get_batch(ids)),
			Self::S3(s3) => s3.try_get_batch(ids).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_get_batch(ids).await,
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
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_get_cache_reference(id).await,
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
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => {
				scylla.put(arg).await?;
			},
		}
		Ok(())
	}

	pub async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => {
				fdb.put_batch(args).await?;
			},
			Self::Lmdb(lmdb) => {
				lmdb.put_batch(args).await?;
			},
			Self::Memory(memory) => {
				memory.put_batch(args);
			},
			Self::S3(s3) => {
				s3.put_batch(args).await?;
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => {
				scylla.put_batch(args).await?;
			},
		}
		Ok(())
	}

	#[allow(dead_code)]
	pub async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => {
				fdb.delete(arg).await?;
			},
			Self::Lmdb(lmdb) => {
				lmdb.delete(arg).await?;
			},
			Self::Memory(memory) => {
				memory.delete(arg);
			},
			Self::S3(s3) => {
				s3.delete(arg).await?;
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => {
				scylla.delete(arg).await?;
			},
		}
		Ok(())
	}

	pub async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => {
				fdb.delete_batch(args).await?;
			},
			Self::Lmdb(lmdb) => {
				lmdb.delete_batch(args).await?;
			},
			Self::Memory(memory) => {
				memory.delete_batch(args);
			},
			Self::S3(s3) => {
				s3.delete_batch(args).await?;
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => {
				scylla.delete_batch(args).await?;
			},
		}
		Ok(())
	}
}

impl CacheReference {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|source| tg::error!(!source, "failed to serialize"))?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let mut reader = std::io::Cursor::new(bytes.as_ref());
		let format = reader
			.read_u8()
			.map_err(|source| tg::error!(!source, "failed to read the format"))?;
		match format {
			0 => tangram_serialize::from_reader(&mut reader)
				.map_err(|source| tg::error!(!source, "failed to deserialize")),
			b'{' => serde_json::from_slice(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize")),
			_ => Err(tg::error!("invalid format")),
		}
	}
}
