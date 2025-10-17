use {byteorder::ReadBytesExt as _, bytes::Bytes, std::path::PathBuf, tangram_client as tg};

#[cfg(feature = "foundationdb")]
pub mod fdb;
#[cfg(feature = "lmdb")]
pub mod lmdb;
pub mod memory;
pub mod s3;
#[cfg(feature = "scylla")]
pub mod scylla;

pub mod prelude {
	pub use super::{Error as _, Store as _};
}

pub trait Error: std::error::Error + Send + Sync + 'static {
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self;
}

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub trait Store {
	type Error: Error;

	fn try_get(
		&self,
		id: &tg::object::Id,
	) -> impl std::future::Future<Output = std::result::Result<Option<Bytes>, Self::Error>> + Send;

	fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
	) -> impl std::future::Future<Output = std::result::Result<Vec<Option<Bytes>>, Self::Error>> + Send;

	fn try_get_cache_reference(
		&self,
		id: &tg::object::Id,
	) -> impl std::future::Future<Output = std::result::Result<Option<CacheReference>, Self::Error>> + Send;

	fn put(
		&self,
		arg: PutArg,
	) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;

	fn put_batch(
		&self,
		args: Vec<PutArg>,
	) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;

	fn delete(
		&self,
		arg: DeleteArg,
	) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;

	fn delete_batch(
		&self,
		args: Vec<DeleteArg>,
	) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;

	fn flush(
		&self,
	) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;
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

impl CacheReference {
	pub fn serialize(&self) -> Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> Result<Self> {
		let bytes = bytes.into();
		let mut reader = std::io::Cursor::new(bytes.as_ref());
		let format = reader
			.read_u8()
			.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
		match format {
			0 => tangram_serialize::from_reader(&mut reader)
				.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) }),
			b'{' => serde_json::from_slice(&bytes)
				.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) }),
			_ => Err("invalid format".into()),
		}
	}
}
