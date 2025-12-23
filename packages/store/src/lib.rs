use {byteorder::ReadBytesExt as _, bytes::Bytes, std::path::PathBuf, tangram_client::prelude::*};

#[cfg(feature = "lmdb")]
pub mod lmdb;
pub mod memory;
#[cfg(feature = "scylla")]
pub mod scylla;

pub mod prelude {
	pub use super::{Error as _, Store as _};
}

pub trait Error: std::error::Error + Send + Sync + 'static {
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self;
}

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub mod log {
	use {bytes::Bytes, tangram_client as tg};
	#[derive(Debug, Clone, tangram_serialize::Serialize, tangram_serialize::Deserialize)]
	pub struct Chunk {
		#[tangram_serialize(id = 0)]
		pub bytes: Bytes,
		#[tangram_serialize(id = 1)]
		pub combined_position: u64,
		#[tangram_serialize(id = 2)]
		pub stream_position: u64,
		#[tangram_serialize(id = 3)]
		pub stream: tg::process::log::Stream,
		#[tangram_serialize(id = 4)]
		pub timestamp: i64,
	}
}

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

	fn try_read_log(
		&self,
		arg: ReadLogArg,
	) -> impl std::future::Future<Output = std::result::Result<Option<Bytes>, Self::Error>> + Send;

	fn try_get_log_length(
		&self,
		id: &tg::process::Id,
		_stream: Option<tg::process::log::Stream>,
	) -> impl std::future::Future<Output = std::result::Result<Option<u64>, Self::Error>> + Send;

	fn try_get_log_entry(
		&self,
		id: &tg::process::Id,
		index: u64,
	) -> impl std::future::Future<Output = std::result::Result<Option<log::Chunk>, Self::Error>> + Send;

	fn try_get_num_log_entries(
		&self,
		id: &tg::process::Id,
	) -> impl std::future::Future<Output = std::result::Result<Option<u64>, Self::Error>> + Send;

	fn try_get_cache_reference(
		&self,
		id: &tg::object::Id,
	) -> impl std::future::Future<Output = std::result::Result<Option<CacheReference>, Self::Error>> + Send;

	fn put(
		&self,
		arg: PutArg,
	) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;

	fn put_log(
		&self,
		arg: PutLogArg,
	) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;

	fn put_batch(
		&self,
		args: Vec<PutArg>,
	) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;

	fn delete(
		&self,
		arg: DeleteArg,
	) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send;

	fn delete_log(
		&self,
		arg: DeleteLogArg,
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
pub struct ReadLogArg {
	pub process: tg::process::Id,
	pub position: u64,
	pub length: u64,
	pub stream: Option<tg::process::log::Stream>,
}

#[derive(Clone, Debug)]
pub struct PutLogArg {
	pub bytes: Bytes,
	pub process: tg::process::Id,
	pub stream: tg::process::log::Stream,
	pub timestamp: i64,
}

#[derive(Clone, Debug)]
pub struct DeleteArg {
	pub id: tg::object::Id,
	pub now: i64,
	pub ttl: u64,
}

#[derive(Clone, Debug)]
pub struct DeleteLogArg {
	pub process: tg::process::Id,
	pub stream: tg::process::log::Stream,
	pub stream_position: u64,
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
		tangram_serialize::to_writer(&mut bytes, self).map_err(|source| {
			let error: Box<dyn std::error::Error + Send + Sync> = Box::new(tg::error!(
				!source,
				"failed to serialize the cache reference"
			));
			error
		})?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> Result<Self> {
		let bytes = bytes.into();
		let mut reader = std::io::Cursor::new(bytes.as_ref());
		let format = reader.read_u8().map_err(|source| {
			let error: Box<dyn std::error::Error + Send + Sync> =
				Box::new(tg::error!(!source, "failed to read the format byte"));
			error
		})?;
		match format {
			0 => tangram_serialize::from_reader(&mut reader).map_err(|source| {
				let error: Box<dyn std::error::Error + Send + Sync> = Box::new(tg::error!(
					!source,
					"failed to deserialize the cache reference"
				));
				error
			}),
			b'{' => serde_json::from_slice(&bytes).map_err(|source| {
				let error: Box<dyn std::error::Error + Send + Sync> = Box::new(tg::error!(
					!source,
					"failed to deserialize the cache reference"
				));
				error
			}),
			_ => Err("invalid cache reference format".into()),
		}
	}
}
