use {
	bytes::Bytes,
	std::{borrow::Cow, path::PathBuf},
	tangram_client::prelude::*,
};

#[cfg(feature = "lmdb")]
pub mod lmdb;
pub mod memory;
#[cfg(feature = "scylla")]
pub mod scylla;

pub mod prelude {
	pub use super::Store as _;
}

#[derive(Clone, Debug)]
pub struct PutArg {
	pub bytes: Option<Bytes>,
	pub cache_pointer: Option<CachePointer>,
	pub id: tg::object::Id,
	pub stored_at: i64,
}

#[derive(Clone, Debug)]
pub struct DeleteArg {
	pub id: tg::object::Id,
	pub now: i64,
	pub ttl: u64,
}

#[derive(Clone, Debug, tangram_serialize::Serialize, tangram_serialize::Deserialize)]
pub struct Object<'a> {
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub bytes: Option<Cow<'a, [u8]>>,

	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub cache_pointer: Option<CachePointer>,

	#[tangram_serialize(id = 2)]
	pub stored_at: i64,
}

#[derive(
	Clone,
	Debug,
	serde::Serialize,
	serde::Deserialize,
	tangram_serialize::Serialize,
	tangram_serialize::Deserialize,
)]
pub struct CachePointer {
	#[tangram_serialize(id = 0)]
	pub artifact: tg::artifact::Id,

	#[tangram_serialize(id = 1)]
	pub length: u64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[tangram_serialize(id = 3)]
	pub position: u64,
}

pub trait Store {
	fn try_get(
		&self,
		id: &tg::object::Id,
	) -> impl std::future::Future<Output = tg::Result<Option<Object<'static>>>> + Send;

	fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
	) -> impl std::future::Future<Output = tg::Result<Vec<Option<Object<'static>>>>> + Send;

	fn put(&self, arg: PutArg) -> impl std::future::Future<Output = tg::Result<()>> + Send;

	fn put_batch(
		&self,
		args: Vec<PutArg>,
	) -> impl std::future::Future<Output = tg::Result<()>> + Send;

	fn delete(&self, arg: DeleteArg) -> impl std::future::Future<Output = tg::Result<()>> + Send;

	fn delete_batch(
		&self,
		args: Vec<DeleteArg>,
	) -> impl std::future::Future<Output = tg::Result<()>> + Send;

	fn flush(&self) -> impl std::future::Future<Output = tg::Result<()>> + Send;
}

impl Object<'_> {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|source| tg::error!(!source, "failed to serialize the object value"))?;
		Ok(bytes.into())
	}
}

impl Object<'static> {
	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let bytes = bytes.as_ref();
		if bytes.is_empty() {
			return Err(tg::error!("empty object value data"));
		}
		let format = bytes[0];
		match format {
			0 => {
				let object: Object<'_> =
					tangram_serialize::from_slice(&bytes[1..]).map_err(|source| {
						tg::error!(!source, "failed to deserialize the object value")
					})?;
				Ok(object.into_static())
			},
			_ => Err(tg::error!("invalid object value format")),
		}
	}
}

impl Object<'_> {
	#[must_use]
	pub fn into_static(self) -> Object<'static> {
		Object {
			bytes: self.bytes.map(|bytes| Cow::Owned(bytes.into_owned())),
			cache_pointer: self.cache_pointer,
			stored_at: self.stored_at,
		}
	}
}

impl CachePointer {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|source| tg::error!(!source, "failed to serialize the cache pointer"))?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let bytes = bytes.as_ref();
		if bytes.is_empty() {
			return Err(tg::error!("empty cache pointer data"));
		}
		let format = bytes[0];
		match format {
			0 => tangram_serialize::from_slice(&bytes[1..])
				.map_err(|source| tg::error!(!source, "failed to deserialize the cache pointer")),
			b'{' => serde_json::from_slice(bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the cache pointer")),
			_ => Err(tg::error!("invalid cache pointer format")),
		}
	}
}
