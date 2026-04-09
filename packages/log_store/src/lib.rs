use {
	bytes::Bytes,
	std::{borrow::Cow, collections::BTreeSet},
	tangram_client::prelude::*,
};

#[cfg(feature = "foundationdb")]
pub mod fdb;
#[cfg(feature = "lmdb")]
pub mod lmdb;
pub mod memory;

pub mod prelude {
	pub use super::Store as _;
}

#[derive(Clone, Debug)]
pub struct ReadProcessLogArg {
	pub length: u64,
	pub position: u64,
	pub process: tg::process::Id,
	pub streams: BTreeSet<tg::process::stdio::Stream>,
}

#[derive(Clone, Debug)]
pub struct PutProcessLogArg {
	pub bytes: Bytes,
	pub process: tg::process::Id,
	pub stream: tg::process::stdio::Stream,
	pub timestamp: i64,
}

#[derive(Clone, Debug)]
pub struct DeleteProcessLogArg {
	pub process: tg::process::Id,
}

#[derive(Clone, Debug, tangram_serialize::Serialize, tangram_serialize::Deserialize)]
pub struct ProcessLogEntry<'a> {
	#[tangram_serialize(id = 0)]
	pub bytes: Cow<'a, [u8]>,

	#[tangram_serialize(id = 1)]
	pub position: u64,

	#[tangram_serialize(id = 2)]
	pub stream: tg::process::stdio::Stream,

	#[tangram_serialize(id = 3)]
	pub stream_position: u64,

	#[tangram_serialize(id = 4)]
	pub timestamp: i64,
}

pub trait Store {
	fn try_read_process_log(
		&self,
		arg: ReadProcessLogArg,
	) -> impl std::future::Future<Output = tg::Result<Vec<ProcessLogEntry<'static>>>> + Send;

	fn try_get_process_log_length(
		&self,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> impl std::future::Future<Output = tg::Result<Option<u64>>> + Send;

	fn put_process_log(
		&self,
		arg: PutProcessLogArg,
	) -> impl std::future::Future<Output = tg::Result<()>> + Send;

	fn delete_process_log(
		&self,
		arg: DeleteProcessLogArg,
	) -> impl std::future::Future<Output = tg::Result<()>> + Send;
}

impl ProcessLogEntry<'_> {
	#[must_use]
	pub fn into_static(self) -> ProcessLogEntry<'static> {
		ProcessLogEntry {
			bytes: Cow::Owned(self.bytes.into_owned()),
			position: self.position,
			stream: self.stream,
			stream_position: self.stream_position,
			timestamp: self.timestamp,
		}
	}
}
