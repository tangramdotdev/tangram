use {
	crate::{Server, Session},
	futures::{future::BoxFuture, stream::BoxStream},
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The indexer component.
pub trait Component: crate::component::Lifecycle {
	fn index<'a>(
		&'a self,
		session: &'a Session,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<()>>>>>;

	fn index_batch<'a>(
		&'a self,
		server: &'a Server,
		arg: tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn index_batch_inner<'a>(
		&'a self,
		server: &'a Server,
		arg: tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<tg::Result<()>>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn get_indexers<'a>(
		&'a self,
		server: &'a Server,
	) -> BoxFuture<'a, tg::Result<Vec<tangram_index::indexer::Indexer>>>;

	fn send_indexer_request<'a>(
		&'a self,
		server: &'a Server,
		indexer: Option<&'a tg::indexer::Id>,
		arg: RequestArg,
	) -> BoxFuture<'a, tg::Result<tg::Result<ResponseOutput>>>;

	fn clean_batch<'a>(
		&'a self,
		server: &'a Server,
		arg: CleanBatchArg,
	) -> BoxFuture<'a, tg::Result<tangram_index::clean::Output>>;

	fn select_indexer<'a>(
		&'a self,
		server: &'a Server,
		index: u64,
		refresh: bool,
	) -> BoxFuture<'a, tg::Result<tg::indexer::Id>>;
}

pub struct CleanBatchArg {
	pub batch_size: usize,
	pub now: i64,
	pub object_time_to_live: Duration,
	pub partition_end: u64,
	pub partition_start: u64,
	pub process_time_to_live: Duration,
	pub sandbox_time_to_live: Duration,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum RequestArg {
	Archive(ArchiveRequestArg),
	Index(IndexRequestArg),
	Wait,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ArchiveRequestArg {
	pub object: tg::object::Id,
	pub put: [u8; 16],
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct IndexRequestArg {
	pub batch: crate::cache::index::queue::batch::Id,
	pub fragment: u64,
	pub fragments: u64,
	#[serde(with = "bytes_base64")]
	pub payload: bytes::Bytes,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseOutput {
	Archive,
	Busy,
	Index,
	Wait,
}

#[must_use]
pub fn database_index_queue_subject() -> String {
	"database.index.queue".to_owned()
}

mod bytes_base64 {
	use serde::{Deserialize as _, Deserializer, Serializer};

	pub fn deserialize<'de, D>(deserializer: D) -> Result<bytes::Bytes, D::Error>
	where
		D: Deserializer<'de>,
	{
		let value = String::deserialize(deserializer)?;
		let bytes = data_encoding::BASE64
			.decode(value.as_bytes())
			.map_err(serde::de::Error::custom)?;

		Ok(bytes.into())
	}

	pub fn serialize<S>(value: &bytes::Bytes, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_str(&data_encoding::BASE64.encode(value))
	}
}
