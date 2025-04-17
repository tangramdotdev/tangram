use bytes::Bytes;
use futures::Stream;
use std::time::Duration;

pub use self::acker::Acker;

pub mod acker;
pub mod either;
pub mod memory;
pub mod nats;

pub struct Message {
	pub subject: String,
	pub payload: Bytes,
	pub acker: Acker,
}

#[derive(Clone, Debug)]
pub struct StreamConfig {
	pub max_bytes: Option<u64>,
	pub max_messages: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct StreamInfo {
	pub first_sequence: Option<u64>,
	pub last_sequence: u64,
}

pub struct StreamPublishInfo {
	pub sequence: u64,
}

pub struct BatchConfig {
	pub max_bytes: Option<u64>,
	pub max_messages: Option<u64>,
	pub timeout: Option<Duration>,
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub enum Error {
	NotFound,
	MaxMessages,
	MaxBytes,
	PublishFailed,
	Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}

pub trait Messenger {
	fn publish(
		&self,
		subject: String,
		payload: Bytes,
	) -> impl Future<Output = Result<(), Error>> + Send;

	fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> impl Future<Output = Result<impl Stream<Item = Message> + Send + 'static, Error>> + Send;

	fn get_or_create_stream(
		&self,
		name: String,
		config: StreamConfig,
	) -> impl Future<Output = Result<(), Error>> + Send;

	fn delete_stream(&self, name: String) -> impl Future<Output = Result<(), Error>> + Send;

	fn stream_info(&self, name: String) -> impl Future<Output = Result<StreamInfo, Error>> + Send;

	fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> impl Future<Output = Result<impl Future<Output = Result<StreamPublishInfo, Error>>, Error>> + Send;

	fn stream_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
	) -> impl Future<
		Output = Result<impl Stream<Item = Result<Message, Error>> + Send + 'static, Error>,
	> + Send;

	fn stream_batch_publish(
		&self,
		name: String,
		payloads: Vec<Bytes>,
	) -> impl Future<
		Output = Result<impl Future<Output = Result<Vec<StreamPublishInfo>, Error>>, Error>,
	> + Send;

	fn stream_batch_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
		config: BatchConfig,
	) -> impl Future<
		Output = Result<impl Stream<Item = Result<Message, Error>> + Send + 'static, Error>,
	> + Send;
}

impl Message {
	pub fn split(self) -> (Bytes, Acker) {
		(self.payload, self.acker)
	}
}

impl Error {
	pub fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>) -> Self {
		Self::Other(error.into())
	}
}
