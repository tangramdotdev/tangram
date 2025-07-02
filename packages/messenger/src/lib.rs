use bytes::Bytes;
use std::time::Duration;

pub use self::acker::Acker;

pub mod acker;
pub mod either;
pub mod memory;
#[cfg(feature = "nats")]
pub mod nats;

pub mod prelude {
	pub use super::{Consumer as _, Messenger as _, Stream as _};
}

pub struct Message {
	pub subject: String,
	pub payload: Bytes,
	pub acker: Acker,
}

#[derive(Clone, Debug, Default)]
pub struct StreamConfig {
	pub discard: DiscardPolicy,
	pub max_bytes: Option<u64>,
	pub max_messages: Option<u64>,
	pub retention: RetentionPolicy,
}

#[derive(Clone, Debug, Default)]
pub enum DiscardPolicy {
	#[default]
	Old,
	New,
}

#[derive(Clone, Debug, Default)]
pub enum RetentionPolicy {
	#[default]
	Limits,
	Interest,
	WorkQueue,
}

#[derive(Clone, Debug)]
pub struct StreamInfo {
	pub first_sequence: u64,
	pub last_sequence: u64,
}

#[derive(Clone, Debug, Default)]
pub struct ConsumerConfig {
	pub deliver: DeliverPolicy,
}

#[derive(Clone, Debug, Default)]
pub enum DeliverPolicy {
	#[default]
	All,
	New,
}

#[derive(Debug)]
pub struct ConsumerInfo {
	pub sequence: u64,
}

#[derive(Default)]
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
	type Stream: Stream;

	fn publish(
		&self,
		subject: String,
		payload: Bytes,
	) -> impl Future<Output = Result<(), Error>> + Send;

	fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> impl Future<Output = Result<impl futures::Stream<Item = Message> + Send + 'static, Error>> + Send;

	fn get_stream(&self, name: String) -> impl Future<Output = Result<Self::Stream, Error>> + Send;

	fn create_stream(
		&self,
		name: String,
		config: StreamConfig,
	) -> impl Future<Output = Result<Self::Stream, Error>> + Send;

	fn get_or_create_stream(
		&self,
		name: String,
		config: StreamConfig,
	) -> impl Future<Output = Result<Self::Stream, Error>> + Send;

	fn delete_stream(&self, name: String) -> impl Future<Output = Result<(), Error>> + Send;

	fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> impl Future<Output = Result<impl Future<Output = Result<u64, Error>>, Error>> + Send;

	fn stream_batch_publish(
		&self,
		name: String,
		payloads: Vec<Bytes>,
	) -> impl Future<Output = Result<impl Future<Output = Result<Vec<u64>, Error>> + Send, Error>> + Send;
}

pub trait Stream {
	type Consumer: Consumer;

	fn info(&self) -> impl Future<Output = Result<StreamInfo, Error>> + Send;

	fn get_consumer(
		&self,
		name: String,
	) -> impl Future<Output = Result<Self::Consumer, Error>> + Send;

	fn create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> impl Future<Output = Result<Self::Consumer, Error>> + Send;

	fn get_or_create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> impl Future<Output = Result<Self::Consumer, Error>> + Send;

	fn delete_consumer(&self, name: String) -> impl Future<Output = Result<(), Error>> + Send;
}

pub trait Consumer {
	fn info(&self) -> impl Future<Output = Result<ConsumerInfo, Error>> + Send;

	fn subscribe(
		&self,
	) -> impl Future<
		Output = Result<
			impl futures::Stream<Item = Result<Message, Error>> + Send + 'static,
			Error,
		>,
	> + Send;

	fn batch_subscribe(
		&self,
		config: BatchConfig,
	) -> impl Future<
		Output = Result<
			impl futures::Stream<Item = Result<Message, Error>> + Send + 'static,
			Error,
		>,
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
