use bytes::Bytes;
use futures::{FutureExt as _, Stream, future::BoxFuture};

pub mod either;
pub mod memory;
pub mod nats;

pub struct Message {
	pub subject: String,
	pub payload: Bytes,
	pub acker: Acker,
}

#[derive(Clone, Debug)]
pub struct StreamInfo {
	/// The lowest sequence number in the stream. None if the stream is empty.
	pub first_sequence: Option<u64>,

	/// The most recently assigned sequence number.
	pub last_sequence: u64,
}

pub struct Acker {
	ack: Option<BoxFuture<'static, Result<(), StdError>>>,
	acked: bool,
	retry: Option<BoxFuture<'static, ()>>,
}

type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub trait Messenger {
	type Error: std::error::Error + Send + 'static;

	fn publish(
		&self,
		subject: String,
		payload: Bytes,
	) -> impl Future<Output = Result<(), Self::Error>> + Send;

	fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> impl Future<Output = Result<impl Stream<Item = Message> + Send + 'static, Self::Error>> + Send;

	fn create_stream(&self, name: String) -> impl Future<Output = Result<(), Self::Error>> + Send;

	fn destroy_stream(&self, name: String) -> impl Future<Output = Result<(), Self::Error>> + Send;

	fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> impl Future<Output = Result<(), Self::Error>> + Send;

	fn stream_subscribe(
		&self,
		name: String,
		consumer_name: Option<String>,
	) -> impl Future<
		Output = Result<
			impl Stream<Item = Result<Message, Self::Error>> + Send + 'static,
			Self::Error,
		>,
	> + Send;

	fn stream_info(
		&self,
		name: String,
	) -> impl Future<Output = Result<StreamInfo, Self::Error>> + Send;
}

impl Default for Acker {
	fn default() -> Self {
		Self {
			ack: None,
			acked: true,
			retry: None,
		}
	}
}

impl Acker {
	pub fn new(
		ack: impl Future<Output = Result<(), StdError>> + Send + 'static,
		retry: impl Future<Output = ()> + Send + 'static,
	) -> Self {
		Self {
			ack: Some(ack.boxed()),
			acked: false,
			retry: Some(retry.boxed()),
		}
	}

	pub async fn ack(mut self) -> Result<(), StdError> {
		if let Some(fut) = self.ack.take() {
			fut.await?;
		}
		self.acked = true;
		Ok(())
	}
}

impl Drop for Acker {
	fn drop(&mut self) {
		drop(self.ack.take());
		if let (false, Some(retry)) = (self.acked, self.retry.take()) {
			tokio::spawn(retry);
		}
	}
}

impl Message {
	pub fn split(self) -> (Bytes, Acker) {
		(self.payload, self.acker)
	}
}
