use bytes::Bytes;
use futures::{Stream, future::BoxFuture};

pub use self::acker::Acker;

pub mod acker;
pub mod either;
pub mod memory;
pub mod nats;

pub struct PublishFuture<E> {
	inner: BoxFuture<'static, Result<Published, E>>,
}

pub struct Published {
	pub sequence: u64,
}

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

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

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

	fn delete_stream(&self, name: String) -> impl Future<Output = Result<(), Self::Error>> + Send;

	fn stream_info(
		&self,
		name: String,
	) -> impl Future<Output = Result<StreamInfo, Self::Error>> + Send;

	fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> impl Future<Output = Result<PublishFuture<Self::Error>, Self::Error>> + Send;

	fn stream_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
	) -> impl Future<
		Output = Result<
			impl Stream<Item = Result<Message, Self::Error>> + Send + 'static,
			Self::Error,
		>,
	> + Send;
}

impl Message {
	pub fn split(self) -> (Bytes, Acker) {
		(self.payload, self.acker)
	}
}

impl<E> Future for PublishFuture<E> {
	type Output = Result<Published, E>;
	fn poll(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Self::Output> {
		let future = &mut self.get_mut().inner;
		std::pin::pin!(future).poll(cx)
	}
}
