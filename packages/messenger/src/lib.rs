use bytes::Bytes;
use futures::{Future, Stream};

pub mod either;
pub mod memory;
pub mod nats;

pub trait Messenger {
	type Error: std::error::Error + Send + Sync + 'static;

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
}

#[derive(Clone)]
pub struct Message {
	pub subject: String,
	pub payload: Bytes,
}
