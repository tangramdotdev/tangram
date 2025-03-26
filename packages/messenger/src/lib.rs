use bytes::Bytes;
use futures::Stream;

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
}

#[derive(Clone, Debug)]
pub struct Message {
	pub subject: String,
	pub payload: Bytes,
}
