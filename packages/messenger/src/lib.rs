use std::future::Future;

pub use self::payload::Payload;

pub mod either;
pub mod memory;
#[cfg(feature = "nats")]
pub mod nats;
pub mod payload;

pub mod prelude {
	pub use super::{Delivery, Messenger as _};
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Delivery {
	One,
	#[default]
	All,
}

pub struct Message<T> {
	pub subject: String,
	pub payload: T,
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub enum Error {
	#[display("serialization failed")]
	Serialization(Box<dyn std::error::Error + Send + Sync + 'static>),
	#[display("deserialization failed")]
	Deserialization(Box<dyn std::error::Error + Send + Sync + 'static>),
	Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}

pub trait Messenger {
	fn publish<T>(
		&self,
		subject: String,
		payload: T,
	) -> impl Future<Output = Result<(), Error>> + Send
	where
		T: Payload;

	fn subscribe<T>(
		&self,
		subject: String,
	) -> impl Future<
		Output = Result<
			impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static,
			Error,
		>,
	> + Send
	where
		T: Payload;

	fn subscribe_with_delivery<T>(
		&self,
		subject: String,
		delivery: Delivery,
	) -> impl Future<
		Output = Result<
			impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static,
			Error,
		>,
	> + Send
	where
		T: Payload;
}

impl Error {
	pub fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>) -> Self {
		Self::Other(error.into())
	}

	pub fn serialization(
		error: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
	) -> Self {
		Self::Serialization(error.into())
	}

	pub fn deserialization(
		error: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
	) -> Self {
		Self::Deserialization(error.into())
	}
}
