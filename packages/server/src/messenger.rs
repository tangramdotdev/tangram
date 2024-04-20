use async_nats as nats;
use bytes::Bytes;
use futures::{future, Stream, StreamExt as _};
use tangram_client as tg;
use tokio_stream::wrappers::BroadcastStream;

#[derive(Debug)]
pub enum Messenger {
	Channel(tokio::sync::broadcast::Sender<Message>),
	Nats(nats::Client),
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
	Channel,
	Nats,
}

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Debug, derive_more::TryUnwrap)]
pub enum Message {
	BuildCreated,
	BuildChildren(tg::build::Id),
	BuildLog(tg::build::Id),
	BuildStatus(tg::build::Id),
}

impl Messenger {
	pub fn new_channel() -> Self {
		let sender = tokio::sync::broadcast::Sender::new(1_000_000);
		Self::Channel(sender)
	}

	pub fn new_nats(client: nats::Client) -> Self {
		Self::Nats(client)
	}

	pub fn kind(&self) -> Kind {
		match self {
			Messenger::Channel(_) => Kind::Channel,
			Messenger::Nats(_) => Kind::Nats,
		}
	}

	pub async fn publish_to_build_created(&self) -> tg::Result<()> {
		match self {
			Self::Channel(sender) => {
				sender.send(Message::BuildCreated).ok();
			},
			Self::Nats(client) => {
				let subject = "builds.created";
				let payload = Bytes::new();
				client
					.publish(subject, payload)
					.await
					.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
			},
		}
		Ok(())
	}

	pub async fn subscribe_to_build_created(&self) -> tg::Result<impl Stream<Item = ()> + 'static> {
		match self {
			Messenger::Channel(sender) => Ok(BroadcastStream::new(sender.subscribe())
				.filter_map(|result| future::ready(result.ok()))
				.filter_map(|message| future::ready(message.try_unwrap_build_created().ok()))
				.left_stream()),
			Messenger::Nats(client) => {
				let subject = "builds.created";
				Ok(client
					.subscribe(subject)
					.await
					.map_err(|source| tg::error!(!source, "failed to subscribe"))?
					.map(|_| ())
					.right_stream())
			},
		}
	}

	pub async fn publish_to_build_children(&self, id: &tg::build::Id) -> tg::Result<()> {
		match self {
			Self::Channel(sender) => {
				sender.send(Message::BuildChildren(id.clone())).ok();
			},
			Self::Nats(client) => {
				let subject = format!("builds.{id}.children");
				let payload = Bytes::new();
				client
					.publish(subject, payload)
					.await
					.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
			},
		}
		Ok(())
	}

	pub async fn subscribe_to_build_children(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<impl Stream<Item = ()> + 'static> {
		match self {
			Messenger::Channel(sender) => Ok(BroadcastStream::new(sender.subscribe())
				.filter_map(|result| future::ready(result.ok()))
				.filter_map(|message| future::ready(message.try_unwrap_build_children().ok()))
				.map(|_| ())
				.left_stream()),
			Messenger::Nats(client) => {
				let subject = format!("builds.{id}.children");
				Ok(client
					.subscribe(subject)
					.await
					.map_err(|source| tg::error!(!source, "failed to subscribe"))?
					.map(|_| ())
					.right_stream())
			},
		}
	}

	pub async fn publish_to_build_log(&self, id: &tg::build::Id) -> tg::Result<()> {
		match self {
			Self::Channel(sender) => {
				sender.send(Message::BuildLog(id.clone())).ok();
			},
			Self::Nats(client) => {
				let subject = format!("builds.{id}.log");
				let payload = Bytes::new();
				client
					.publish(subject, payload)
					.await
					.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
			},
		}
		Ok(())
	}

	pub async fn subscribe_to_build_log(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<impl Stream<Item = ()> + 'static> {
		match self {
			Messenger::Channel(sender) => Ok(BroadcastStream::new(sender.subscribe())
				.filter_map(|result| future::ready(result.ok()))
				.filter_map(|message| future::ready(message.try_unwrap_build_log().ok()))
				.map(|_| ())
				.left_stream()),
			Messenger::Nats(client) => {
				let subject = format!("builds.{id}.log");
				Ok(client
					.subscribe(subject)
					.await
					.map_err(|source| tg::error!(!source, "failed to subscribe"))?
					.map(|_| ())
					.right_stream())
			},
		}
	}

	pub async fn publish_to_build_status(&self, id: &tg::build::Id) -> tg::Result<()> {
		match self {
			Self::Channel(sender) => {
				sender.send(Message::BuildStatus(id.clone())).ok();
			},
			Self::Nats(client) => {
				let subject = format!("builds.{id}.status");
				let payload = Bytes::new();
				client
					.publish(subject, payload)
					.await
					.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
			},
		}
		Ok(())
	}

	pub async fn subscribe_to_build_status(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<impl Stream<Item = ()> + 'static> {
		match self {
			Messenger::Channel(sender) => Ok(BroadcastStream::new(sender.subscribe())
				.filter_map(|result| future::ready(result.ok()))
				.filter_map(|message| future::ready(message.try_unwrap_build_status().ok()))
				.map(|_| ())
				.left_stream()),
			Messenger::Nats(client) => {
				let subject = format!("builds.{id}.status");
				Ok(client
					.subscribe(subject)
					.await
					.map_err(|source| tg::error!(!source, "failed to subscribe"))?
					.map(|_| ())
					.right_stream())
			},
		}
	}
}
