use crate::{Acker, BatchConfig, Error, Message, StreamConfig, StreamInfo, StreamPublishInfo};
use async_broadcast as broadcast;
use bytes::Bytes;
use dashmap::DashMap;
use futures::{StreamExt as _, future, stream};
use std::{
	collections::{BTreeSet, VecDeque},
	sync::Arc,
};

pub struct Messenger {
	receiver: broadcast::InactiveReceiver<(String, Bytes)>,
	sender: broadcast::Sender<(String, Bytes)>,
	streams: DashMap<String, Arc<Stream>>,
}

struct Stream {
	state: tokio::sync::RwLock<StreamState>,
}

struct StreamState {
	bytes: u64,
	config: StreamConfig,
	consumers: Vec<Consumer>,
	messages: VecDeque<(u64, Bytes)>,
	notify: Arc<tokio::sync::Notify>,
}

struct Consumer {
	delivered: BTreeSet<u64>,
	sequence: usize,
}

impl Messenger {
	#[must_use]
	pub fn new() -> Self {
		let (mut sender, receiver) = async_broadcast::broadcast(1_000_000);
		let receiver = receiver.deactivate();
		sender.set_overflow(true);
		sender.set_await_active(false);
		let streams = DashMap::new();
		Self {
			receiver,
			sender,
			streams,
		}
	}

	async fn publish(&self, subject: String, payload: Bytes) -> Result<(), Error> {
		self.sender.try_broadcast((subject, payload)).ok();
		// for stream in self.streams.iter() {
		// 	if stream.state.read().await.config.subjects.contains(&subject) {
		// 		stream.publish(payload)
		// 	}
		// }
		Ok(())
	}

	async fn subscribe(
		&self,
		subject: String,
		_group: Option<String>,
	) -> Result<impl futures::Stream<Item = Message> + Send + 'static, Error> {
		let stream = self
			.receiver
			.activate_cloned()
			.filter_map(move |(subject_, payload)| {
				future::ready({
					(subject_ == subject).then(|| Message {
						subject: subject_,
						payload,
						acker: Acker::default(),
					})
				})
			});
		Ok(stream)
	}

	async fn create_stream(&self, name: String, config: StreamConfig) -> Result<(), Error> {
		// self.streams
		// 	.entry(name.clone())
		// 	.or_insert_with(|| Stream::new(&name, capacity));
		Ok(())
	}

	async fn delete_stream(&self, name: String) -> Result<(), Error> {
		// self.streams.remove(&name);
		Ok(())
	}

	async fn stream_info(&self, name: String) -> Result<StreamInfo, Error> {
		todo!()
	}

	async fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> Result<impl Future<Output = Result<StreamPublishInfo, Error>>, Error> {
		Ok(future::ok(todo!()))
	}

	async fn stream_subscribe(
		&self,
		name: String,
	) -> Result<impl futures::Stream<Item = Result<Message, Error>> + Send + 'static, Error> {
		Ok(stream::empty())
	}

	async fn stream_batch_publish(
		&self,
		name: String,
		payloads: Vec<Bytes>,
	) -> Result<impl Future<Output = Result<Vec<StreamPublishInfo>, Error>>, Error> {
		Ok(future::ok(todo!()))
	}

	async fn stream_batch_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
		config: BatchConfig,
	) -> Result<impl futures::Stream<Item = Result<Message, Error>> + 'static + Send, Error> {
		Ok(stream::empty())
	}
}

impl Default for Messenger {
	fn default() -> Self {
		Self::new()
	}
}

impl crate::Messenger for Messenger {
	fn publish(&self, subject: String, payload: Bytes) -> impl Future<Output = Result<(), Error>> {
		self.publish(subject, payload)
	}

	fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> impl Future<Output = Result<impl futures::Stream<Item = Message> + 'static, Error>> {
		self.subscribe(subject, group)
	}

	fn get_or_create_stream(
		&self,
		name: String,
		config: StreamConfig,
	) -> impl Future<Output = Result<(), Error>> + Send {
		self.create_stream(name, config)
	}

	fn delete_stream(&self, name: String) -> impl Future<Output = Result<(), Error>> + Send {
		self.delete_stream(name)
	}

	fn stream_info(
		&self,
		name: String,
	) -> impl Future<Output = Result<crate::StreamInfo, Error>> + Send {
		self.stream_info(name)
	}

	fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> impl Future<Output = Result<impl Future<Output = Result<StreamPublishInfo, Error>>, Error>> + Send
	{
		self.stream_publish(name, payload)
	}

	fn stream_subscribe(
		&self,
		name: String,
		_consumer: Option<String>,
	) -> impl Future<
		Output = Result<
			impl futures::Stream<Item = Result<Message, Error>> + Send + 'static,
			Error,
		>,
	> + Send {
		self.stream_subscribe(name)
	}

	fn stream_batch_publish(
		&self,
		name: String,
		payloads: Vec<Bytes>,
	) -> impl Future<
		Output = Result<impl Future<Output = Result<Vec<StreamPublishInfo>, Error>>, Error>,
	> + Send {
		self.stream_batch_publish(name, payloads)
	}

	fn stream_batch_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
		config: crate::BatchConfig,
	) -> impl Future<
		Output = Result<
			impl futures::Stream<Item = Result<Message, Error>> + Send + 'static,
			Error,
		>,
	> + Send {
		self.stream_batch_subscribe(name, consumer, config)
	}
}
