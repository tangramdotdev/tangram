use {
	crate::Server,
	bytes::Bytes,
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_messenger::{self as messenger, prelude::*},
};

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Message {
	#[tangram_serialize(id = 0)]
	pub(crate) id: tg::process::Id,
}

impl Server {
	pub(crate) async fn finalizer_task(&self, config: &crate::config::Finalizer) -> tg::Result<()> {
		// Get the message stream.
		let stream = self.finalizer_create_message_stream(config).await?;
		let mut stream = pin!(stream);

		loop {
			// Handle the result.
			let message = match stream.try_next().await {
				Ok(Some(messages)) => messages,
				Ok(None) => {
					panic!("the stream ended")
				},
				Err(error) => {
					tracing::error!(?error, "failed to get a batch of messages");
					tokio::time::sleep(Duration::from_secs(1)).await;
					continue;
				},
			};

			// Handle the message.
			let result = self.finalizer_handle_message(config, message).await;
			if let Err(error) = result {
				tracing::error!(?error, "failed to handle the messages");
				tokio::time::sleep(Duration::from_secs(1)).await;
			} else {
				// Publish finalizer progress.
				self.messenger
					.publish("finalizer_progress".to_owned(), ())
					.await
					.ok();
			}
		}
	}

	async fn finalizer_create_message_stream(
		&self,
		config: &crate::config::Finalizer,
	) -> tg::Result<impl Stream<Item = tg::Result<Vec<(Message, messenger::Acker)>>>> {
		let stream = self
			.messenger
			.get_stream("finalize".to_owned())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the finalize stream"))?;
		let consumer = stream
			.get_consumer("finalize".to_owned())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the finalize consumer"))?;
		let batch_config = messenger::BatchConfig {
			max_bytes: None,
			max_messages: Some(config.message_batch_size.to_u64().unwrap()),
			timeout: Some(config.message_batch_timeout),
		};
		let stream = consumer
			.batch_subscribe::<Message>(batch_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the stream"))?
			.boxed()
			.map_err(|source| tg::error!(!source, "failed to get a message from the stream"))
			.map_ok(|message| {
				let (message, acker) = message.split();
				(message, acker)
			})
			.inspect_err(|error| {
				tracing::error!(?error);
			})
			.filter_map(|result| future::ready(result.ok()));
		let stream = tokio_stream::StreamExt::chunks_timeout(
			stream,
			config.message_batch_size,
			config.message_batch_timeout,
		)
		.map(Ok);
		Ok(stream)
	}

	async fn finalizer_handle_message(
		&self,
		_config: &crate::config::Finalizer,
		messages: Vec<(Message, messenger::Acker)>,
	) -> tg::Result<()> {
		// Handle the messages.
		for (message, acker) in messages {
			let process = message.id;

			// Compact the log.
			self.compact_process_log(&process)
				.boxed()
				.await
				.inspect_err(|error| tracing::error!(?error, %process, "failed to compact log"))
				.ok();

			// Spawn the index task.
			self.finalizer_spawn_index_task(&process).await?;

			// Acknowledge the message.
			acker
				.ack()
				.await
				.map_err(|source| tg::error!(!source, "failed to acknowledge the message"))?;
		}
		Ok(())
	}

	async fn finalizer_spawn_index_task(&self, id: &tg::process::Id) -> tg::Result<()> {
		let tg::process::get::Output { data, .. } = self
			.try_get_process_local(id, false)
			.await?
			.ok_or_else(|| tg::error!("failed to find the process"))?;
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let command = (
			data.command.clone().into(),
			tangram_index::ProcessObjectKind::Command,
		);
		let errors = data
			.error
			.as_ref()
			.into_iter()
			.flat_map(|error| match error {
				tg::Either::Left(data) => {
					let mut children = BTreeSet::new();
					data.children(&mut children);
					children
						.into_iter()
						.map(|object| {
							let kind = tangram_index::ProcessObjectKind::Error;
							(object, kind)
						})
						.collect::<Vec<_>>()
				},
				tg::Either::Right(id) => {
					let id = id.clone().into();
					let kind = tangram_index::ProcessObjectKind::Error;
					vec![(id, kind)]
				},
			});
		let log = data.log.as_ref().map(|id| {
			let id = id.clone().into();
			let kind = tangram_index::ProcessObjectKind::Log;
			(id, kind)
		});
		let mut outputs = BTreeSet::new();
		if let Some(output) = &data.output {
			output.children(&mut outputs);
		}
		let outputs = outputs.into_iter().map(|object| {
			let kind = tangram_index::ProcessObjectKind::Output;
			(object, kind)
		});
		let objects = std::iter::once(command)
			.chain(errors)
			.chain(log)
			.chain(outputs)
			.collect();
		let children = data
			.children
			.as_ref()
			.ok_or_else(|| tg::error!("expected the children to be set"))?
			.iter()
			.map(|referent| referent.item.clone())
			.collect();
		let put_process_arg = tangram_index::PutProcessArg {
			children,
			stored: tangram_index::ProcessStored::default(),
			id: id.clone(),
			metadata: tg::process::Metadata::default(),
			objects,
			touched_at: now,
		};
		self.index_tasks
			.spawn(|_| {
				let server = self.clone();
				async move {
					if let Err(error) = server
						.index
						.put(tangram_index::PutArg {
							processes: vec![put_process_arg],
							..Default::default()
						})
						.await
					{
						tracing::error!(?error, "failed to put process to index");
					}
				}
			})
			.detach();
		Ok(())
	}
}

impl Message {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let bytes = bytes.as_ref();
		if bytes.is_empty() {
			return Err(tg::error!("missing format byte"));
		}
		let format = bytes[0];
		match format {
			0 => tangram_serialize::from_slice(&bytes[1..])
				.map_err(|source| tg::error!(!source, "failed to deserialize the message")),
			b'{' => serde_json::from_slice(bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the message")),
			_ => Err(tg::error!("invalid format")),
		}
	}
}

impl messenger::Payload for Message {
	fn serialize(&self) -> Result<Bytes, messenger::Error> {
		Message::serialize(self).map_err(messenger::Error::other)
	}

	fn deserialize(bytes: Bytes) -> Result<Self, messenger::Error> {
		Message::deserialize(bytes).map_err(messenger::Error::other)
	}
}
