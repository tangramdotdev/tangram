use {
	crate::{Server, handle::ServerOrProxy},
	futures::{Stream, StreamExt as _, TryStreamExt as _, future},
	num::ToPrimitive as _,
	std::{pin::pin, task::Poll, time::Duration},
	tangram_client::{self as tg, Handle as _},
	tangram_database as db,
	tangram_futures::{stream::Ext as _, task::Stop},
	tangram_http::{Body, request::Ext as _},
	tangram_messenger::{self as messenger, Acker, prelude::*},
	tokio_util::task::AbortOnDropHandle,
};

#[cfg(feature = "postgres")]
pub mod postgres;
pub mod sqlite;

pub use self::message::Message;

pub mod message;

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Index {
	#[cfg(feature = "postgres")]
	Postgres(db::postgres::Database),
	Sqlite(db::sqlite::Database),
}

impl Server {
	pub async fn index(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		let progress = crate::progress::Handle::new();
		let task = AbortOnDropHandle::new(tokio::spawn({
			let progress = progress.clone();
			let server = self.clone();
			async move {
				// Get the stream.
				let stream = server
					.messenger
					.get_stream("index".to_owned())
					.await
					.map_err(|source| tg::error!(!source, "failed to get the index stream"))?;

				// Wait for outstanding tasks to complete.
				server.tasks.wait().await;

				// Wait for the index stream's first sequence to reach the current last sequence.
				let info = stream
					.info()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the index stream info"))?;
				let mut first_sequence = info.first_sequence;
				let last_sequence = info.last_sequence;
				let total = info.last_sequence.saturating_sub(info.first_sequence);
				progress.start(
					"messages".to_string(),
					"messages".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(0),
					Some(total),
				);
				while first_sequence < last_sequence {
					let info = stream.info().await.map_err(|source| {
						tg::error!(!source, "failed to get the index stream info")
					})?;
					progress.increment("messages", info.first_sequence - first_sequence);
					first_sequence = info.first_sequence;
					tokio::time::sleep(Duration::from_millis(10)).await;
				}
				progress.finish("messages");

				// Wait until the index's queue no longer has items whose transaction id is less than or equal to the current transaction id.
				let transaction_id = server.indexer_get_transaction_id().await?;
				let count = server.indexer_get_queue_size(transaction_id).await?;
				progress.start(
					"queue".to_string(),
					"queue".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(count),
					None,
				);
				loop {
					let count = server.indexer_get_queue_size(transaction_id).await?;
					progress.set("queue", count);
					if count == 0 {
						break;
					}
					tokio::time::sleep(Duration::from_millis(10)).await;
				}
				progress.finish("queue");

				progress.output(());

				Ok::<_, tg::Error>(())
			}
		}));
		let stream = progress.stream().attach(task);
		Ok(stream)
	}

	pub(crate) async fn indexer_task(&self, config: &crate::config::Indexer) -> tg::Result<()> {
		// Get the messages stream.
		let stream = self.indexer_create_message_stream(config).await?;
		let mut stream = pin!(stream);

		let mut wait = false;
		loop {
			let result = if wait {
				stream.try_next().await
			} else {
				match futures::poll!(stream.try_next()) {
					Poll::Ready(result) => result,
					Poll::Pending => {
						let result = self.indexer_handle_queue(config).await;
						let n = match result {
							Ok(n) => n,
							Err(error) => {
								tracing::error!(?error, "failed to handle the index queue");
								tokio::time::sleep(Duration::from_secs(1)).await;
								continue;
							},
						};
						if n == 0 {
							wait = true;
						}
						continue;
					},
				}
			};

			// Handle the result.
			let messages = match result {
				Ok(Some(messages)) => {
					wait = false;
					messages
				},
				Ok(None) => {
					panic!("the stream ended")
				},
				Err(error) => {
					tracing::error!(?error, "failed to get a batch of messages");
					tokio::time::sleep(Duration::from_secs(1)).await;
					continue;
				},
			};

			// Handle the messages.
			let result = self.indexer_handle_messages(config, messages).await;
			if let Err(error) = result {
				tracing::error!(?error, "failed to handle the messages");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn indexer_create_message_stream(
		&self,
		config: &crate::config::Indexer,
	) -> tg::Result<impl Stream<Item = tg::Result<Vec<(Vec<Message>, Acker)>>>> {
		let stream = self
			.messenger
			.get_stream("index".to_owned())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the index stream"))?;
		let consumer = stream
			.get_consumer("index".to_owned())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the index consumer"))?;
		let batch_config = messenger::BatchConfig {
			max_bytes: None,
			max_messages: Some(config.message_batch_size.to_u64().unwrap()),
			timeout: Some(config.message_batch_timeout),
		};
		let stream = consumer
			.batch_subscribe(batch_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the stream"))?
			.boxed()
			.map_err(|source| tg::error!(!source, "failed to get a message from the stream"))
			.and_then(|message| async {
				let (payload, acker) = message.split();
				let len = payload.len();
				let mut position = 0usize;
				let mut messages = Vec::new();
				while position < len {
					let message = Message::deserialize(&payload[position..])
						.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
					let serialized = message.serialize()?;
					position += serialized.len();
					messages.push(message);
				}
				Ok::<_, tg::Error>((messages, acker))
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

	async fn indexer_handle_messages(
		&self,
		config: &crate::config::Indexer,
		messages: Vec<(Vec<Message>, Acker)>,
	) -> tg::Result<()> {
		// Create the state.
		let mut n = 0;
		let mut put_cache_entry_messages = Vec::new();
		let mut put_object_messages = Vec::new();
		let mut touch_object_messages = Vec::new();
		let mut put_process_messages = Vec::new();
		let mut touch_process_messages = Vec::new();
		let mut put_tag_messages = Vec::new();
		let mut delete_tag_messages = Vec::new();
		let mut ackers: Vec<Acker> = Vec::new();

		for (messages, acker) in messages {
			for message in messages {
				if n >= config.insert_batch_size {
					// Handle the messages.
					match &self.index {
						#[cfg(feature = "postgres")]
						Index::Postgres(index) => {
							self.indexer_task_handle_messages_postgres(
								index,
								put_cache_entry_messages,
								put_object_messages,
								touch_object_messages,
								put_process_messages,
								touch_process_messages,
								put_tag_messages,
								delete_tag_messages,
							)
							.await?;
						},
						Index::Sqlite(index) => {
							self.indexer_task_handle_messages_sqlite(
								index,
								put_cache_entry_messages,
								put_object_messages,
								touch_object_messages,
								put_process_messages,
								touch_process_messages,
								put_tag_messages,
								delete_tag_messages,
							)
							.await?;
						},
					}

					// Acknowledge the messages.
					future::try_join_all(ackers.drain(..).map(async |acker| {
						acker.ack().await.map_err(|source| {
							tg::error!(!source, "failed to acknowledge the message")
						})?;
						Ok::<_, tg::Error>(())
					}))
					.await?;

					// Reset the state.
					n = 0;
					put_cache_entry_messages = Vec::new();
					put_object_messages = Vec::new();
					touch_object_messages = Vec::new();
					put_process_messages = Vec::new();
					touch_process_messages = Vec::new();
					put_tag_messages = Vec::new();
					delete_tag_messages = Vec::new();
				}

				// Add the message.
				n += 1;
				match message {
					Message::PutCacheEntry(message) => {
						put_cache_entry_messages.push(message);
					},
					Message::PutObject(message) => {
						put_object_messages.push(message);
					},
					Message::TouchObject(message) => {
						touch_object_messages.push(message);
					},
					Message::PutProcess(message) => {
						put_process_messages.push(message);
					},
					Message::TouchProcess(message) => {
						touch_process_messages.push(message);
					},
					Message::PutTag(message) => {
						put_tag_messages.push(message);
					},
					Message::DeleteTag(message) => {
						delete_tag_messages.push(message);
					},
				}
			}

			// Add the acker.
			ackers.push(acker);
		}

		if n == 0 {
			return Ok(());
		}

		// Handle the messages.
		match &self.index {
			#[cfg(feature = "postgres")]
			Index::Postgres(index) => {
				self.indexer_task_handle_messages_postgres(
					index,
					put_cache_entry_messages,
					put_object_messages,
					touch_object_messages,
					put_process_messages,
					touch_process_messages,
					put_tag_messages,
					delete_tag_messages,
				)
				.await?;
			},
			Index::Sqlite(index) => {
				self.indexer_task_handle_messages_sqlite(
					index,
					put_cache_entry_messages,
					put_object_messages,
					touch_object_messages,
					put_process_messages,
					touch_process_messages,
					put_tag_messages,
					delete_tag_messages,
				)
				.await?;
			},
		}

		// Acknowledge the messages.
		future::try_join_all(ackers.drain(..).map(async |acker| {
			acker
				.ack()
				.await
				.map_err(|source| tg::error!(!source, "failed to acknowledge the message"))?;
			Ok::<_, tg::Error>(())
		}))
		.await?;

		Ok(())
	}

	async fn indexer_handle_queue(&self, config: &crate::config::Indexer) -> tg::Result<usize> {
		match &self.index {
			#[cfg(feature = "postgres")]
			Index::Postgres(database) => self.indexer_handle_queue_postgres(config, database).await,
			Index::Sqlite(database) => self.indexer_handle_queue_sqlite(config, database).await,
		}
	}

	async fn indexer_get_transaction_id(&self) -> tg::Result<u64> {
		match &self.index {
			#[cfg(feature = "postgres")]
			Index::Postgres(database) => self.indexer_get_transaction_id_postgres(database).await,
			Index::Sqlite(database) => self.indexer_get_transaction_id_sqlite(database).await,
		}
	}

	async fn indexer_get_queue_size(&self, transaction_id: u64) -> tg::Result<u64> {
		match &self.index {
			#[cfg(feature = "postgres")]
			Index::Postgres(database) => {
				self.indexer_get_queue_size_postgres(database, transaction_id)
					.await
			},
			Index::Sqlite(database) => {
				self.indexer_get_queue_size_sqlite(database, transaction_id)
					.await
			},
		}
	}

	pub(crate) async fn handle_index_request(
		handle: &ServerOrProxy,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the stream.
		let stream = handle.index().await?;

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move {
			stop.wait().await;
		};
		let stream = stream.take_until(stop);

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}

impl Index {
	#[expect(dead_code)]
	pub async fn sync(&self) -> tg::Result<()> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(database) => {
				database
					.sync()
					.await
					.map_err(|error| tg::error!(!error, "failed to sync the index"))?;
			},
			Self::Sqlite(database) => {
				database
					.sync()
					.await
					.map_err(|error| tg::error!(!error, "failed to sync the index"))?;
			},
		}
		Ok(())
	}
}
