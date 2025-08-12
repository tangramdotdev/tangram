use crate::Server;
use futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future};
use num::ToPrimitive as _;
use std::{pin::pin, time::Duration};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _};
use tangram_messenger::{self as messenger, Acker, prelude::*};
use tokio_util::task::AbortOnDropHandle;

#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

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

				// Get the info.
				let info = stream
					.info()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the index stream info"))?;

				// Start the progress indicator.
				let total = info.last_sequence.saturating_sub(info.first_sequence);
				progress.start(
					"index".to_string(),
					"items".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(0),
					Some(total),
				);

				// Wait for the indexing task to catch up.
				let mut first_sequence = info.first_sequence;
				let last_sequence = info.last_sequence;
				while first_sequence <= last_sequence {
					let info = stream.info().await.map_err(|source| {
						tg::error!(!source, "failed to get the index stream info")
					})?;
					progress.increment("index", info.first_sequence - first_sequence);
					first_sequence = info.first_sequence;
					tokio::time::sleep(Duration::from_millis(10)).await;
				}

				progress.finish_all();
				progress.output(());

				Ok::<_, tg::Error>(())
			}
		}));
		let stream = progress.stream().attach(task);
		Ok(stream)
	}

	pub(crate) async fn indexer_task(&self, config: &crate::config::Indexer) -> tg::Result<()> {
		// Get the messages stream.
		let stream = self.indexer_task_create_message_stream(config).await?;
		let mut stream = pin!(stream);

		loop {
			// Get a batch of messages.
			let result = stream.try_next().await;
			let messages = match result {
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
			// Insert objects from the messages.
			let result = self.indexer_task_handle_messages(config, messages).await;
			if let Err(error) = result {
				tracing::error!(?error, "failed to handle the messages");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn indexer_task_create_message_stream(
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

	async fn indexer_task_handle_messages(
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

	pub(crate) async fn handle_index_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
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

pub async fn migrate(database: &db::sqlite::Database) -> tg::Result<()> {
	let migrations = vec![migration_0000(database).boxed()];

	let connection = database
		.connection()
		.await
		.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
	let version =
		connection
			.with(|connection| {
				connection
					.pragma_query_value(None, "user_version", |row| {
						Ok(row.get_unwrap::<_, usize>(0))
					})
					.map_err(|source| tg::error!(!source, "failed to get the version"))
			})
			.await?;
	drop(connection);

	// If this path is from a newer version of Tangram, then return an error.
	if version > migrations.len() {
		return Err(tg::error!(
			r"The index has run migrations from a newer version of Tangram. Please run `tg self update` to update to the latest version of Tangram."
		));
	}

	// Run all migrations and update the version.
	let migrations = migrations.into_iter().enumerate().skip(version);
	for (version, migration) in migrations {
		// Run the migration.
		migration.await?;

		// Update the version.
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		connection
			.with(move |connection| {
				connection
					.pragma_update(None, "user_version", version + 1)
					.map_err(|source| tg::error!(!source, "failed to get the version"))
			})
			.await?;
	}

	Ok(())
}

async fn migration_0000(database: &db::sqlite::Database) -> tg::Result<()> {
	let sql = include_str!("index/schema.sql");
	let connection = database
		.write_connection()
		.await
		.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
	connection
		.with(move |connection| {
			connection
				.execute_batch(sql)
				.map_err(|source| tg::error!(!source, "failed to execute the statements"))?;
			Ok::<_, tg::Error>(())
		})
		.await?;
	Ok(())
}
