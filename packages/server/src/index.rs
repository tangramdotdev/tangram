use crate::{Server, util::iter::Ext as _};
use async_nats as nats;
use futures::{Stream, StreamExt as _, TryStreamExt, future, stream};
use indoc::indoc;
use num::ToPrimitive as _;
use std::{
	collections::{BTreeSet, HashMap},
	pin::pin,
	time::Duration,
};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Message {
	pub(crate) children: BTreeSet<tg::object::Id>,
	pub(crate) count: Option<u64>,
	pub(crate) depth: Option<u64>,
	pub(crate) id: tg::object::Id,
	pub(crate) size: u64,
	pub(crate) touched_at: i64,
	pub(crate) weight: Option<u64>,
}

type Acker = nats::jetstream::message::Acker;

impl Server {
	pub async fn index(&self) -> tg::Result<()> {
		Err(tg::error!("unimplemented"))
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
				Ok(None) => panic!("the stream ended"),
				Err(error) => {
					tracing::error!(?error, "failed to get a batch of messages");
					tokio::time::sleep(Duration::from_secs(1)).await;
					continue;
				},
			};

			// Insert objects from the messages.
			let result = self.indexer_task_insert_objects(config, messages).await;
			if let Err(error) = result {
				tracing::error!(?error, "failed to get a batch of messages");
				tokio::time::sleep(Duration::from_secs(1)).await;
				continue;
			}
		}
	}

	async fn indexer_task_create_message_stream(
		&self,
		config: &crate::config::Indexer,
	) -> tg::Result<impl Stream<Item = tg::Result<Vec<(Message, Option<Acker>)>>>> {
		match &self.messenger {
			Either::Left(messenger) => self
				.indexer_task_create_message_stream_memory(config, messenger)
				.await
				.map(futures::StreamExt::left_stream),
			Either::Right(messenger) => self
				.indexer_task_create_message_stream_nats(config, messenger)
				.await
				.map(futures::StreamExt::right_stream),
		}
	}

	async fn indexer_task_create_message_stream_memory(
		&self,
		config: &crate::config::Indexer,
		messenger: &tangram_messenger::memory::Messenger,
	) -> tg::Result<impl Stream<Item = tg::Result<Vec<(Message, Option<Acker>)>>>> {
		messenger
			.streams()
			.create_stream("index".into())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the subject"))?;
		let stream = messenger
			.subscribe("index".to_string(), None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the objects stream"))?
			.map(|message| {
				let message =
					serde_json::from_slice::<Message>(&message.payload).map_err(|source| {
						tg::error!(!source, "failed to deserialize the message payload")
					})?;
				Ok::<_, tg::Error>((message, None))
			})
			.filter_map(|result| future::ready(result.ok()))
			.ready_chunks(config.message_batch_size)
			.map(Ok);
		Ok(stream)
	}

	async fn indexer_task_create_message_stream_nats(
		&self,
		config: &crate::config::Indexer,
		messenger: &tangram_messenger::nats::Messenger,
	) -> tg::Result<impl Stream<Item = tg::Result<Vec<(Message, Option<Acker>)>>>> {
		// Get the stream.
		let stream_config = async_nats::jetstream::stream::Config {
			name: "index".to_string(),
			max_messages: i64::MAX,
			..Default::default()
		};
		let stream = messenger
			.jetstream
			.get_or_create_stream(stream_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the index stream"))?;

		// Get the consumer.
		let consumer_config = async_nats::jetstream::consumer::pull::Config {
			durable_name: Some("index".to_string()),
			..Default::default()
		};
		let consumer = stream
			.get_or_create_consumer("index", consumer_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the index consumer"))?;

		// Create the stream.
		let stream = stream::try_unfold(consumer, |consumer| async {
			let mut batch = consumer
				.batch()
				.max_messages(config.message_batch_size)
				.expires(config.message_batch_timeout)
				.messages()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the batch"))?;
			let mut messages = Vec::new();
			while let Some(message) = batch.try_next().await? {
				let (message, acker) = message.split();
				let result = serde_json::from_slice::<Message>(&message.payload);
				let message = match result {
					Ok(message) => message,
					Err(source) => {
						tracing::error!(?source, "failed to deserialize the message payload");
						acker.ack().await?;
						continue;
					},
				};
				messages.push((message, Some(acker)));
			}
			Ok(Some((messages, consumer)))
		})
		.try_filter(|messages| future::ready(!messages.is_empty()));

		Ok(stream)
	}

	async fn indexer_task_insert_objects(
		&self,
		config: &crate::config::Indexer,
		messages: Vec<(Message, Option<Acker>)>,
	) -> tg::Result<()> {
		if messages.is_empty() {
			return Ok(());
		}
		for messages in messages.into_iter().batches(config.insert_batch_size) {
			match &self.database {
				Either::Left(database) => {
					self.indexer_insert_objects_sqlite(messages, database)
						.await?;
				},
				Either::Right(database) => {
					self.indexer_insert_objects_postgres(messages, database)
						.await?;
				},
			}
		}
		Ok(())
	}

	async fn indexer_insert_objects_sqlite(
		&self,
		messages: Vec<(Message, Option<Acker>)>,
		database: &db::sqlite::Database,
	) -> tg::Result<()> {
		// Split the messages and ackers.
		let (messages, ackers) = messages.into_iter().collect::<(Vec<_>, Vec<_>)>();

		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		connection
			.with({
				move |connection| {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// Prepare a statement for the object children
					let children_statement = indoc!(
						"
							insert into object_children (object, child)
							values (?1, ?2)
							on conflict (object, child) do nothing;
						"
					);
					let mut children_statement = transaction
						.prepare_cached(children_statement)
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

					// Prepare a statement for the objects.
					let objects_statement = indoc!(
						"
							insert into objects (id, size, touched_at)
							values (?1, ?2, ?3)
							on conflict (id) do update set touched_at = ?3;
						"
					);
					let mut objects_statement = transaction
						.prepare_cached(objects_statement)
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

					// Execute inserts for each object in the batch.
					for message in messages {
						let id = message.id;
						let size = message.size;
						let children = message.children;
						let touched_at =
							time::OffsetDateTime::from_unix_timestamp(message.touched_at)
								.unwrap()
								.format(&Rfc3339)
								.unwrap();

						// Insert the children.
						for child in children {
							let child = child.to_string();
							let params = rusqlite::params![&id.to_string(), &child];
							children_statement.execute(params).map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
						}

						// Insert the object.
						let params = rusqlite::params![&id.to_string(), size, touched_at];
						objects_statement.execute(params).map_err(|source| {
							tg::error!(!source, "failed to execute the statement")
						})?;
					}

					// Drop the statements.
					drop(children_statement);
					drop(objects_statement);

					// Commit the transaction.
					transaction.commit().map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;

					Ok::<_, tg::Error>(())
				}
			})
			.await?;

		// Acknowledge the messages.
		future::try_join_all(ackers.into_iter().map(async |acker| {
			if let Some(acker) = acker {
				acker
					.ack()
					.await
					.map_err(|source| tg::error!(!source, "failed to acknowledge the message"))?;
			}
			Ok::<_, tg::Error>(())
		}))
		.await?;

		Ok(())
	}

	async fn indexer_insert_objects_postgres(
		&self,
		messages: Vec<(Message, Option<Acker>)>,
		database: &db::postgres::Database,
	) -> tg::Result<()> {
		// Get a database connection.
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let mut connection = database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the unique messages.
		let unique_messages: HashMap<&tg::object::Id, &Message, fnv::FnvBuildHasher> = messages
			.iter()
			.map(|(message, _)| (&message.id, message))
			.collect();

		// Begin a transaction.
		let transaction = connection
			.client_mut()
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Insert into the objects and object_children tables.
		let ids = unique_messages
			.values()
			.map(|message| message.id.to_string())
			.collect::<Vec<_>>();
		let size = unique_messages
			.values()
			.map(|message| message.size.to_i64().unwrap())
			.collect::<Vec<_>>();
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let children = unique_messages
			.values()
			.flat_map(|message| message.children.iter().map(ToString::to_string))
			.collect::<Vec<_>>();
		let parent_indices = unique_messages
			.values()
			.enumerate()
			.flat_map(|(index, message)| {
				std::iter::repeat_n((index + 1).to_i64().unwrap(), message.children.len())
			})
			.collect::<Vec<_>>();
		let count = unique_messages
			.values()
			.map(|message| message.count.map(|c| c.to_i64().unwrap()))
			.collect::<Vec<_>>();
		let depth = unique_messages
			.values()
			.map(|message| message.depth.map(|d| d.to_i64().unwrap()))
			.collect::<Vec<_>>();
		let weight = unique_messages
			.values()
			.map(|message| message.weight.map(|w| w.to_i64().unwrap()))
			.collect::<Vec<_>>();
		let statement = indoc!(
			"
				CALL insert_objects_and_children(
					$1::text[],
					$2::int8[],
					$3::text,
					$4::text[],
					$5::int8[],
					$7::int8[],
					$8::int8[],
					$9::int8[]
				);
			"
		);
		transaction
			.execute(
				statement,
				&[
					&ids.as_slice(),
					&size.as_slice(),
					&now,
					&children.as_slice(),
					&parent_indices.as_slice(),
					&count.as_slice(),
					&depth.as_slice(),
					&weight.as_slice(),
				],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the procedure"))?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Acknowledge the messages.
		future::try_join_all(
			messages
				.iter()
				.filter_map(|(_, acker)| acker.as_ref())
				.map(async |acker| acker.ack().await),
		)
		.await?;

		Ok(())
	}
}
