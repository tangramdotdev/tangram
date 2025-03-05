use crate::Server;
use async_nats as nats;
use futures::{Stream, StreamExt as _, TryStreamExt, future, stream};
use indoc::{formatdoc, indoc};
use num::ToPrimitive as _;
use std::{collections::BTreeSet, pin::pin, time::Duration};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Message {
	pub(crate) id: tg::object::Id,
	pub(crate) size: u64,
	pub(crate) children: BTreeSet<tg::object::Id>,
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
			// Update complete objects until there are no more updates.
			loop {
				let updated = self.indexer_task_update_complete_objects(config).await?;
				if updated == 0 {
					break;
				}
			}

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
			let result = self.indexer_task_insert_objects(messages).await;
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
		messages: Vec<(Message, Option<Acker>)>,
	) -> tg::Result<()> {
		if messages.is_empty() {
			return Ok(());
		}
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

					// Get the current time.
					let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();

					// Execute inserts for each object in the batch.
					for message in messages {
						let id = message.id;
						let size = message.size;
						let children = message.children;

						// Insert the children.
						for child in children {
							let child = child.to_string();
							let params = rusqlite::params![&id.to_string(), &child];
							children_statement.execute(params).map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
						}

						// Insert the object.
						let params = rusqlite::params![&id.to_string(), size, now];
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

		for messages in messages.chunks(128) {
			// Begin a transaction.
			let transaction = connection
				.client_mut()
				.transaction()
				.await
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

			// Insert into the object children and objects tables.
			let statement = indoc!(
				"
					with inserted_object_children as (
						insert into object_children (object, child)
						select ($1::text[])[object_index], child
						from unnest($3::int8[], $2::text[]) as c (object_index, child)
						on conflict (object, child) do nothing
					),
					inserted_objects as (
						insert into objects (id, size, touched_at)
						select id, size, $5
						from unnest($1::text[], $4::int8[]) as t (id, size)
						on conflict (id) do update set touched_at = $5
					)
					select 1;
				"
			);
			let ids = messages
				.iter()
				.map(|(message, _)| message.id.to_string())
				.collect::<Vec<_>>();
			let children = messages
				.iter()
				.flat_map(|(message, _)| message.children.iter().map(ToString::to_string))
				.collect::<Vec<_>>();
			let parent_indices = messages
				.iter()
				.map(|(message, _)| message)
				.enumerate()
				.flat_map(|(index, message)| {
					std::iter::repeat_n((index + 1).to_i64().unwrap(), message.children.len())
				})
				.collect::<Vec<_>>();
			let size = messages
				.iter()
				.map(|(message, _)| message.size.to_i64().unwrap())
				.collect::<Vec<_>>();
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
			transaction
				.execute(
					statement,
					&[
						&ids.as_slice(),
						&children.as_slice(),
						&parent_indices.as_slice(),
						&size.as_slice(),
						&now,
					],
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Set reference counts and incomplete children.
			let statement = indoc!(
				"
					update objects
					set incomplete_children = (
						select count(*)
						from object_children
						left join objects child_objects on child_objects.id = object_children.child
						where object_children.object = t.id and (child_objects.complete is null or child_objects.complete = 0)
					),
					reference_count = (
						(select count(*) from object_children where child = t.id) +
						(select count(*) from process_objects where object = t.id) +
						(select count(*) from tags where item = t.id)
					)
					from unnest($1::text[]) as t (id)
					where objects.id = t.id;
				"
			);
			transaction
				.execute(statement, &[&ids.as_slice()])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

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
		}

		Ok(())
	}

	async fn indexer_task_update_complete_objects(
		&self,
		config: &crate::config::Indexer,
	) -> tg::Result<u64> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = self
			.database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let locked = match connection {
			Either::Left(_) => "",
			Either::Right(_) => "for update skip locked",
		};
		let statement = formatdoc!(
			"
				with update_objects as (
					select id, size
					from objects
					where objects.complete = 0 and objects.incomplete_children = 0
					{locked}
				),
				updates as (
					select
						update_objects.id as id,
						coalesce(min(child_objects.complete), 1) as complete,
						1 + coalesce(sum(child_objects.count), 0) as count,
						1 + coalesce(max(child_objects.depth), 0) as depth,
						update_objects.size + coalesce(sum(child_objects.weight), 0) as weight
					from update_objects
					left join object_children on object_children.object = update_objects.id
					left join objects child_objects on child_objects.id = object_children.child
					group by update_objects.id, update_objects.size
					limit {p}1
				)
				update objects
				set
					complete = updates.complete,
					count = updates.count,
					depth = updates.depth,
					weight = updates.weight
				from updates
				where objects.id = updates.id;
			"
		);
		let batch_size = config.update_complete_batch_size;
		let params = db::params![batch_size];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(n)
	}
}
