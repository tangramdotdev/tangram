use crate::database::Database;
use crate::{Server, util::iter::Ext as _};
use async_nats as nats;
use futures::FutureExt as _;
use futures::stream::FuturesUnordered;
use futures::{Stream, StreamExt as _, TryStreamExt, future, stream};
use indoc::{formatdoc, indoc};
use num::ToPrimitive as _;
use std::sync::Arc;
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
pub(crate) enum Message {
	Object(ObjectMessage),
	Process(ProcessMessage),
	PutTag(PutTagMessage),
	DeleteTag(DeleteTagMessage),
	TouchObject(TouchObjectMessage),
	TouchProcess(TouchProcessMessage),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct ObjectMessage {
	pub(crate) children: BTreeSet<tg::object::Id>,
	pub(crate) id: tg::object::Id,
	pub(crate) size: u64,
	pub(crate) touched_at: i64,
	pub(crate) cache_reference: Option<tg::blob::Id>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct TouchObjectMessage {
	pub(crate) id: tg::object::Id,
	pub(crate) touched_at: i64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct ProcessMessage {
	pub(crate) children: Option<Vec<tg::process::Id>>,
	pub(crate) id: tg::process::Id,
	pub(crate) touched_at: i64,
	pub(crate) objects: Vec<(Kind, tg::object::Id)>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct TouchProcessMessage {
	pub(crate) id: tg::process::Id,
	pub(crate) touched_at: i64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) enum Kind {
	Command,
	Output,
}

impl std::fmt::Display for Kind {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Command => write!(formatter, "command"),
			Self::Output => write!(formatter, "output"),
		}
	}
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct PutTagMessage {
	pub(crate) tag: String,
	pub(crate) item: Either<tg::process::Id, tg::object::Id>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct DeleteTagMessage {
	pub(crate) tag: String,
}

type Acker = nats::jetstream::message::Acker;

pub async fn migrate(database: &Database) -> tg::Result<()> {
	if database.is_right() {
		return Ok(());
	}

	let migrations = vec![migration_0000(database).boxed()];

	let version = match database {
		Either::Left(database) => {
			let connection = database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
			connection
				.with(|connection| {
					connection
						.pragma_query_value(None, "user_version", |row| {
							Ok(row.get_unwrap::<_, usize>(0))
						})
						.map_err(|source| tg::error!(!source, "failed to get the version"))
				})
				.await?
		},
		Either::Right(_) => {
			unreachable!()
		},
	};

	// If this path is from a newer version of Tangram, then return an error.
	if version > migrations.len() {
		return Err(tg::error!(
			r"The index has run migrations from a newer version of Tangram. Please run `tg self update` to update to the latest version of Tangram."
		));
	}

	// Run all migrations and update the version file.
	let migrations = migrations.into_iter().enumerate().skip(version);
	for (version, migration) in migrations {
		// Run the migration.
		migration.await?;

		// Update the version.
		match database {
			Either::Left(database) => {
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
			},
			Either::Right(_) => {
				unreachable!()
			},
		}
	}

	Ok(())
}

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
			let result = self.indexer_task_handle_messages(config, messages).await;
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

	async fn indexer_task_handle_messages(
		&self,
		config: &crate::config::Indexer,
		messages: Vec<(Message, Option<Acker>)>,
	) -> tg::Result<()> {
		if messages.is_empty() {
			return Ok(());
		}
		let batches = messages.into_iter().batches(config.insert_batch_size);
		for messages in batches {
			let mut object_messages = Vec::new();
			let mut process_messages = Vec::new();
			let mut put_tag_messages = Vec::new();
			let mut delete_tag_messages = Vec::new();
			let mut touch_object_messages = Vec::new();
			let mut touch_process_messages = Vec::new();
			messages
				.into_iter()
				.for_each(|(message, acker)| match message {
					Message::Object(object_message) => {
						object_messages.push((object_message, acker))
					},
					Message::Process(process_message) => {
						process_messages.push((process_message, acker))
					},
					Message::PutTag(put_tag_message) => {
						put_tag_messages.push((put_tag_message, acker))
					},
					Message::DeleteTag(delete_tag_message) => {
						delete_tag_messages.push((delete_tag_message, acker))
					},
					Message::TouchObject(touch_object_message) => {
						touch_object_messages.push((touch_object_message, acker))
					},
					Message::TouchProcess(touch_process_message) => {
						touch_process_messages.push((touch_process_message, acker))
					},
				});
			match &self.index {
				Either::Left(index) => {
					self.indexer_insert_objects_sqlite(object_messages, index)
						.await?;
					self.indexer_insert_processes_sqlite(process_messages, index)
						.await?;
					self.indexer_insert_tags_sqlite(put_tag_messages, index)
						.await?;
					self.indexer_delete_tags_sqlite(delete_tag_messages, index)
						.await?;
					self.indexer_touch_object_sqlite(touch_object_messages, index)
						.await?;
					self.indexer_touch_process_sqlite(touch_process_messages, index)
						.await?;
				},
				Either::Right(index) => {
					self.indexer_insert_objects_postgres(object_messages, index)
						.await?;
					self.indexer_insert_processes_postgres(process_messages, index)
						.await?;
					self.indexer_insert_tags_postgres(put_tag_messages, index)
						.await?;
					self.indexer_delete_tags_postgres(delete_tag_messages, index)
						.await?;
					self.indexer_touch_object_postgres(touch_object_messages, index)
						.await?;
					self.indexer_touch_process_postgres(touch_process_messages, index)
						.await?;
				},
			}
		}
		Ok(())
	}

	async fn indexer_insert_objects_sqlite(
		&self,
		messages: Vec<(ObjectMessage, Option<Acker>)>,
		index: &db::sqlite::Database,
	) -> tg::Result<()> {
		// Split the messages and ackers.
		let (messages, ackers) = messages.into_iter().collect::<(Vec<_>, Vec<_>)>();

		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = index
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

					let cache_entries_statement = indoc!(
						"
							insert into cache_entries (id, reference_count)
							values (?1, 0)
							on conflict (id) do nothing;
						"
					);
					let mut cache_entries_statement = transaction
						.prepare_cached(cache_entries_statement)
						.map_err(|source| {
							tg::error!(!source, "failed to prepare the blobs statement")
						})?;

					let cache_references_statement = indoc!(
						"
							insert into cache_references (id, file)
							values (?1, ?2)
							on conflict (id) do nothing;
						"
					);
					let mut cache_references_statement = transaction
						.prepare_cached(cache_references_statement)
						.map_err(|source| {
							tg::error!(!source, "failed to prepare the references statement")
						})?;

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

						// Insert the cache references.
						if let Some(reference) = message.cache_reference {
							// Insert the cache entries references.
							let params = rusqlite::params![&id.to_string()];
							cache_entries_statement.execute(params).map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;

							let params = rusqlite::params![&id.to_string(), &reference.to_string()];
							cache_references_statement
								.execute(params)
								.map_err(|source| {
									tg::error!(!source, "failed to execute the statement")
								})?;
						}
					}

					// Drop the statements.
					drop(children_statement);
					drop(objects_statement);
					drop(cache_entries_statement);
					drop(cache_references_statement);

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

	async fn indexer_insert_processes_sqlite(
		&self,
		messages: Vec<(ProcessMessage, Option<Acker>)>,
		index: &db::sqlite::Database,
	) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = index
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let transaction = Arc::new(transaction);

		for (message, _) in &messages {
			// Insert the process.
			let statement = indoc!(
				"
				insert into processes (
					id,
					touched_at
				)
				values (
					?1,
					?2
				)
				on conflict (id) do update set
					touched_at = ?2;
			"
			);
			let params = db::params![
				&message.id,
				time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap(),
			];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Insert the children.
			if let Some(children) = &message.children {
				let statement = formatdoc!(
					"
						insert into process_children (process, position, child)
						values (?1, ?2, ?3);
					"
				);
				children
					.iter()
					.enumerate()
					.map(|(position, child)| {
						let transaction = transaction.clone();
						let statement = statement.clone();
						async move {
							let params = db::params![&message.id, position, child];
							transaction
								.execute(statement.into(), params)
								.await
								.map_err(|source| {
									tg::error!(!source, "failed to execute the statement")
								})?;
							Ok::<_, tg::Error>(())
						}
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect::<()>()
					.await?;
			}

			// Insert the objects.
			let statement = formatdoc!(
				"
					insert into process_objects (process, object, kind)
					values (?1, ?2, ?3)
					on conflict (process, object, kind) do nothing;
				"
			);
			message
				.objects
				.iter()
				.map(|(kind, object)| {
					let transaction = transaction.clone();
					let statement = statement.clone();
					async move {
						let params = db::params![&message.id, object, kind];
						transaction
							.execute(statement.into(), params)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
						Ok::<_, tg::Error>(())
					}
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<()>()
				.await?;
		}

		// Commit the transaction.
		Arc::into_inner(transaction)
			.unwrap()
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

		// Drop the connection.
		drop(connection);

		Ok(())
	}

	async fn indexer_insert_tags_sqlite(
		&self,
		messages: Vec<(PutTagMessage, Option<Acker>)>,
		index: &db::sqlite::Database,
	) -> tg::Result<()> {
		// Get a database connection.
		let connection = index
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		for (message, _) in &messages {
			// Insert the tag.
			let p = connection.p();
			let statement = formatdoc!(
				"
				insert into tags (tag, item)
				values ({p}1, {p}2)
				on conflict (tag) do update set item = {p}2;
			"
			);
			let params = db::params![message.tag, message.item];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Drop the database connection.
		drop(connection);

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

	async fn indexer_delete_tags_sqlite(
		&self,
		messages: Vec<(DeleteTagMessage, Option<Acker>)>,
		index: &db::sqlite::Database,
	) -> tg::Result<()> {
		// Get a database connection.
		let connection = index
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		for (message, _) in &messages {
			// Delete the tag.
			let p = connection.p();
			let statement = formatdoc!(
				"
					delete from tags
					where tag = {p}1;
				"
			);
			let params = db::params![message.tag];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Drop the connection.
		drop(connection);

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

	async fn indexer_touch_object_sqlite(
		&self,
		messages: Vec<(TouchObjectMessage, Option<Acker>)>,
		index: &db::sqlite::Database,
	) -> tg::Result<()> {
		let connection = index
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		for (message, _) in &messages {
			let p = connection.p();
			let statement = format!(
				"
				update objects
				set touched_at = {p}1
				where id = {p}2;
			"
			);
			let params = db::params![message.touched_at, message.id];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Drop the connection.
		drop(connection);

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

	async fn indexer_touch_process_sqlite(
		&self,
		messages: Vec<(TouchProcessMessage, Option<Acker>)>,
		index: &db::sqlite::Database,
	) -> tg::Result<()> {
		let connection = index
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		for (message, _) in &messages {
			let p = connection.p();
			let statement = format!(
				"
					update processes
					set touched_at = {p}1
					where id = {p}2;
				"
			);
			let params = db::params![message.touched_at, message.id];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Drop the connection.
		drop(connection);

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

	async fn indexer_insert_objects_postgres(
		&self,
		messages: Vec<(ObjectMessage, Option<Acker>)>,
		index: &db::postgres::Database,
	) -> tg::Result<()> {
		// Get a database connection.
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let mut connection = index
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the unique messages.
		let unique_messages: HashMap<&tg::object::Id, &ObjectMessage, fnv::FnvBuildHasher> =
			messages
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
		let cache_references = unique_messages
			.values()
			.filter_map(|message| message.cache_reference.as_ref())
			.map(ToString::to_string)
			.collect::<Vec<_>>();
		let statement = indoc!(
			"
				CALL insert_objects_and_children(
					$1::text[],
					$2::int8[],
					$3::text,
					$4::text[],
					$5::int8[],
					$6::text[]
				);
			"
		);
		transaction
			.execute(statement, &[
				&ids.as_slice(),
				&size.as_slice(),
				&now,
				&children.as_slice(),
				&parent_indices.as_slice(),
				&cache_references.as_slice(),
			])
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

	async fn indexer_insert_processes_postgres(
		&self,
		messages: Vec<(ProcessMessage, Option<Acker>)>,
		index: &db::postgres::Database,
	) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = index
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.client_mut()
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		for (message, _) in &messages {
			// Insert the process.
			let statement = indoc!(
				"
				insert into processes (
					id,
					touched_at
				)
				values (
					$1,
					$2
				)
				on conflict (id) do update set
					touched_at = $2;
			"
			);
			transaction
				.execute(statement, &[
					&message.id.to_string(),
					&time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap(),
				])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Insert the children.
			if let Some(children) = &message.children {
				let positions: Vec<i64> = (0..children.len().to_i64().unwrap()).collect();
				let statement = indoc!(
					"
						insert into process_children (process, position, child)
						select $1, unnest($2::int8[]), unnest($3::text[]);
					"
				);
				transaction
					.execute(statement, &[
						&message.id.to_string(),
						&positions.as_slice(),
						&children.iter().map(ToString::to_string).collect::<Vec<_>>(),
					])
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Insert the objects.
			let objects: Vec<&tg::object::Id> = message.objects.iter().map(|(_, id)| id).collect();
			let kinds: Vec<&Kind> = message.objects.iter().map(|(kind, _)| kind).collect();
			if !message.objects.is_empty() {
				let statement = indoc!(
					"
						insert into process_objects (process, object)
						select $1, unnest($2::text[]), unnest($3::text[])
						on conflict (process, object, kind) do nothing;
					"
				);

				transaction
					.execute(statement, &[
						&message.id.to_string(),
						&objects.iter().map(ToString::to_string).collect::<Vec<_>>(),
						&kinds.iter().map(ToString::to_string).collect::<Vec<_>>(),
					])
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Drop the connection.
		drop(connection);

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

	async fn indexer_insert_tags_postgres(
		&self,
		messages: Vec<(PutTagMessage, Option<Acker>)>,
		index: &db::postgres::Database,
	) -> tg::Result<()> {
		// Get a database connection.
		let connection = index
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		for (message, _) in &messages {
			let statement = indoc!(
				"
					insert into tags (tag, item)
					values ($1, $2)
					on conflict (tag) do update set item = $2;
				"
			);
			let params = db::params![message.tag, message.item];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Drop the connection.
		drop(connection);

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

	async fn indexer_delete_tags_postgres(
		&self,
		messages: Vec<(DeleteTagMessage, Option<Acker>)>,
		index: &db::postgres::Database,
	) -> tg::Result<()> {
		// Get a database connection.
		let connection = index
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		for (message, _) in &messages {
			// Delete the tag.
			let p = connection.p();
			let statement = formatdoc!(
				"
					delete from tags
					where tag = {p}1;
				"
			);
			let params = db::params![message.tag];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Drop the connection.
		drop(connection);

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

	async fn indexer_touch_object_postgres(
		&self,
		messages: Vec<(TouchObjectMessage, Option<Acker>)>,
		index: &db::postgres::Database,
	) -> tg::Result<()> {
		let connection = index
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		for (message, _) in &messages {
			let p = connection.p();
			let statement = format!(
				"
				update objects
				set touched_at = {p}1
				where id = {p}2;
			"
			);
			let params = db::params![message.touched_at, message.id];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Drop the connection.
		drop(connection);

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

	async fn indexer_touch_process_postgres(
		&self,
		messages: Vec<(TouchProcessMessage, Option<Acker>)>,
		index: &db::postgres::Database,
	) -> tg::Result<()> {
		let connection = index
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		for (message, _) in &messages {
			let p = connection.p();
			let statement = format!(
				"
				update processes
				set touched_at = {p}1
				where id = {p}2;
			"
			);
			let params = db::params![message.touched_at, message.id];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Drop the connection.
		drop(connection);

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

async fn migration_0000(database: &Database) -> tg::Result<()> {
	let sql = indoc!(
		r#"
			create table cache_entries (
				id text primary key,
				reference_count integer
			);

			create index cache_entries_reference_count_zero_index on cache_entries ((1)) where reference_count = 0;

			create table cache_references (
				id text primary key,
				file text not null
			);

			create trigger cache_entries_increment_reference_count_trigger
			after insert on cache_references
			for each row
			begin
				update cache_entries set reference_count = reference_count + 1
				where id = new.file;
			end;

			create trigger cache_entries_decrement_reference_count_trigger
			after delete on cache_references
			for each row
			begin
				update cache_entries set reference_count = reference_count - 1
				where id = old.file;
			end;

			create table objects (
				id text primary key,
				complete integer not null default 0,
				count integer,
				depth integer,
				incomplete_children integer,
				reference_count integer,
				size integer not null,
				touched_at text,
				weight integer
			);

			create index objects_reference_count_zero_index on objects (touched_at) where reference_count = 0;

			create trigger objects_insert_complete_trigger
			after insert on objects
			when new.complete = 0 and new.incomplete_children = 0
			begin
				update objects
				set
					complete = updates.complete,
					count = updates.count,
					depth = updates.depth,
					weight = updates.weight
				from (
					select
						objects.id,
						coalesce(min(child_objects.complete), 1) as complete,
						1 + coalesce(sum(child_objects.count), 0) as count,
						1 + coalesce(max(child_objects.depth), 0) as depth,
						objects.size + coalesce(sum(child_objects.weight), 0) as weight
					from objects
					left join object_children on object_children.object = objects.id
					left join objects as child_objects on child_objects.id = object_children.child
					where objects.id = new.id 
					group by objects.id, objects.size
				) as updates
				where objects.id = updates.id;
			end;

			create trigger objects_update_complete_trigger
			after update of complete, incomplete_children on objects
			when new.complete = 0 and new.incomplete_children = 0
			begin
				update objects
				set
					complete = updates.complete,
					count = updates.count,
					depth = updates.depth,
					weight = updates.weight
				from (
					select
						objects.id,
						coalesce(min(child_objects.complete), 1) as complete,
						1 + coalesce(sum(child_objects.count), 0) as count,
						1 + coalesce(max(child_objects.depth), 0) as depth,
						objects.size + coalesce(sum(child_objects.weight), 0) as weight
					from objects
					left join object_children on object_children.object = objects.id
					left join objects as child_objects on child_objects.id = object_children.child
					where objects.id = new.id 
					group by objects.id, objects.size
				) as updates
				where objects.id = updates.id;
			end;

			create trigger objects_insert_incomplete_children_trigger
			after insert on objects
			for each row
			when (new.incomplete_children is null)
			begin
				update objects
				set incomplete_children = (
					select count(*)
					from object_children
					left join objects child_objects on child_objects.id = object_children.child
					where object_children.object = new.id and (child_objects.complete is null or child_objects.complete = 0)
				)
				where id = new.id;
			end;

			create trigger objects_insert_reference_count_trigger
			after insert on objects
			for each row
			when (new.reference_count is null)
			begin
				update objects
				set reference_count = (
					(select count(*) from object_children where child = new.id) +
					(select count(*) from process_objects where object = new.id) +
					(select count(*) from tags where item = new.id)
				)
				where id = new.id;
			end;

			create trigger objects_update_incomplete_children_trigger
			after update of complete on objects
			for each row
			when (old.complete = 0 and new.complete = 1)
			begin
				update objects
				set incomplete_children = incomplete_children - 1
				where id in (
					select object
					from object_children
					where child = new.id
				);
			end;

			create trigger objects_delete_trigger
			after delete on objects
			for each row
			begin
				delete from object_children
				where object = old.id;

				delete from cache_references
				where id = old.id;
			end;

			create table object_children (
				object text not null,
				child text not null
			);

			create unique index object_children_index on object_children (object, child);

			create index object_children_child_index on object_children (child);

			create trigger object_children_insert_trigger
			after insert on object_children
			for each row
			begin
				update objects
				set reference_count = objects.reference_count + 1
				where id = new.child;
			end;

			create trigger object_children_delete_trigger
			after delete on object_children
			for each row
			begin
				update objects
				set reference_count = objects.reference_count - 1
				where id = old.child;
			end;

			create table processes (
				id text primary key,
				count integer,
				commands_complete integer not null default 0,
				commands_count integer,
				commands_depth integer,
				commands_weight integer,
				complete integer not null default 0,
				outputs_complete integer not null default 0,
				outputs_count integer,
				outputs_depth integer,
				outputs_weight integer,
				reference_count integer,
				touched_at text
			);

			create index processes_reference_count_zero_index on processes (touched_at) where reference_count = 0;

			create trigger processes_insert_reference_count_trigger
			after insert on processes
			for each row
			when (new.reference_count is null)
			begin
				update processes
				set reference_count = (
					(select count(*) from process_children where child = new.id) +
					(select count(*) from tags where item = new.id)
				)
				where id = new.id;
			end;

			create trigger processes_delete_trigger
			after delete on processes
			for each row
			begin
				delete from process_children
				where process = old.id;

				delete from process_objects
				where process = old.id;
			end;

			create table process_children (
				process text not null,
				child text not null,
				position integer not null
			);

			create unique index process_children_process_child_index on process_children (process, child);

			create index process_children_index on process_children (process, position);

			create index process_children_child_process_index on process_children (child, process);

			create trigger process_children_insert_trigger
			after insert on process_children
			for each row
			begin
				update processes
				set reference_count = processes.reference_count + 1
				where id = new.child;
			end;

			create trigger process_children_delete_trigger
			after delete on process_children
			for each row
			begin
				update processes
				set reference_count = processes.reference_count - 1
				where id = old.child;
			end;

			create table process_objects (
				process text not null,
				object text not null,
				kind text not null
			);

			create unique index process_objects_index on process_objects (process, object);

			create index process_objects_object_index on process_objects (object);

			create trigger process_objects_insert_trigger
			after insert on process_objects
			begin
				update objects
				set reference_count = reference_count + 1
				where id = new.object;
			end;

			create trigger process_objects_delete_trigger
			after delete on process_objects
			begin
				update objects
				set reference_count = reference_count - 1
				where id = old.object;
			end;

			create table tags (
				tag text primary key,
				item text not null
			);

			create trigger tags_insert_trigger
			after insert on tags
			for each row
			begin
				update objects set reference_count = reference_count + 1
				where id = new.item;

				update processes set reference_count = reference_count + 1
				where id = new.item;
			end;

			create trigger tags_delete_trigger
			after delete on tags
			for each row
			begin
				update objects set reference_count = reference_count - 1
				where id = old.item;

				update processes set reference_count = reference_count - 1
				where id = old.item;
			end;
		"#
	);
	let database = database.as_ref().unwrap_left();
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
	connection
		.with(move |connection| {
			let sql =
				"insert into remotes (name, url) values ('default', 'https://cloud.tangram.dev');";
			connection
				.execute_batch(sql)
				.map_err(|source| tg::error!(!source, "failed to execute the statements"))?;
			Ok::<_, tg::Error>(())
		})
		.await?;
	Ok(())
}
