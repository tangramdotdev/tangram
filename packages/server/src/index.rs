use crate::{Server, database::Database, util::iter::Ext as _};
use futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream};
use indoc::indoc;
use num::ToPrimitive as _;
use rusqlite as sqlite;
use std::{
	collections::{BTreeSet, HashMap},
	pin::pin,
	time::Duration,
};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{Body, response::builder::Ext as _};
use tangram_messenger::{Acker, Messenger};
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Message {
	PutCacheEntry(PutCacheEntryMessage),
	PutObject(PutObjectMessage),
	TouchObject(TouchObjectMessage),
	PutProcess(PutProcessMessage),
	TouchProcess(TouchProcessMessage),
	PutTag(PutTagMessage),
	DeleteTag(DeleteTagMessage),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PutCacheEntryMessage {
	pub id: tg::artifact::Id,
	pub touched_at: i64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PutObjectMessage {
	pub cache_reference: Option<tg::artifact::Id>,
	pub children: BTreeSet<tg::object::Id>,
	pub id: tg::object::Id,
	pub size: u64,
	pub touched_at: i64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct TouchObjectMessage {
	pub id: tg::object::Id,
	pub touched_at: i64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PutProcessMessage {
	pub children: Option<Vec<tg::process::Id>>,
	pub id: tg::process::Id,
	pub touched_at: i64,
	pub objects: Vec<(tg::object::Id, ProcessObjectKind)>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct TouchProcessMessage {
	pub id: tg::process::Id,
	pub touched_at: i64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PutTagMessage {
	pub tag: String,
	pub item: Either<tg::process::Id, tg::object::Id>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DeleteTagMessage {
	pub tag: String,
}

#[derive(Clone, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub enum ProcessObjectKind {
	Command,
	Output,
}

impl Server {
	pub async fn index(&self) -> tg::Result<()> {
		let mut info = self
			.messenger
			.stream_info("index".into())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the index stream info"))?;

		// Get the most recent sequence number.
		let last_sequence = info.last_sequence;

		// Wait for the indexing task to catch up.
		loop {
			match &info.first_sequence {
				Some(first_sequence) if first_sequence <= &last_sequence => {
					tokio::task::yield_now().await;
					info = self
						.messenger
						.stream_info("index".into())
						.await
						.map_err(|source| {
							tg::error!(!source, "failed to get the index stream info")
						})?;
					continue;
				},
				_ => break,
			}
		}
		Ok(())
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
				continue;
			}
		}
	}

	async fn indexer_task_create_message_stream(
		&self,
		config: &crate::config::Indexer,
	) -> tg::Result<impl Stream<Item = tg::Result<Vec<(Message, Acker)>>>> {
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
	) -> tg::Result<impl Stream<Item = tg::Result<Vec<(Message, Acker)>>>> {
		messenger
			.create_stream("index".to_owned())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the subject"))?;
		let stream = messenger
			.stream_subscribe("index".to_owned(), None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the index stream"))?
			.map_err(|source| tg::error!(!source, "failed to get a message from the stream"))
			.and_then(|message| {
				let (payload, acker) = message.split();
				future::ready({
					serde_json::from_slice::<Message>(&payload)
						.map_err(|error| {
							tracing::error!(?error, "failed to deserialize the message");
							tg::error!(!error, "failed to deserialize the message")
						})
						.map(|msg| (msg, acker))
				})
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
	) -> tg::Result<impl Stream<Item = tg::Result<Vec<(Message, Acker)>>>> {
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
				messages.push((message, acker.into()));
			}
			Ok(Some((messages, consumer)))
		})
		.try_filter(|messages| future::ready(!messages.is_empty()));

		Ok(stream)
	}

	async fn indexer_task_handle_messages(
		&self,
		config: &crate::config::Indexer,
		messages: Vec<(Message, Acker)>,
	) -> tg::Result<()> {
		if messages.is_empty() {
			return Ok(());
		}
		let batches = messages.into_iter().batches(config.insert_batch_size);
		for messages in batches {
			// Split the messages and ackers.
			let (messages, ackers) = messages.into_iter().collect::<(Vec<_>, Vec<_>)>();

			// Group the messages by variant.
			let mut put_cache_entry_messages = Vec::new();
			let mut put_object_messages = Vec::new();
			let mut touch_object_messages = Vec::new();
			let mut put_process_messages = Vec::new();
			let mut touch_process_messages = Vec::new();
			let mut put_tag_messages = Vec::new();
			let mut delete_tag_messages = Vec::new();
			for message in messages {
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

			// Handle the messages.
			match &self.index {
				Either::Left(index) => {
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
				Either::Right(index) => {
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
			}

			// Acknowledge the messages.
			future::try_join_all(ackers.into_iter().map(async |acker| {
				acker
					.ack()
					.await
					.map_err(|source| tg::error!(!source, "failed to acknowledge the message"))?;
				Ok::<_, tg::Error>(())
			}))
			.await?;
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn indexer_task_handle_messages_sqlite(
		&self,
		database: &db::sqlite::Database,
		put_cache_entry_messages: Vec<PutCacheEntryMessage>,
		put_object_messages: Vec<PutObjectMessage>,
		touch_object_messages: Vec<TouchObjectMessage>,
		put_process_messages: Vec<PutProcessMessage>,
		touch_process_messages: Vec<TouchProcessMessage>,
		put_tag_messages: Vec<PutTagMessage>,
		delete_tag_messages: Vec<DeleteTagMessage>,
	) -> tg::Result<()> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		connection
			.with(move |connection| {
				// Begin a transaction.
				let transaction = connection
					.transaction()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

				// Handle the messages.
				Self::indexer_put_cache_entries_sqlite(put_cache_entry_messages, &transaction)?;
				Self::indexer_put_objects_sqlite(put_object_messages, &transaction)?;
				Self::indexer_touch_objects_sqlite(touch_object_messages, &transaction)?;
				Self::indexer_put_processes_sqlite(put_process_messages, &transaction)?;
				Self::indexer_touch_processes_sqlite(touch_process_messages, &transaction)?;
				Self::indexer_put_tags_sqlite(put_tag_messages, &transaction)?;
				Self::indexer_delete_tags_sqlite(delete_tag_messages, &transaction)?;

				// Commit the transaction.
				transaction
					.commit()
					.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

				Ok::<_, tg::Error>(())
			})
			.await?;

		Ok(())
	}

	fn indexer_put_cache_entries_sqlite(
		messages: Vec<PutCacheEntryMessage>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				insert or replace into cache_entries (id, touched_at)
				values (?1, ?2);
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		for message in messages {
			let touched_at = time::OffsetDateTime::from_unix_timestamp(message.touched_at)
				.unwrap()
				.format(&Rfc3339)
				.unwrap();
			let params = sqlite::params![message.id.to_string(), touched_at];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	fn indexer_put_objects_sqlite(
		messages: Vec<PutObjectMessage>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
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
				insert into objects (id, cache_reference, size, touched_at)
				values (?1, ?2, ?3, ?4)
				on conflict (id) do update set touched_at = ?4;
			"
		);
		let mut objects_statement = transaction
			.prepare_cached(objects_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		for message in messages {
			// Execute inserts for each object in the batch.
			let id = message.id;
			let cache_reference = message.cache_reference.as_ref().map(ToString::to_string);
			let children = message.children;
			let size = message.size;
			let touched_at = time::OffsetDateTime::from_unix_timestamp(message.touched_at)
				.unwrap()
				.format(&Rfc3339)
				.unwrap();

			// Insert the children.
			for child in children {
				let child = child.to_string();
				let params = rusqlite::params![&id.to_string(), &child];
				children_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Insert the object.
			let params = rusqlite::params![&id.to_string(), cache_reference, size, touched_at];
			objects_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	fn indexer_touch_objects_sqlite(
		messages: Vec<TouchObjectMessage>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				update objects
				set touched_at = ?1
				where id = ?2;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		for message in messages {
			let touched_at = time::OffsetDateTime::from_unix_timestamp(message.touched_at)
				.unwrap()
				.format(&Rfc3339)
				.unwrap();
			let params = sqlite::params![touched_at, message.id.to_string()];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	fn indexer_put_processes_sqlite(
		messages: Vec<PutProcessMessage>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				insert into processes (id, touched_at)
				values (?1, ?2)
				on conflict (id) do update set touched_at = ?2;
			"
		);
		let mut process_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				insert into process_children (process, position, child)
				values (?1, ?2, ?3);
			"
		);
		let mut child_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				insert into process_objects (process, object, kind)
				values (?1, ?2, ?3)
				on conflict (process, object, kind) do nothing;
			"
		);
		let mut object_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		for message in messages {
			let touched_at = time::OffsetDateTime::from_unix_timestamp(message.touched_at)
				.unwrap()
				.format(&Rfc3339)
				.unwrap();

			// Insert the process.
			let params = sqlite::params![message.id.to_string(), touched_at];
			process_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Insert the children.
			if let Some(children) = message.children {
				for (position, child) in children.iter().enumerate() {
					let params =
						sqlite::params![message.id.to_string(), position, child.to_string()];
					child_statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				}
			}

			// Insert the objects.
			for (object, kind) in message.objects {
				let params =
					sqlite::params![message.id.to_string(), object.to_string(), kind.to_string()];
				object_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
		}

		Ok(())
	}

	fn indexer_touch_processes_sqlite(
		messages: Vec<TouchProcessMessage>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				update processes
				set touched_at = ?1
				where id = ?2;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		for message in messages {
			let touched_at = time::OffsetDateTime::from_unix_timestamp(message.touched_at)
				.unwrap()
				.format(&Rfc3339)
				.unwrap();
			let params = sqlite::params![touched_at, message.id.to_string()];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	fn indexer_put_tags_sqlite(
		messages: Vec<PutTagMessage>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				insert or replace into tags (tag, item)
				values (?1, ?2);
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		for message in messages {
			let params = sqlite::params![message.tag, message.item.to_string()];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	fn indexer_delete_tags_sqlite(
		messages: Vec<DeleteTagMessage>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				delete from tags
				where tag = ?1;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		for message in messages {
			let params = sqlite::params![message.tag];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn indexer_task_handle_messages_postgres(
		&self,
		database: &db::postgres::Database,
		put_cache_entry_messages: Vec<PutCacheEntryMessage>,
		put_object_messages: Vec<PutObjectMessage>,
		touch_object_messages: Vec<TouchObjectMessage>,
		put_process_messages: Vec<PutProcessMessage>,
		touch_process_messages: Vec<TouchProcessMessage>,
		put_tag_messages: Vec<PutTagMessage>,
		delete_tag_messages: Vec<DeleteTagMessage>,
	) -> tg::Result<()> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let mut connection = database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Handle the messages.
		self.indexer_put_cache_entries_postgres(put_cache_entry_messages, &transaction)
			.await?;
		self.indexer_put_objects_postgres(put_object_messages, &transaction)
			.await?;
		self.indexer_touch_objects_postgres(touch_object_messages, &transaction)
			.await?;
		self.indexer_put_processes_postgres(put_process_messages, &transaction)
			.await?;
		self.indexer_touch_processes_postgres(touch_process_messages, &transaction)
			.await?;
		self.indexer_put_tags_postgres(put_tag_messages, &transaction)
			.await?;
		self.indexer_delete_tags_postgres(delete_tag_messages, &transaction)
			.await?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(())
	}

	async fn indexer_put_cache_entries_postgres(
		&self,
		messages: Vec<PutCacheEntryMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			let statement = indoc!(
				"
					insert or replace into cache_entries (id, touched_at)
					values ($1, $2);
				"
			);
			let touched_at = time::OffsetDateTime::from_unix_timestamp(message.touched_at)
				.unwrap()
				.format(&Rfc3339)
				.unwrap();
			let params = db::params![message.id.to_string(), touched_at];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	async fn indexer_put_objects_postgres(
		&self,
		messages: Vec<PutObjectMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		// Get the unique messages.
		let unique_messages: HashMap<&tg::object::Id, &PutObjectMessage, fnv::FnvBuildHasher> =
			messages
				.iter()
				.map(|message| (&message.id, message))
				.collect();

		// Insert into the objects and object_children tables.
		let ids = unique_messages
			.values()
			.map(|message| message.id.to_string())
			.collect::<Vec<_>>();
		let size = unique_messages
			.values()
			.map(|message| message.size.to_i64().unwrap())
			.collect::<Vec<_>>();
		let touched_ats = unique_messages
			.values()
			.map(|message| {
				time::OffsetDateTime::from_unix_timestamp(message.touched_at)
					.unwrap()
					.format(&Rfc3339)
					.unwrap()
			})
			.collect::<Vec<_>>();
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
				call insert_objects_and_children(
					$1::text[],
					$2::text[],
					$3::int8[],
					$4::text[],
					$5::text[],
					$6::int8[]
				);
			"
		);
		transaction
			.inner()
			.execute(
				statement,
				&[
					&ids.as_slice(),
					&cache_references.as_slice(),
					&size.as_slice(),
					&touched_ats.as_slice(),
					&children.as_slice(),
					&parent_indices.as_slice(),
				],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the procedure"))?;

		Ok(())
	}

	async fn indexer_touch_objects_postgres(
		&self,
		messages: Vec<TouchObjectMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			let touched_at = time::OffsetDateTime::from_unix_timestamp(message.touched_at)
				.unwrap()
				.format(&Rfc3339)
				.unwrap();
			let statement = indoc!(
				"
					update objects
					set touched_at = $1
					where id = $2;
				"
			);
			let params = db::params![touched_at, message.id];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	async fn indexer_put_processes_postgres(
		&self,
		messages: Vec<PutProcessMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			let touched_at = time::OffsetDateTime::from_unix_timestamp(message.touched_at)
				.unwrap()
				.format(&Rfc3339)
				.unwrap();

			// Insert the process.
			let process_statement = indoc!(
				"
					insert into processes (id, touched_at)
					values ($1, $2)
					on conflict (id) do update set
						touched_at = $2;
				"
			);
			transaction
				.inner()
				.execute(process_statement, &[&message.id.to_string(), &touched_at])
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
					.inner()
					.execute(
						statement,
						&[
							&message.id.to_string(),
							&positions.as_slice(),
							&children.iter().map(ToString::to_string).collect::<Vec<_>>(),
						],
					)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Insert the objects.
			let objects: Vec<&tg::object::Id> = message.objects.iter().map(|(id, _)| id).collect();
			let kinds: Vec<&ProcessObjectKind> =
				message.objects.iter().map(|(_, kind)| kind).collect();
			if !message.objects.is_empty() {
				let statement = indoc!(
					"
						insert into process_objects (process, object)
						select $1, unnest($2::text[]), unnest($3::text[])
						on conflict (process, object, kind) do nothing;
					"
				);
				transaction
					.inner()
					.execute(
						statement,
						&[
							&message.id.to_string(),
							&objects.iter().map(ToString::to_string).collect::<Vec<_>>(),
							&kinds.iter().map(ToString::to_string).collect::<Vec<_>>(),
						],
					)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
		}

		Ok(())
	}

	async fn indexer_touch_processes_postgres(
		&self,
		messages: Vec<TouchProcessMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			let touched_at = time::OffsetDateTime::from_unix_timestamp(message.touched_at)
				.unwrap()
				.format(&Rfc3339)
				.unwrap();
			let statement = indoc!(
				"
					update processes
					set touched_at = ?1
					where id = ?2;
				"
			);
			let params = db::params![touched_at, message.id];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	async fn indexer_put_tags_postgres(
		&self,
		messages: Vec<PutTagMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			let statement = indoc!(
				"
					insert or replace into tags (tag, item)
					values ($1, $2);
				"
			);
			let params = db::params![message.tag, message.item];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	async fn indexer_delete_tags_postgres(
		&self,
		messages: Vec<DeleteTagMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			let statement = indoc!(
				"
					delete from tags
					where tag = $1;
				"
			);
			let params = db::params![message.tag];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_index_request<H>(
		handle: &H,
		_request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		handle.index().await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}

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

async fn migration_0000(database: &Database) -> tg::Result<()> {
	let sql = indoc!(
		"
			create table cache_entries (
				id text primary key,
				reference_count integer,
				touched_at text
			);

			create index cache_entries_reference_count_zero_index on cache_entries (touched_at) where reference_count = 0;

			create trigger cache_entries_insert_reference_count_trigger
			after insert on cache_entries
			for each row
			when (new.reference_count is null)
			begin
				update cache_entries
				set reference_count = (
					(select count(*) from objects where cache_reference = new.id) +
					(select count(*) from tags where item = new.id)
				)
				where id = new.id;
			end;

			create table objects (
				id text primary key,
				cache_reference text,
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

			create index objects_cache_entry_index on objects (cache_reference) where cache_reference is not null;

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

			create trigger objects_insert_cache_reference_trigger
			after insert on objects
			for each row
			begin
				update cache_entries
				set reference_count = reference_count + 1
				where id = new.cache_reference;
			end;

			create trigger objects_delete_trigger
			after delete on objects
			for each row
			begin
				update cache_entries
				set reference_count = reference_count - 1
				where id = old.cache_reference;

				delete from object_children
				where object = old.id;
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

			create unique index process_objects_index on process_objects (process, object, kind);

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
				update cache_entries set reference_count = reference_count + 1
				where id = new.item;

				update objects set reference_count = reference_count + 1
				where id = new.item;

				update processes set reference_count = reference_count + 1
				where id = new.item;
			end;

			create trigger tags_delete_trigger
			after delete on tags
			for each row
			begin
				update cache_entries set reference_count = reference_count - 1
				where id = old.item;

				update objects set reference_count = reference_count - 1
				where id = old.item;

				update processes set reference_count = reference_count - 1
				where id = old.item;
			end;
		"
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
	Ok(())
}

impl std::fmt::Display for ProcessObjectKind {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Command => write!(formatter, "command"),
			Self::Output => write!(formatter, "output"),
		}
	}
}

impl std::str::FromStr for ProcessObjectKind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"command" => Ok(Self::Command),
			"output" => Ok(Self::Output),
			_ => Err(tg::error!("invalid kind")),
		}
	}
}
