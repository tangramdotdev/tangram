use crate::{Server, database::Database, util::iter::Ext as _};
use futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future};
use indoc::indoc;
use num::ToPrimitive as _;
use rusqlite as sqlite;
#[cfg(feature = "postgres")]
use std::collections::HashMap;
use std::{collections::BTreeSet, pin::pin, time::Duration};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _};
use tangram_messenger::{self as messenger, Acker, prelude::*};
use tokio_util::task::AbortOnDropHandle;

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
	pub children: Option<Vec<tg::Referent<tg::process::Id>>>,
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
	Error,
	Output,
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
				while first_sequence < last_sequence {
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
	) -> tg::Result<impl Stream<Item = tg::Result<Vec<(Message, Acker)>>>> {
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
				let message = serde_json::from_slice::<Message>(&payload)
					.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;
				Ok::<_, tg::Error>((message, acker))
			})
			.inspect_err(|error| {
				tracing::error!(?error);
			})
			.filter_map(|result| future::ready(result.ok()))
			.ready_chunks(config.message_batch_size)
			.map(Ok);
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
				Database::Sqlite(index) => {
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
				#[cfg(feature = "postgres")]
				Database::Postgres(index) => {
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
			let params = sqlite::params![message.id.to_string(), message.touched_at];
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
			let touched_at = message.touched_at;

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
			let params = sqlite::params![message.touched_at, message.id.to_string()];
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
		let object_statement = indoc!(
			"
				insert into process_objects (process, object, kind)
				values (?1, ?2, ?3)
				on conflict (process, object, kind) do nothing;
			"
		);
		let mut object_statement = transaction
			.prepare_cached(object_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let child_statement = indoc!(
			"
				insert into process_children (process, position, child, path, tag)
				values (?1, ?2, ?3, ?4, ?5)
				on conflict (process, child) do nothing;
			"
		);
		let mut child_statement = transaction
			.prepare_cached(child_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let process_statement = indoc!(
			"
				insert into processes (id, touched_at)
				values (?1, ?2)
				on conflict (id) do update set touched_at = ?2;
			"
		);
		let mut process_statement = transaction
			.prepare_cached(process_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		for message in messages {
			// Insert the objects.
			for (object, kind) in message.objects {
				let params =
					sqlite::params![message.id.to_string(), object.to_string(), kind.to_string()];
				object_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Insert the children.
			if let Some(children) = message.children {
				for (position, child) in children.iter().enumerate() {
					let params = sqlite::params![
						message.id.to_string(),
						position,
						child.item.to_string(),
						child.path().map(|path| path.display().to_string()),
						child.tag().map(ToString::to_string),
					];
					child_statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				}
			}

			// Insert the process.
			let params = sqlite::params![message.id.to_string(), message.touched_at];
			process_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
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
			let params = sqlite::params![message.touched_at, message.id.to_string()];
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
	#[cfg(feature = "postgres")]
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

	#[cfg(feature = "postgres")]
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
			let params = db::params![message.id.to_string(), message.touched_at];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	#[cfg(feature = "postgres")]
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
			.map(|message| message.touched_at)
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
					$4::int8[],
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

	#[cfg(feature = "postgres")]
	async fn indexer_touch_objects_postgres(
		&self,
		messages: Vec<TouchObjectMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			let statement = indoc!(
				"
					update objects
					set touched_at = $1
					where id = $2;
				"
			);
			let params = db::params![message.touched_at, message.id.to_string()];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	#[cfg(feature = "postgres")]
	async fn indexer_put_processes_postgres(
		&self,
		messages: Vec<PutProcessMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			// Insert the objects.
			let objects: Vec<&tg::object::Id> = message.objects.iter().map(|(id, _)| id).collect();
			let kinds: Vec<&ProcessObjectKind> =
				message.objects.iter().map(|(_, kind)| kind).collect();
			if !message.objects.is_empty() {
				let statement = indoc!(
					"
						insert into process_objects (process, object, kind)
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

			// Insert the children.
			if let Some(children) = &message.children {
				let positions: Vec<i64> = (0..children.len().to_i64().unwrap()).collect();
				let statement = indoc!(
					"
						insert into process_children (process, position, child, path, tag)
						select $1, unnest($2::int8[]), unnest($3::text[]), unnest($4::text[]), unnest($5::text[])
						on conflict (process, child) do nothing;
					"
				);
				transaction
					.inner()
					.execute(
						statement,
						&[
							&message.id.to_string(),
							&positions.as_slice(),
							&children
								.iter()
								.map(|referent| referent.item.to_string())
								.collect::<Vec<_>>(),
							&children
								.iter()
								.map(|referent| {
									referent.path().map(|path| path.display().to_string())
								})
								.collect::<Vec<_>>(),
							&children
								.iter()
								.map(|referent| referent.tag().map(ToString::to_string))
								.collect::<Vec<_>>(),
						],
					)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

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
				.execute(
					process_statement,
					&[&message.id.to_string(), &message.touched_at],
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	#[cfg(feature = "postgres")]
	async fn indexer_touch_processes_postgres(
		&self,
		messages: Vec<TouchProcessMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			let statement = indoc!(
				"
					update processes
					set touched_at = $1
					where id = $2;
				"
			);
			let params = db::params![message.touched_at, message.id.to_string()];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	#[cfg(feature = "postgres")]
	async fn indexer_put_tags_postgres(
		&self,
		messages: Vec<PutTagMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			let statement = indoc!(
				"
					insert into tags (tag, item)
					values ($1, $2)
					on conflict (tag) do update
					set tag = $1, item = $2;
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

	#[cfg(feature = "postgres")]
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

pub async fn migrate(database: &Database) -> tg::Result<()> {
	#[allow(irrefutable_let_patterns)]
	let Database::Sqlite(database) = database else {
		return Ok(());
	};

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

impl std::fmt::Display for ProcessObjectKind {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Command => write!(formatter, "command"),
			Self::Error => write!(formatter, "error"),
			Self::Output => write!(formatter, "output"),
		}
	}
}

impl std::str::FromStr for ProcessObjectKind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"command" => Ok(Self::Command),
			"error" => Ok(Self::Error),
			"output" => Ok(Self::Output),
			_ => Err(tg::error!("invalid kind")),
		}
	}
}
