#[cfg(any(feature = "postgres", feature = "sqlite"))]
use tangram_database as db;
use tangram_index::{self as index, prelude::*};
use {
	crate::{Context, Server},
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream},
	num::ToPrimitive as _,
	std::{panic::AssertUnwindSafe, pin::pin, task::Poll, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::{
		stream::Ext as _,
		task::{Stop, Task},
	},
	tangram_http::{Body, request::Ext as _},
	tangram_messenger::{self as messenger, prelude::*},
	tokio_stream::wrappers::IntervalStream,
};

pub use self::message::Message;

pub mod message;

// Re-export types from the index crate.
pub use index::{ObjectStored, ProcessStored};

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Index {
	#[cfg(feature = "postgres")]
	Postgres(index::postgres::Index),
	#[cfg(feature = "sqlite")]
	Sqlite(index::sqlite::Index),
}

impl Index {
	#[cfg(feature = "postgres")]
	pub fn new_postgres(database: db::postgres::Database) -> Self {
		Self::Postgres(index::postgres::Index::new(database))
	}

	#[cfg(feature = "sqlite")]
	pub fn new_sqlite(database: db::sqlite::Database) -> Self {
		Self::Sqlite(index::sqlite::Index::new(database))
	}
}

// Implement the Index trait by delegation.
impl index::Index for Index {
	async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.try_get_object_metadata(id).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.try_get_object_metadata(id).await,
		}
	}

	async fn try_get_object_metadata_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::object::Metadata>>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.try_get_object_metadata_batch(ids).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.try_get_object_metadata_batch(ids).await,
		}
	}

	async fn try_get_object_stored(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<index::ObjectStored>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.try_get_object_stored(id).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.try_get_object_stored(id).await,
		}
	}

	async fn try_get_object_stored_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<index::ObjectStored>>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.try_get_object_stored_batch(ids).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.try_get_object_stored_batch(ids).await,
		}
	}

	async fn try_get_object_stored_and_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(index::ObjectStored, tg::object::Metadata)>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.try_get_object_stored_and_metadata(id).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.try_get_object_stored_and_metadata(id).await,
		}
	}

	async fn try_get_object_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(index::ObjectStored, tg::object::Metadata)>>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.try_get_object_stored_and_metadata_batch(ids).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.try_get_object_stored_and_metadata_batch(ids).await,
		}
	}

	async fn try_touch_object_and_get_stored_and_metadata(
		&self,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(index::ObjectStored, tg::object::Metadata)>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => {
				index
					.try_touch_object_and_get_stored_and_metadata(id, touched_at)
					.await
			},
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => {
				index
					.try_touch_object_and_get_stored_and_metadata(id, touched_at)
					.await
			},
		}
	}

	async fn try_touch_object_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(index::ObjectStored, tg::object::Metadata)>>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => {
				index
					.try_touch_object_and_get_stored_and_metadata_batch(ids, touched_at)
					.await
			},
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => {
				index
					.try_touch_object_and_get_stored_and_metadata_batch(ids, touched_at)
					.await
			},
		}
	}

	async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.try_get_process_metadata(id).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.try_get_process_metadata(id).await,
		}
	}

	async fn try_get_process_metadata_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::Metadata>>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.try_get_process_metadata_batch(ids).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.try_get_process_metadata_batch(ids).await,
		}
	}

	async fn try_get_process_stored(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<index::ProcessStored>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.try_get_process_stored(id).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.try_get_process_stored(id).await,
		}
	}

	async fn try_get_process_stored_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<index::ProcessStored>>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.try_get_process_stored_batch(ids).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.try_get_process_stored_batch(ids).await,
		}
	}

	async fn try_get_process_stored_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(index::ProcessStored, tg::process::Metadata)>>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.try_get_process_stored_and_metadata_batch(ids).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.try_get_process_stored_and_metadata_batch(ids).await,
		}
	}

	async fn try_touch_process_and_get_stored_and_metadata(
		&self,
		id: &tg::process::Id,
		touched_at: i64,
	) -> tg::Result<Option<(index::ProcessStored, tg::process::Metadata)>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => {
				index
					.try_touch_process_and_get_stored_and_metadata(id, touched_at)
					.await
			},
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => {
				index
					.try_touch_process_and_get_stored_and_metadata(id, touched_at)
					.await
			},
		}
	}

	async fn try_touch_process_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(index::ProcessStored, tg::process::Metadata)>>> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => {
				index
					.try_touch_process_and_get_stored_and_metadata_batch(ids, touched_at)
					.await
			},
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => {
				index
					.try_touch_process_and_get_stored_and_metadata_batch(ids, touched_at)
					.await
			},
		}
	}

	async fn handle_messages(
		&self,
		put_cache_entry: Vec<index::PutCacheEntryArg>,
		put_object: Vec<index::PutObjectArg>,
		touch_object: Vec<index::TouchObjectArg>,
		put_process: Vec<index::PutProcessArg>,
		touch_process: Vec<index::TouchProcessArg>,
		put_tag: Vec<index::PutTagArg>,
		delete_tag: Vec<index::DeleteTagArg>,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => {
				index
					.handle_messages(
						put_cache_entry,
						put_object,
						touch_object,
						put_process,
						touch_process,
						put_tag,
						delete_tag,
					)
					.await
			},
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => {
				index
					.handle_messages(
						put_cache_entry,
						put_object,
						touch_object,
						put_process,
						touch_process,
						put_tag,
						delete_tag,
					)
					.await
			},
		}
	}

	async fn handle_queue(&self, batch_size: usize) -> tg::Result<usize> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.handle_queue(batch_size).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.handle_queue(batch_size).await,
		}
	}

	async fn get_transaction_id(&self) -> tg::Result<u64> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.get_transaction_id().await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.get_transaction_id().await,
		}
	}

	async fn get_queue_size(&self, transaction_id: u64) -> tg::Result<u64> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.get_queue_size(transaction_id).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.get_queue_size(transaction_id).await,
		}
	}

	async fn clean(&self, max_touched_at: i64, n: usize) -> tg::Result<index::CleanOutput> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.clean(max_touched_at, n).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.clean(max_touched_at, n).await,
		}
	}

	async fn touch_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.touch_object(id).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.touch_object(id).await,
		}
	}

	async fn touch_process(&self, id: &tg::process::Id) -> tg::Result<()> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.touch_process(id).await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.touch_process(id).await,
		}
	}

	async fn sync(&self) -> tg::Result<()> {
		match self {
			#[cfg(feature = "postgres")]
			Self::Postgres(index) => index.sync().await,
			#[cfg(feature = "sqlite")]
			Self::Sqlite(index) => index.sync().await,
		}
	}
}

impl Server {
	pub(crate) async fn index_with_context(
		&self,
		context: &Context,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + use<>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		let progress = crate::progress::Handle::new();
		let task = Task::spawn({
			let progress = progress.clone();
			let server = self.clone();
			|_| async move {
				let result = AssertUnwindSafe(server.index_task(&progress))
					.catch_unwind()
					.await;
				match result {
					Ok(Ok(())) => {
						progress.output(());
					},
					Ok(Err(error)) => {
						progress.error(error);
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						progress.error(tg::error!(?message, "the task panicked"));
					},
				}
			}
		});
		let stream = progress.stream().attach(task);
		Ok(stream)
	}

	async fn index_task(&self, progress: &crate::progress::Handle<()>) -> tg::Result<()> {
		// Wait for outstanding tasks to finish.
		progress.spinner("tasks", "waiting for tasks");
		self.tasks.wait().await;
		progress.finish("tasks");

		// Get the index stream.
		let index_stream = self
			.messenger
			.get_stream("index".to_owned())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the index stream"))?;

		// Subscribe to indexer progress.
		let indexer_progress_stream = self
			.messenger
			.subscribe::<()>("indexer_progress".to_owned(), None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to indexer progress"))?;
		let interval = IntervalStream::new(tokio::time::interval(Duration::from_secs(1)));
		let mut indexer_progress_stream =
			stream::select(indexer_progress_stream.map(|_| ()), interval.map(|_| ()));

		// Wait for the index stream's first sequence to reach the current last sequence.
		let info = index_stream
			.info()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the index stream info"))?;
		let mut first_sequence = info.first_sequence;
		let last_sequence = info.last_sequence;
		let total = info.last_sequence.saturating_sub(info.first_sequence);
		if last_sequence > 0 {
			progress.start(
				"messages".to_owned(),
				"messages".to_owned(),
				tg::progress::IndicatorFormat::Normal,
				Some(0),
				Some(total),
			);
			loop {
				let info = index_stream
					.info()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the index stream info"))?;
				progress.increment("messages", info.first_sequence - first_sequence);
				first_sequence = info.first_sequence;
				if first_sequence > last_sequence {
					break;
				}
				indexer_progress_stream.next().await;
			}
			progress.finish("messages");
		}

		// Wait until the index's queue no longer has items whose transaction id is less than or equal to the current transaction id.
		let transaction_id = self
			.index
			.get_transaction_id()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the transaction id"))?;
		let count = self
			.index
			.get_queue_size(transaction_id)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the queue size"))?;
		progress.start(
			"queue".to_owned(),
			"queue".to_owned(),
			tg::progress::IndicatorFormat::Normal,
			Some(count),
			None,
		);
		loop {
			let count = self
				.index
				.get_queue_size(transaction_id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the queue size"))?;
			progress.set("queue", count);
			if count == 0 {
				break;
			}
			indexer_progress_stream.next().await;
		}
		progress.finish("queue");

		Ok::<_, tg::Error>(())
	}

	pub(crate) async fn indexer_task(&self, config: &crate::config::Indexer) -> tg::Result<()> {
		// Get the messages stream.
		let stream = self
			.indexer_create_message_stream(config)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the message stream"))?;
		let mut stream = pin!(stream);

		let mut wait = false;
		loop {
			let result = if wait {
				stream.try_next().await
			} else {
				match futures::poll!(stream.try_next()) {
					Poll::Ready(result) => result,
					Poll::Pending => {
						let result = self.index.handle_queue(config.queue_batch_size).await;
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
						} else {
							self.messenger
								.publish("indexer_progress".to_owned(), ())
								.await
								.ok();
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
			} else {
				// Publish indexer progress.
				self.messenger
					.publish("indexer_progress".to_owned(), ())
					.await
					.ok();
			}
		}
	}

	async fn indexer_create_message_stream(
		&self,
		config: &crate::config::Indexer,
	) -> tg::Result<impl Stream<Item = tg::Result<Vec<(Vec<Message>, messenger::Acker)>>>> {
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
			.batch_subscribe::<message::Messages>(batch_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the stream"))?
			.boxed()
			.map_err(|source| tg::error!(!source, "failed to get a message from the stream"))
			.map_ok(|message| {
				let (messages, acker) = message.split();
				(messages.0, acker)
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
		messages: Vec<(Vec<Message>, messenger::Acker)>,
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
		let mut ackers: Vec<messenger::Acker> = Vec::new();

		for (messages, acker) in messages {
			for message in messages {
				if n >= config.insert_batch_size {
					// Handle the messages.
					let put_cache_entry_arg = put_cache_entry_messages
						.drain(..)
						.map(|m: message::PutCacheEntry| index::PutCacheEntryArg {
							id: m.id,
							touched_at: m.touched_at,
						})
						.collect();
					let put_object_arg = put_object_messages
						.drain(..)
						.map(|m: message::PutObject| index::PutObjectArg {
							cache_entry: m.cache_entry,
							children: m.children,
							id: m.id,
							metadata: m.metadata,
							stored: m.stored,
							touched_at: m.touched_at,
						})
						.collect();
					let touch_object_arg = touch_object_messages
						.drain(..)
						.map(|m: message::TouchObject| index::TouchObjectArg {
							id: m.id,
							touched_at: m.touched_at,
						})
						.collect();
					let put_process_arg = put_process_messages
						.drain(..)
						.map(|m: message::PutProcess| index::PutProcessArg {
							children: m.children,
							id: m.id,
							metadata: m.metadata,
							objects: m
								.objects
								.into_iter()
								.map(|(id, kind)| (id, kind.into()))
								.collect(),
							stored: m.stored,
							touched_at: m.touched_at,
						})
						.collect();
					let touch_process_arg = touch_process_messages
						.drain(..)
						.map(|m: message::TouchProcess| index::TouchProcessArg {
							id: m.id,
							touched_at: m.touched_at,
						})
						.collect();
					let put_tag_arg = put_tag_messages
						.drain(..)
						.map(|m: message::PutTagMessage| index::PutTagArg {
							tag: m.tag,
							item: m.item,
						})
						.collect();
					let delete_tag_arg = delete_tag_messages
						.drain(..)
						.map(|m: message::DeleteTag| index::DeleteTagArg { tag: m.tag })
						.collect();
					self.index
						.handle_messages(
							put_cache_entry_arg,
							put_object_arg,
							touch_object_arg,
							put_process_arg,
							touch_process_arg,
							put_tag_arg,
							delete_tag_arg,
						)
						.await?;

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
		let put_cache_entry_arg = put_cache_entry_messages
			.drain(..)
			.map(|m: message::PutCacheEntry| index::PutCacheEntryArg {
				id: m.id,
				touched_at: m.touched_at,
			})
			.collect();
		let put_object_arg = put_object_messages
			.drain(..)
			.map(|m: message::PutObject| index::PutObjectArg {
				cache_entry: m.cache_entry,
				children: m.children,
				id: m.id,
				metadata: m.metadata,
				stored: m.stored,
				touched_at: m.touched_at,
			})
			.collect();
		let touch_object_arg = touch_object_messages
			.drain(..)
			.map(|m: message::TouchObject| index::TouchObjectArg {
				id: m.id,
				touched_at: m.touched_at,
			})
			.collect();
		let put_process_arg = put_process_messages
			.drain(..)
			.map(|m: message::PutProcess| index::PutProcessArg {
				children: m.children,
				id: m.id,
				metadata: m.metadata,
				objects: m
					.objects
					.into_iter()
					.map(|(id, kind)| (id, kind.into()))
					.collect(),
				stored: m.stored,
				touched_at: m.touched_at,
			})
			.collect();
		let touch_process_arg = touch_process_messages
			.drain(..)
			.map(|m: message::TouchProcess| index::TouchProcessArg {
				id: m.id,
				touched_at: m.touched_at,
			})
			.collect();
		let put_tag_arg = put_tag_messages
			.drain(..)
			.map(|m: message::PutTagMessage| index::PutTagArg {
				tag: m.tag,
				item: m.item,
			})
			.collect();
		let delete_tag_arg = delete_tag_messages
			.drain(..)
			.map(|m: message::DeleteTag| index::DeleteTagArg { tag: m.tag })
			.collect();
		self.index
			.handle_messages(
				put_cache_entry_arg,
				put_object_arg,
				touch_object_arg,
				put_process_arg,
				touch_process_arg,
				put_tag_arg,
				delete_tag_arg,
			)
			.await?;

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

	pub(crate) async fn handle_index_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the stream.
		let stream = self
			.index_with_context(context)
			.await
			.map_err(|source| tg::error!(!source, "failed to start the index task"))?;

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
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
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
