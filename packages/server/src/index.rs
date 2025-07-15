use crate::{Server, util::iter::Ext as _};
use futures::{Stream, StreamExt as _, TryStreamExt as _, future};
use num::ToPrimitive as _;
use std::{collections::BTreeSet, pin::pin, time::Duration};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _};
use tangram_messenger::{self as messenger, Acker, prelude::*};
use tokio_util::task::AbortOnDropHandle;

mod lmdb;

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

			self.indexer_put_objects(put_object_messages).await?;

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

	async fn indexer_put_objects(&self, messages: Vec<PutObjectMessage>) -> tg::Result<()> {
		self.index.put_objects(messages)?;
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

pub use self::lmdb::Lmdb;

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Index {
	// #[cfg(feature = "foundationdb")]
	// Fdb(Fdb),
	Lmdb(Lmdb),
	// Memory(Memory),
	// S3(S3),
}

#[derive(Clone, Debug)]
pub struct PutObjectBatchArg {
	objects: Vec<PutObjectArg>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PutObjectArg {
	pub cache_reference: Option<tg::artifact::Id>,
	pub id: tg::object::Id,
	pub size: Option<u64>,
	pub touched_at: Option<i64>,
	pub incomplete_children: Option<u64>,
	pub complete: bool,
	pub count: Option<u64>,
	pub depth: Option<u64>,
	pub weight: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct PutObjectChildrenArg {
	pub id: tg::object::Id,
	pub children: Vec<tg::object::Id>,
}

#[derive(Clone, Debug)]
pub struct PutObjectChildrenBatchArg {
	items: Vec<PutObjectChildrenArg>,
}

pub struct DecrementObjectIncompleteChildrenBatchArg {
	pub ids: Vec<tg::object::Id>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Object {
	pub id: tg::object::Id,
	pub complete: bool,
	pub count: Option<u64>,
	pub depth: Option<u64>,
	pub incomplete_children: Option<u64>,
	pub size: Option<u64>,
	pub weight: Option<u64>,
}

impl Index {
	// #[cfg(feature = "foundationdb")]
	// pub fn new_fdb(config: &crate::config::FdbStore) -> tg::Result<Self> {
	// 	let fdb = Fdb::new(config)?;
	// 	Ok(Self::Fdb(fdb))
	// }

	pub fn new_lmdb(config: &crate::config::LmdbIndex) -> tg::Result<Self> {
		let lmdb = Lmdb::new(config)?;
		Ok(Self::Lmdb(lmdb))
	}

	// pub fn new_memory() -> Self {
	// 	Self::Memory(Memory::new())
	// }
	pub fn put_objects(&self, messages: Vec<PutObjectMessage>) -> tg::Result<()> {
		match self {
			// #[cfg(feature = "foundationdb")]
			// Self::Fdb(fdb) => fdb.put_objects(messages).await,
			Self::Lmdb(lmdb) => lmdb.put_objects(&messages),
			// Self::Memory(memory) => memory.put_objects(messages),
		}
	}

	pub async fn try_get_object_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<Object>>> {
		match self {
			// #[cfg(feature = "foundationdb")]
			// Self::Fdb(fdb) => fdb.try_get_objects_batch(ids).await,
			Self::Lmdb(lmdb) => lmdb.try_get_object_batch(ids),
			// Self::Memory(memory) => Ok(memory.try_get_objects_batch(ids)),
		}
	}

	pub async fn try_get_object_incomplete_children_count_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<u64>> {
		match self {
			// #[cfg(feature = "foundationdb")]
			// Self::Fdb(fdb) => fdb.try_get_objects_complete_batch(ids).await,
			Self::Lmdb(lmdb) => lmdb.try_get_object_incomplete_children_count_batch(ids),
			// Self::Memory(memory) => Ok(memory.try_get_objects_complete_batch(ids)),
		}
	}

	pub async fn try_get_object_complete_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
		match self {
			// #[cfg(feature = "foundationdb")]
			// Self::Fdb(fdb) => fdb.try_get_objects_complete_batch(ids).await,
			Self::Lmdb(lmdb) => lmdb.try_get_object_complete_batch(ids),
			// Self::Memory(memory) => Ok(memory.try_get_objects_complete_batch(ids)),
		}
	}

	pub async fn try_get_object_parents_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Vec<tg::object::Id>>> {
		match self {
			// #[cfg(feature = "foundationdb")]
			// Self::Fdb(fdb) => fdb.try_get_object_parents_batch(ids).await,
			Self::Lmdb(lmdb) => lmdb.try_get_object_parents_batch(ids),
			// Self::Memory(memory) => Ok(memory.try_get_object_parents_batch(ids)),
		}
	}

	pub async fn try_get_object_children_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Vec<tg::object::Id>>> {
		match self {
			// #[cfg(feature = "foundationdb")]
			// Self::Fdb(fdb) => fdb.try_get_object_children_batch(ids).await,
			Self::Lmdb(lmdb) => lmdb.try_get_object_children_batch(ids),
			// Self::Memory(memory) => Ok(memory.try_get_object_children_batch(ids)),
		}
	}

	// pub async fn put_object_batch(&self, arg: PutObjectBatchArg) -> tg::Result<()> {
	// 	match self {
	// 		// #[cfg(feature = "foundationdb")]
	// 		// Self::Fdb(fdb) => fdb.put_objects_batch(arg).await,
	// 		Self::Lmdb(lmdb) => lmdb.put_object_batch(arg).await,
	// 		// Self::Memory(memory) => memory.put_objects_batch(arg),
	// 	}
	// }

	// pub async fn put_object_children_batch(
	// 	&self,
	// 	arg: PutObjectChildrenBatchArg,
	// ) -> tg::Result<()> {
	// 	match self {
	// 		// #[cfg(feature = "foundationdb")]
	// 		// Self::Fdb(fdb) => fdb.put_object_children_batch(arg).await,
	// 		Self::Lmdb(lmdb) => lmdb.put_object_children_batch(arg).await,
	// 		// Self::Memory(memory) => memory.put_object_children_batch(arg),
	// 	}
	// }

	// pub async fn decrement_object_incomplete_children_count_batch(
	// 	&self,
	// 	ids: &[tg::object::Id],
	// ) -> tg::Result<Vec<u64>> {
	// 	match self {
	// 		// #[cfg(feature = "foundationdb")]
	// 		// Self::Fdb(fdb) => fdb.decrement_object_incomplete_children_count_batch(ids).await,
	// 		Self::Lmdb(lmdb) => {
	// 			lmdb.decrement_object_incomplete_children_count_batch(ids)
	// 				.await
	// 		},
	// 		// Self::Memory(memory) => memory.decrement_object_incomplete_children_count_batch(ids),
	// 	}
	// }
}
