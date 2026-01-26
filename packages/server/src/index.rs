use {
	crate::{Context, Server},
	futures::{FutureExt as _, Stream, StreamExt as _, stream},
	std::{panic::AssertUnwindSafe, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::{
		stream::Ext as _,
		task::{Stop, Task},
	},
	tangram_http::{Body, request::Ext as _},
	tangram_index::{self as index, prelude::*},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Index {
	#[cfg(feature = "foundationdb")]
	Fdb(index::fdb::Index),
	#[cfg(feature = "lmdb")]
	Lmdb(index::lmdb::Index),
}

impl Index {
	#[cfg(feature = "foundationdb")]
	pub fn new_fdb(cluster: &std::path::Path, prefix: Option<String>) -> tg::Result<Self> {
		Ok(Self::Fdb(index::fdb::Index::new(cluster, prefix)?))
	}

	#[cfg(feature = "lmdb")]
	pub fn new_lmdb(config: &index::lmdb::Config) -> tg::Result<Self> {
		Ok(Self::Lmdb(index::lmdb::Index::new(config)?))
	}
}

impl index::Index for Index {
	async fn try_get_objects(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<index::Object>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_objects(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_objects(ids).await,
		}
	}

	async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<index::Process>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_processes(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_processes(ids).await,
		}
	}

	async fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<index::Object>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.touch_objects(ids, touched_at).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.touch_objects(ids, touched_at).await,
		}
	}

	async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<index::Process>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.touch_processes(ids, touched_at).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.touch_processes(ids, touched_at).await,
		}
	}

	async fn put(&self, arg: index::PutArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put(arg).await,
		}
	}

	async fn put_tags(&self, args: &[index::PutTagArg]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_tags(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_tags(args).await,
		}
	}

	async fn delete_tags(&self, tags: &[String]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.delete_tags(tags).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.delete_tags(tags).await,
		}
	}

	async fn update_batch(&self, batch_size: usize) -> tg::Result<usize> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.update_batch(batch_size).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.update_batch(batch_size).await,
		}
	}

	async fn get_transaction_id(&self) -> tg::Result<u128> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.get_transaction_id().await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.get_transaction_id().await,
		}
	}

	async fn get_queue_size(&self, transaction_id: u128) -> tg::Result<u64> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.get_queue_size(transaction_id).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.get_queue_size(transaction_id).await,
		}
	}

	async fn clean(
		&self,
		max_touched_at: i64,
		batch_size: usize,
	) -> tg::Result<index::CleanOutput> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.clean(max_touched_at, batch_size).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.clean(max_touched_at, batch_size).await,
		}
	}

	async fn sync(&self) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.sync().await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.sync().await,
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
		self.index_tasks.wait().await;
		progress.finish("tasks");

		// Subscribe to indexer progress.
		let indexer_progress_stream = self
			.messenger
			.subscribe::<()>("indexer_progress".to_owned(), None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to indexer progress"))?;
		let interval = IntervalStream::new(tokio::time::interval(Duration::from_secs(1)));
		let mut indexer_progress_stream =
			stream::select(indexer_progress_stream.map(|_| ()), interval.map(|_| ()));

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
		loop {
			let result = self.index.update_batch(config.queue_batch_size).await;
			match result {
				Ok(0) => {
					// No items processed, wait a bit.
					tokio::time::sleep(Duration::from_millis(100)).await;
				},
				Ok(_n) => {
					// Items processed, publish progress.
					self.messenger
						.publish("indexer_progress".to_owned(), ())
						.await
						.ok();
				},
				Err(error) => {
					tracing::error!(?error, "failed to handle the index update");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
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
