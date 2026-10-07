use {
	crate::{Server, Session},
	futures::{FutureExt as _, Stream, StreamExt as _, future},
	std::{ops::ControlFlow, panic::AssertUnwindSafe, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::{self as index, Index as _},
};

mod checkout;
mod group;
mod indexer;
mod object;
mod organization;
mod permission;
mod process;
mod sandbox;
mod tag;
mod usage;
mod user;
mod wait;

pub(crate) use self::wait::Sender as WaitSender;

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Index {
	#[cfg(feature = "foundationdb")]
	Fdb(tangram_index_fdb::Index),
	#[cfg(feature = "lmdb")]
	Lmdb(tangram_index_lmdb::Index),
}

impl Index {
	#[cfg(feature = "foundationdb")]
	pub fn new_fdb(options: &tangram_index_fdb::Options) -> tg::Result<Self> {
		Ok(Self::Fdb(tangram_index_fdb::Index::new(options)?))
	}

	#[cfg(feature = "lmdb")]
	pub fn new_lmdb(config: &tangram_index_lmdb::Config) -> tg::Result<Self> {
		Ok(Self::Lmdb(tangram_index_lmdb::Index::new(config)?))
	}
}

impl index::Index for Index {
	async fn verify_batch(
		&self,
		args: &[index::verify::Arg],
		config: index::verify::Config,
		principal: &tg::Principal,
	) -> tg::Result<Vec<index::verify::Output>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.verify_batch(args, config, principal).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.verify_batch(args, config, principal).await,
		}
	}

	async fn contains_ids(&self, ids: &[tg::Id]) -> tg::Result<Vec<bool>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.contains_ids(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.contains_ids(ids).await,
		}
	}

	async fn visible(&self, ids: &[tg::Id], principal: &tg::Principal) -> tg::Result<Vec<bool>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.visible(ids, principal).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.visible(ids, principal).await,
		}
	}

	async fn batch(&self, arg: index::batch::Arg) -> tg::Result<tg::Result<()>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.batch(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.batch(arg).await,
		}
	}

	async fn try_get_ancestors(&self, id: &tg::Id) -> tg::Result<Option<Vec<tg::Id>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_ancestors(id).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_ancestors(id).await,
		}
	}

	async fn try_get_ids_for_specifiers(
		&self,
		specifiers: &[tg::Specifier],
	) -> tg::Result<Vec<Option<tg::Id>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_ids_for_specifiers(specifiers).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_ids_for_specifiers(specifiers).await,
		}
	}

	async fn get_requester_subjects(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<Vec<tg::authorization::Subject>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.get_requester_subjects(principal).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.get_requester_subjects(principal).await,
		}
	}

	async fn try_get_specifiers_for_ids(
		&self,
		ids: &[tg::Id],
	) -> tg::Result<Vec<Option<tg::Specifier>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_specifiers_for_ids(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_specifiers_for_ids(ids).await,
		}
	}

	async fn try_get_oldest_update_transaction_id(
		&self,
		kind: index::update::Kind,
	) -> tg::Result<Option<u64>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_oldest_update_transaction_id(kind).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_oldest_update_transaction_id(kind).await,
		}
	}

	async fn update_batch(
		&self,
		kind: index::update::Kind,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<index::update::Output> {
		#[cfg(not(feature = "foundationdb"))]
		let _ = (partition_start, partition_end);
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => {
				index
					.update_batch(kind, batch_size, partition_start, partition_end)
					.await
			},
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.update_batch(kind, batch_size).await,
		}
	}

	async fn clean(&self, arg: index::clean::Arg) -> tg::Result<index::clean::Output> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.clean(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.clean(arg).await,
		}
	}

	async fn get_transaction_id(&self) -> tg::Result<u64> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.get_transaction_id().await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.get_transaction_id().await,
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

	fn cleaning_partition_total(&self) -> u64 {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.cleaning_partition_total(),
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.cleaning_partition_total(),
		}
	}

	fn permission_update_partition_total(&self) -> u64 {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.permission_update_partition_total(),
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.permission_update_partition_total(),
		}
	}

	fn storage_and_metadata_update_partition_total(&self) -> u64 {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.storage_and_metadata_update_partition_total(),
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.storage_and_metadata_update_partition_total(),
		}
	}

	fn usage_update_partition_total(&self) -> u64 {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.usage_update_partition_total(),
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.usage_update_partition_total(),
		}
	}
}

impl Server {
	pub(crate) async fn index_batch(&self, arg: index::batch::Arg) -> tg::Result<()> {
		if arg.is_empty() {
			return Ok(());
		}
		if !self.config.advanced.single_process {
			let config = &self.config.object.index_queue;
			let bytes: bytes::Bytes = arg.serialize()?.into();
			let fragments = bytes
				.chunks(config.fragment_size)
				.map(|fragment| bytes.slice_ref(fragment))
				.collect();
			let batch =
				crate::cache::index::queue::batch::Id::new(uuid::Uuid::now_v7().into_bytes());
			self.enqueue_index_batch(batch, fragments).await?;

			return Ok(());
		}
		let command_object_permission = arg.items.iter().any(|item| {
			matches!(
				item,
				index::batch::Item::PutObject(arg)
					if arg.id.kind() == tg::object::Kind::Command
			)
		}) && arg.items.iter().any(|item| {
			matches!(
				item,
				index::batch::Item::PutPermission(arg)
					if arg.resource.kind() == tg::id::Kind::Command
						&& arg.subject.is_process()
			)
		});
		let finished_process = arg.items.iter().any(|item| {
			matches!(
				item,
				index::batch::Item::PutProcess(arg)
					if arg.output.is_some()
			)
		});
		let destroyed_sandbox = arg.items.iter().any(|item| {
			matches!(item, index::batch::Item::PutSandbox(arg)
				if arg.data.as_ref().is_some_and(|data| data.data.status.is_destroyed()))
		});
		let child_process = arg.items.iter().any(
			|item| matches!(item, index::batch::Item::PutProcess(arg) if arg.parent.is_some()),
		);
		let started_process = arg.items.iter().any(|item| {
			matches!(item, index::batch::Item::PutProcess(arg)
				if arg.data.as_ref().is_some_and(|data| data.status.is_started()))
		});
		self.index_tasks
			.spawn({
				let server = self.clone();
				|_| async move {
					crate::checkpoint!(
						server,
						"index.batch",
						child_process,
						command_object_permission,
						destroyed_sandbox,
						finished_process,
						started_process
					)
					.await;
					let result = server.index_batch_inner(arg).await;
					if result.is_ok() {
						server.index_changed.notify_waiters();
					}

					let result = result.and_then(std::convert::identity);
					if let Err(error) = &result {
						tracing::error!(error = %error.trace(), "failed to index a batch");
					}
					crate::checkpoint!(server, "index.batch.finished", command_object_permission)
						.await;

					result
				}
			})
			.detach();

		Ok(())
	}

	pub(crate) async fn index_batch_inner(
		&self,
		arg: index::batch::Arg,
	) -> tg::Result<tg::Result<()>> {
		// Wake index-backed readers after the runner state has been persisted.
		let sandboxes = arg
			.items
			.iter()
			.filter_map(|item| match item {
				index::batch::Item::PutSandbox(arg) if arg.data.is_some() => Some(arg.id.clone()),
				_ => None,
			})
			.collect::<Vec<_>>();
		let processes = arg
			.items
			.iter()
			.filter_map(|item| match item {
				index::batch::Item::PutProcess(arg) if arg.data.is_some() => Some(arg.id.clone()),
				_ => None,
			})
			.collect::<Vec<_>>();
		let result = self.index.batch(arg).await?;
		for id in sandboxes {
			self.spawn_publish_sandbox_status_task(&id);
		}
		for id in processes {
			self.spawn_publish_process_status_task(&id);
		}
		Ok(result)
	}

	async fn enqueue_index_batch(
		&self,
		batch: crate::cache::index::queue::batch::Id,
		fragments: Vec<bytes::Bytes>,
	) -> tg::Result<()> {
		let fragment_count = u64::try_from(fragments.len())
			.map_err(|_| tg::error!("the index batch has too many fragments"))?;
		let fragments: Arc<[bytes::Bytes]> = fragments.into();
		let start = rand::random::<u64>();
		let mut attempt = 0u64;
		let retry = tangram_futures::retry::Options::from(self.config.indexer.batch.retry.clone());
		tangram_futures::retry(&retry, || {
			let fragments = fragments.clone();
			let index = start.wrapping_add(attempt);
			let refresh = attempt > 0;
			attempt = attempt.wrapping_add(1);
			async move {
				let indexer = match self.select_indexer(index, refresh).await {
					Ok(indexer) => indexer,
					Err(error) => return Ok(ControlFlow::Continue(error)),
				};
				let result = self
					.enqueue_index_batch_with_indexer(&indexer, batch, fragment_count, &fragments)
					.await;
				match result {
					Ok(()) => Ok(ControlFlow::Break(())),
					Err(error) => Ok(ControlFlow::Continue(error)),
				}
			}
		})
		.await?;

		Ok(())
	}

	async fn enqueue_index_batch_with_indexer(
		&self,
		indexer: &tg::indexer::Id,
		batch: crate::cache::index::queue::batch::Id,
		fragment_count: u64,
		fragments: &[bytes::Bytes],
	) -> tg::Result<()> {
		let requests = fragments
			.iter()
			.cloned()
			.enumerate()
			.map(|(fragment, payload)| {
				let arg = crate::indexer::RequestArg::Index(crate::indexer::IndexRequestArg {
					batch,
					fragment: u64::try_from(fragment).unwrap(),
					fragments: fragment_count,
					payload,
				});
				async {
					let output = self
						.send_indexer_request(Some(indexer), arg)
						.await
						.map_err(|source| tg::error!(!source, "failed to send an index request"))?
						.map_err(|source| {
							tg::error!(!source, "the indexer failed to enqueue an index fragment")
						})?;
					output
						.try_unwrap_index()
						.map_err(|_| tg::error!("expected an index response"))?;

					Ok::<_, tg::Error>(())
				}
			});
		let request = future::try_join_all(requests);
		tokio::time::timeout(self.config.indexer.batch.timeout, request)
			.await
			.map_err(|source| tg::error!(!source, "timed out enqueueing an index batch"))??;

		Ok(())
	}
}

impl Session {
	pub(crate) async fn index(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + use<>> {
		if !self
			.server
			.config
			.roles
			.contains(&crate::config::Role::Indexer)
			&& self.server.config.advanced.single_process
		{
			return Err(tg::error!("cannot index when the indexer is disabled"));
		}
		let progress = crate::progress::Handle::new();
		let task = Task::spawn({
			let progress = progress.clone();
			let session = self.clone();
			|_| async move {
				let result = AssertUnwindSafe(session.index_task(&progress))
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
		let stream = progress
			.stream()
			.attach(task)
			.with_stopper(self.context.stopper.clone());
		Ok(stream)
	}

	async fn index_task(&self, progress: &crate::progress::Handle<()>) -> tg::Result<()> {
		progress.spinner("index", "waiting for indexing");
		self.server.index_inner().await?;
		progress.finish("index");
		Ok(())
	}
}

impl Session {
	pub(crate) async fn index_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		self.verify_request_from_host()?;

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the stream.
		let stream = self
			.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to start the index task"))?;

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
				(Some(content_type), BoxBody::with_sse_stream(stream))
			},

			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
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
