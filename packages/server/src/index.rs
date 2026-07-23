use {
	crate::{Server, Session},
	futures::{FutureExt as _, Stream, StreamExt as _},
	std::{panic::AssertUnwindSafe, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::{self as index, Index as _},
	tangram_object_store::Store as _,
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
	pub fn new_fdb(options: &index::fdb::Options) -> tg::Result<Self> {
		Ok(Self::Fdb(index::fdb::Index::new(options)?))
	}

	#[cfg(feature = "lmdb")]
	pub fn new_lmdb(config: &index::lmdb::Config) -> tg::Result<Self> {
		Ok(Self::Lmdb(index::lmdb::Index::new(config)?))
	}
}

impl index::Index for Index {
	async fn authorize_batch(
		&self,
		args: &[index::authorize::Arg],
		principal: &tg::Principal,
	) -> tg::Result<Vec<Option<index::authorize::Output>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.authorize_batch(args, principal).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.authorize_batch(args, principal).await,
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

	async fn batch(&self, arg: index::batch::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.batch(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.batch(arg).await,
		}
	}

	async fn try_get_cache_entries(
		&self,
		ids: &[tg::artifact::Id],
	) -> tg::Result<Vec<Option<index::cache::Entry>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_cache_entries(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_cache_entries(ids).await,
		}
	}

	async fn touch_cache_entries(
		&self,
		ids: &[tg::artifact::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::cache::Entry>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => {
				index
					.touch_cache_entries(ids, touched_at, time_to_touch)
					.await
			},
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => {
				index
					.touch_cache_entries(ids, touched_at, time_to_touch)
					.await
			},
		}
	}

	async fn try_get_objects(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<index::object::Object>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_objects(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_objects(ids).await,
		}
	}

	async fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::object::Object>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.touch_objects(ids, touched_at, time_to_touch).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.touch_objects(ids, touched_at, time_to_touch).await,
		}
	}

	async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<index::process::Process>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_processes(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_processes(ids).await,
		}
	}

	async fn try_get_cached_processes(
		&self,
		command: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, index::process::Process)>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_cached_processes(command).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_cached_processes(command).await,
		}
	}

	async fn get_requester_principals(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<Vec<tg::grant::Principal>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.get_requester_principals(principal).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.get_requester_principals(principal).await,
		}
	}

	async fn list_sandboxes_for_creator(
		&self,
		creator: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, index::sandbox::Sandbox)>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.list_sandboxes_for_creator(creator).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.list_sandboxes_for_creator(creator).await,
		}
	}

	async fn list_sandboxes_for_owner(
		&self,
		owner: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, index::sandbox::Sandbox)>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.list_sandboxes_for_owner(owner).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.list_sandboxes_for_owner(owner).await,
		}
	}

	async fn get_runner_sandboxes(
		&self,
		runner: &tg::runner::Id,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.get_runner_sandboxes(runner).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.get_runner_sandboxes(runner).await,
		}
	}

	async fn get_sandbox_processes(
		&self,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Vec<(tg::process::Id, index::process::Process)>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.get_sandbox_processes(sandbox).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.get_sandbox_processes(sandbox).await,
		}
	}

	async fn list_sandboxes(&self) -> tg::Result<Vec<(tg::sandbox::Id, index::sandbox::Sandbox)>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.list_sandboxes().await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.list_sandboxes().await,
		}
	}

	async fn get_scheduler_runners(
		&self,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<Vec<tg::runner::Id>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.get_scheduler_runners(scheduler).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.get_scheduler_runners(scheduler).await,
		}
	}

	async fn process_has_ancestor(
		&self,
		process: &tg::process::Id,
		ancestor: &tg::process::Id,
	) -> tg::Result<bool> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.process_has_ancestor(process, ancestor).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.process_has_ancestor(process, ancestor).await,
		}
	}

	async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::process::Process>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.touch_processes(ids, touched_at, time_to_touch).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.touch_processes(ids, touched_at, time_to_touch).await,
		}
	}

	async fn try_get_sandboxes(
		&self,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<Vec<Option<index::sandbox::Sandbox>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_sandboxes(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_sandboxes(ids).await,
		}
	}

	async fn try_get_runners(
		&self,
		ids: &[tg::runner::Id],
	) -> tg::Result<Vec<Option<index::runner::Runner>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_runners(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_runners(ids).await,
		}
	}

	async fn put_grants(&self, args: &[index::grant::put::Arg]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_grants(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_grants(args).await,
		}
	}

	async fn delete_grants(&self, args: &[index::grant::delete::Arg]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.delete_grants(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.delete_grants(args).await,
		}
	}

	async fn put_groups(&self, args: &[index::group::put::Arg]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_groups(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_groups(args).await,
		}
	}

	async fn delete_groups(&self, ids: &[tg::group::Id]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.delete_groups(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.delete_groups(ids).await,
		}
	}

	async fn put_group_members(&self, args: &[index::group::member::put::Arg]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_group_members(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_group_members(args).await,
		}
	}

	async fn delete_group_members(
		&self,
		args: &[index::group::member::delete::Arg],
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.delete_group_members(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.delete_group_members(args).await,
		}
	}

	async fn put_organizations(&self, args: &[index::organization::put::Arg]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_organizations(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_organizations(args).await,
		}
	}

	async fn delete_organizations(&self, ids: &[tg::organization::Id]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.delete_organizations(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.delete_organizations(ids).await,
		}
	}

	async fn put_organization_members(
		&self,
		args: &[index::organization::member::put::Arg],
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_organization_members(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_organization_members(args).await,
		}
	}

	async fn delete_organization_members(
		&self,
		args: &[index::organization::member::delete::Arg],
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.delete_organization_members(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.delete_organization_members(args).await,
		}
	}

	async fn put_tags(&self, args: &[index::tag::put::Arg]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_tags(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_tags(args).await,
		}
	}

	async fn delete_tags(&self, ids: &[tg::tag::Id]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.delete_tags(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.delete_tags(ids).await,
		}
	}

	async fn put_users(&self, args: &[index::user::put::Arg]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_users(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_users(args).await,
		}
	}

	async fn delete_users(&self, ids: &[tg::user::Id]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.delete_users(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.delete_users(ids).await,
		}
	}

	async fn complete_finalization(&self, entry: &index::finalization::Entry) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.complete_finalization(entry).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.complete_finalization(entry).await,
		}
	}

	async fn enqueue_finalization(&self, item: &index::finalization::Item) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.enqueue_finalization(item).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.enqueue_finalization(item).await,
		}
	}

	async fn finalization_batch(
		&self,
		kind: index::finalization::Kind,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<Vec<index::finalization::Entry>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => {
				index
					.finalization_batch(kind, batch_size, partition_start, partition_end)
					.await
			},
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => {
				index
					.finalization_batch(kind, batch_size, partition_start, partition_end)
					.await
			},
		}
	}

	async fn try_get_oldest_finalization_transaction_id(
		&self,
		kind: index::finalization::Kind,
	) -> tg::Result<Option<u64>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_oldest_finalization_transaction_id(kind).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_oldest_finalization_transaction_id(kind).await,
		}
	}

	async fn try_get_oldest_update_transaction_id(&self) -> tg::Result<Option<u64>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_oldest_update_transaction_id().await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_oldest_update_transaction_id().await,
		}
	}

	async fn update_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<index::update::Output> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => {
				index
					.update_batch(batch_size, partition_start, partition_end)
					.await
			},
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => {
				index
					.update_batch(batch_size, partition_start, partition_end)
					.await
			},
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

	fn partition_total(&self) -> u64 {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.partition_total(),
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.partition_total(),
		}
	}
}

impl Server {
	pub(crate) async fn index_batch(&self, arg: index::batch::Arg) -> tg::Result<()> {
		if arg.is_empty() {
			return Ok(());
		}
		if !self.config.advanced.single_process {
			let config = &self.config.object.outbox;
			let partition = rand::random_range(0..config.partition_total);
			let payload = arg.serialize()?.into();
			let arg = crate::object::outbox::EnqueueArg { partition, payload };
			self.object_store
				.enqueue_outbox(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to enqueue the index batch"))?;

			return Ok(());
		}
		self.index_tasks
			.spawn({
				let server = self.clone();
				|_| async move {
					if let Err(error) = server.index.batch(arg).await {
						tracing::error!(error = %error.trace(), "failed to index a batch");
					}
				}
			})
			.detach();

		Ok(())
	}
}

impl Session {
	pub(crate) async fn index(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + use<>> {
		if self.server.config.indexer.is_none() && self.server.config.advanced.single_process {
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
		let output = self
			.send_indexer_request(crate::indexer::RequestArg::Index)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the indexer request"))??;
		output
			.try_unwrap_index()
			.map_err(|_| tg::error!("expected an index response"))?;
		progress.finish("index");
		Ok(())
	}
}

impl Session {
	pub(crate) async fn index_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

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
