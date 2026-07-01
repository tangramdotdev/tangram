use {
	crate::{Server, Session},
	futures::{FutureExt as _, Stream, StreamExt as _, future, stream},
	num::ToPrimitive as _,
	std::{panic::AssertUnwindSafe, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::{self as index, prelude::*},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

const OBJECT_OUTBOX_PARTITION_COUNT: u64 = 256;

#[derive(serde::Deserialize, serde::Serialize)]
struct PayloadObject {
	cache_entry: Option<tg::artifact::Id>,
	children: std::collections::BTreeSet<tg::object::Id>,
	id: tg::object::Id,
	metadata: tg::object::Metadata,
	stored: tangram_index::object::Stored,
	touched_at: i64,
}

impl From<tangram_index::object::put::Arg> for PayloadObject {
	fn from(arg: tangram_index::object::put::Arg) -> Self {
		Self {
			cache_entry: arg.cache_entry,
			children: arg.children,
			id: arg.id,
			metadata: arg.metadata,
			stored: arg.stored,
			touched_at: arg.touched_at,
		}
	}
}

impl From<PayloadObject> for tangram_index::object::put::Arg {
	fn from(payload: PayloadObject) -> Self {
		Self {
			cache_entry: payload.cache_entry,
			children: payload.children,
			id: payload.id,
			metadata: payload.metadata,
			stored: payload.stored,
			touched_at: payload.touched_at,
		}
	}
}

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

	async fn updates_finished(&self, transaction_id: u64) -> tg::Result<bool> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.updates_finished(transaction_id).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.updates_finished(transaction_id).await,
		}
	}

	async fn update_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<usize> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => {
				index
					.update_batch(batch_size, partition_start, partition_count)
					.await
			},
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => {
				index
					.update_batch(batch_size, partition_start, partition_count)
					.await
			},
		}
	}

	async fn clean(
		&self,
		now: i64,
		max_object_touched_at: i64,
		max_process_touched_at: i64,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<index::clean::Output> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => {
				index
					.clean(
						now,
						max_object_touched_at,
						max_process_touched_at,
						batch_size,
						partition_start,
						partition_count,
					)
					.await
			},
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => {
				index
					.clean(
						now,
						max_object_touched_at,
						max_process_touched_at,
						batch_size,
						partition_start,
						partition_count,
					)
					.await
			},
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

impl Session {
	pub(crate) async fn index(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + use<>> {
		if !self.server.config.advanced.single_process {
			return Err(tg::error!("cannot index in multi-process mode"));
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
		// Subscribe to process finalizer progress.
		let wakeups = self
			.server
			.messenger
			.subscribe::<()>("processes.finalizer.progress".to_owned())
			.await
			.map_err(|error| tg::error!(!error, "failed to subscribe to finalizer progress"))?;
		let interval = IntervalStream::new(tokio::time::interval(Duration::from_secs(1))).skip(1);
		let mut wakeups = stream::select(wakeups.map(|_| ()), interval.map(|_| ()));

		// Wait for the existing finalize queue entries to be handled.
		if let Some(position) = self
			.server
			.try_get_process_finalize_queue_max_position()
			.await?
		{
			let mut remaining = self
				.server
				.get_process_finalize_queue_count_until_position(position)
				.await?;
			if remaining > 0 {
				let total = remaining;
				progress.start(
					"finalize".to_owned(),
					"finalize".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(0),
					Some(total),
				);
				while remaining > 0 {
					wakeups.next().await;
					let next_remaining = self
						.server
						.get_process_finalize_queue_count_until_position(position)
						.await?;
					progress.increment("finalize", remaining.saturating_sub(next_remaining));
					remaining = next_remaining;
				}
				progress.finish("finalize");
			}
		}

		// Wait for remote object put tasks to enqueue their index tasks.
		progress.spinner("tasks", "waiting for tasks");
		self.server.remote_object_put_tasks.wait().await;
		progress.finish("tasks");

		// Wait for outstanding index tasks to finish.
		progress.spinner("tasks", "waiting for tasks");
		self.server.index_tasks.wait().await;
		progress.finish("tasks");

		// Wait for the object outbox to drain all entries enqueued before now. This only applies to servers that run the outbox consumer.
		if self.server.config.object_outbox.is_some() {
			let before = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000)
				.to_i64()
				.unwrap();
			progress.spinner("outbox", "waiting for the object outbox");
			let partitions = (0..OBJECT_OUTBOX_PARTITION_COUNT)
				.map(|partition| partition.to_i64().unwrap())
				.collect::<Vec<_>>();
			while self
				.server
				.object_store
				.outbox_has_pending(partitions.clone(), before)
				.await?
			{
				tokio::time::sleep(Duration::from_millis(100)).await;
			}
			progress.finish("outbox");
		}

		// Subscribe to indexer progress.
		let wakeups = self
			.server
			.messenger
			.subscribe::<()>("indexer_progress".to_owned())
			.await
			.map_err(|error| tg::error!(!error, "failed to subscribe to indexer progress"))?;
		let interval = IntervalStream::new(tokio::time::interval(Duration::from_secs(1))).skip(1);
		let mut wakeups = stream::select(wakeups.map(|_| ()), interval.map(|_| ()));

		// Wait until the index no longer has updates whose transaction id is less than or equal to the current transaction id.
		let transaction_id = self
			.server
			.index
			.get_transaction_id()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the transaction id"))?;
		progress.spinner("updates", "waiting for index updates");
		loop {
			let finished = self
				.server
				.index
				.updates_finished(transaction_id)
				.await
				.map_err(|error| tg::error!(!error, "failed to check if updates are finished"))?;
			if finished {
				break;
			}
			wakeups.next().await;
		}
		progress.finish("updates");

		Ok::<_, tg::Error>(())
	}
}

impl Server {
	pub(crate) fn index_objects(
		&self,
		arg: tangram_index::batch::Arg,
	) -> tangram_futures::task::Shared<()> {
		self.index_tasks.spawn({
			let server = self.clone();
			|_| async move {
				server
					.index_objects_task(arg)
					.await
					.inspect_err(|error| {
						tracing::error!(error = %error.trace(), "failed to index the objects");
					})
					.ok();
			}
		})
	}

	pub(crate) async fn index_objects_task(
		&self,
		mut arg: tangram_index::batch::Arg,
	) -> tg::Result<()> {
		// If the object store does not support the durable outbox, index directly.
		if !self.object_store.supports_outbox() {
			return self.index.batch(arg).await;
		}

		// Take the objects out of the arg.
		let put_objects = std::mem::take(&mut arg.put_objects);

		// Partition the grants into those for an object being enqueued and the rest.
		let object_ids = put_objects
			.iter()
			.map(|object| tg::Id::from(object.id.clone()))
			.collect::<std::collections::HashSet<_>>();
		let mut grants = std::collections::HashMap::new();
		let mut other_grants = Vec::new();
		for grant in std::mem::take(&mut arg.put_grants) {
			if object_ids.contains(&grant.resource) {
				grants.insert(grant.resource.clone(), grant);
			} else {
				other_grants.push(grant);
			}
		}
		arg.put_grants = other_grants;

		// Build the outbox entries. Each object is enqueued as a row carrying its metadata payload and, when present, its grant payload.
		let mut entries = Vec::with_capacity(put_objects.len());
		for object in put_objects {
			let id = object.id.clone();
			let grant = grants
				.remove(&tg::Id::from(id.clone()))
				.map(|grant| serde_json::to_vec(&grant))
				.transpose()
				.map_err(|error| tg::error!(!error, "failed to serialize the grant payload"))?;
			let metadata = serde_json::to_vec(&PayloadObject::from(object))
				.map_err(|error| tg::error!(!error, "failed to serialize the metadata payload"))?;
			let partition =
				crate::object::put::partition_for_id(&id.to_bytes(), OBJECT_OUTBOX_PARTITION_COUNT)
					.to_i64()
					.unwrap();
			entries.push((partition, id, grant, Some(metadata)));
		}

		// Enqueue the objects to the outbox.
		self.object_store.enqueue_outbox_batch(entries).await?;

		// Index any remaining non-object args directly.
		if !arg.is_empty() {
			self.index.batch(arg).await?;
		}

		Ok(())
	}

	pub(crate) async fn indexer_task(&self, config: &crate::config::Indexer) -> tg::Result<()> {
		let partition_start = config.partition_start;
		let partition_count = config.partition_count;
		let concurrency = config.concurrency.to_u64().unwrap();
		loop {
			let futures = (0..config.concurrency).map(|task_index| {
				let task_index = task_index.to_u64().unwrap();
				let partitions_per_task = partition_count / concurrency;
				let extra = partition_count % concurrency;
				let task_start =
					partition_start + task_index * partitions_per_task + task_index.min(extra);
				let task_count = partitions_per_task + u64::from(task_index < extra);
				self.index
					.update_batch(config.batch_size, task_start, task_count)
			});
			let result = future::try_join_all(futures)
				.await
				.map(|counts| counts.into_iter().sum::<usize>());
			match result {
				Ok(0) => {
					tokio::time::sleep(Duration::from_millis(100)).await;
				},
				Ok(_) => {
					self.messenger
						.publish("indexer_progress".to_owned(), ())
						.await
						.ok();
				},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to index");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	pub(crate) async fn object_outbox_task(
		&self,
		config: &crate::config::ObjectOutbox,
	) -> tg::Result<()> {
		let concurrency = config.concurrency;
		loop {
			// Drain all partitions concurrently.
			let counts = stream::iter(0..OBJECT_OUTBOX_PARTITION_COUNT)
				.map(|partition| self.object_outbox_drain_partition(config.batch_size, partition))
				.buffer_unordered(concurrency)
				.collect::<Vec<_>>()
				.await;
			let result = counts.into_iter().sum::<tg::Result<usize>>();
			match result {
				Ok(0) => {
					tokio::time::sleep(Duration::from_millis(100)).await;
				},
				Ok(_) => {
					self.messenger
						.publish("indexer_progress".to_owned(), ())
						.await
						.ok();
				},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to drain the object outbox");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	async fn object_outbox_drain_partition(
		&self,
		batch_size: usize,
		partition: u64,
	) -> tg::Result<usize> {
		let partition = partition.to_i64().unwrap();
		let items = self
			.object_store
			.dequeue_outbox(partition, batch_size.to_i32().unwrap())
			.await?;
		if items.is_empty() {
			return Ok(0);
		}

		// Deserialize the payloads. Each item carries a metadata payload, a grant payload, or both.
		let mut put_objects = Vec::new();
		let mut put_grants = Vec::new();
		let mut tokens = Vec::with_capacity(items.len());
		for item in items {
			tokens.push(item.token);
			if let Some(metadata) = item.metadata {
				let object = serde_json::from_slice::<PayloadObject>(&metadata).map_err(
					|error| tg::error!(!error, id = %item.id, "failed to deserialize the metadata payload"),
				)?;
				put_objects.push(object.into());
			}
			if let Some(grant) = item.grant {
				let grant = serde_json::from_slice::<tangram_index::grant::put::Arg>(&grant)
					.map_err(
						|error| tg::error!(!error, id = %item.id, "failed to deserialize the grant payload"),
					)?;
				put_grants.push(grant);
			}
		}

		// Index the objects and grants.
		if !put_objects.is_empty() || !put_grants.is_empty() {
			let arg = tangram_index::batch::Arg {
				put_grants,
				put_objects,
				..Default::default()
			};
			self.index
				.batch(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the objects to the index"))?;
		}

		// Delete the drained entries from the outbox.
		self.object_store.delete_outbox(partition, &tokens).await?;

		Ok(tokens.len())
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
