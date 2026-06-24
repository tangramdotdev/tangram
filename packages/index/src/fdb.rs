use {
	self::{
		clean::ItemKind,
		request::{Clean, Request, TouchCacheEntries, TouchObjects, TouchProcesses, Update},
		response::Response,
	},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::sync::Arc,
	tangram_client::prelude::*,
};

mod authorize;
mod batch;
mod cache;
mod clean;
mod grant;
mod group;
mod key;
mod node;
mod object;
mod organization;
mod process;
mod request;
mod response;
mod sandbox;
mod tag;
mod task;
mod update;
mod user;
mod visible;

pub(super) use {
	key::{Key, Kind},
	task::Metrics,
};

pub struct Index {
	config: AuthorizeConfig,
	database: Arc<fdb::Database>,
	partition_total: u64,
	subspace: fdbt::Subspace,
	sender_high: RequestSender,
	sender_medium: RequestSender,
	sender_low: RequestSender,
}

pub struct Options {
	pub authorize: AuthorizeConfig,
	pub cluster: std::path::PathBuf,
	pub concurrency: usize,
	pub max_items_per_transaction: usize,
	pub partition_total: u64,
	pub prefix: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub struct AuthorizeConfig {
	pub concurrency: usize,
	pub object_subtree: crate::authorize::ObjectSubtreeConfig,
}

struct TaskArg {
	database: Arc<fdb::Database>,
	subspace: fdbt::Subspace,
	receiver_high: RequestReceiver,
	receiver_medium: RequestReceiver,
	receiver_low: RequestReceiver,
	concurrency: usize,
	max_items_per_transaction: usize,
	partition_total: u64,
	metrics: Metrics,
}

type RequestSender = tokio::sync::mpsc::UnboundedSender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::UnboundedReceiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;

impl Index {
	pub fn new(options: &Options) -> tg::Result<Self> {
		let database = fdb::Database::new(Some(options.cluster.to_str().unwrap()))
			.map_err(|error| tg::error!(!error, "failed to open the foundationdb cluster"))?;
		let database = Arc::new(database);

		let subspace = match &options.prefix {
			Some(s) => fdbt::Subspace::from_bytes(s.clone().into_bytes()),
			None => fdbt::Subspace::all(),
		};

		let partition_total = options.partition_total;
		let config = AuthorizeConfig {
			concurrency: options.authorize.concurrency.max(1),
			object_subtree: options.authorize.object_subtree,
		};

		let metrics = Metrics::new();

		let (sender_high, receiver_high) = tokio::sync::mpsc::unbounded_channel();
		let (sender_medium, receiver_medium) = tokio::sync::mpsc::unbounded_channel();
		let (sender_low, receiver_low) = tokio::sync::mpsc::unbounded_channel();

		let concurrency = options.concurrency;
		let max_items_per_transaction = options.max_items_per_transaction;
		tokio::spawn({
			let database = database.clone();
			let subspace = subspace.clone();
			let metrics = metrics.clone();
			async move {
				let arg = TaskArg {
					database,
					subspace,
					receiver_high,
					receiver_medium,
					receiver_low,
					concurrency,
					max_items_per_transaction,
					partition_total,
					metrics,
				};
				Self::task(arg).await;
			}
		});

		let index = Self {
			config,
			database,
			partition_total,
			subspace,
			sender_high,
			sender_medium,
			sender_low,
		};

		Ok(index)
	}

	fn partition_for_id(id_bytes: &[u8], partition_total: u64) -> u64 {
		let len = id_bytes.len();
		let start = len.saturating_sub(8);
		let mut bytes = [0u8; 8];
		bytes[8 - (len - start)..].copy_from_slice(&id_bytes[start..]);
		u64::from_be_bytes(bytes) % partition_total
	}

	fn pack<T: fdbt::TuplePack>(subspace: &fdbt::Subspace, key: &T) -> Vec<u8> {
		subspace.pack(key)
	}

	fn pack_with_versionstamp<T: fdbt::TuplePack>(subspace: &fdbt::Subspace, key: &T) -> Vec<u8> {
		subspace.pack_with_versionstamp(key)
	}

	fn unpack<'a, T: fdbt::TupleUnpack<'a>>(
		subspace: &fdbt::Subspace,
		bytes: &'a [u8],
	) -> tg::Result<T> {
		subspace
			.unpack(bytes)
			.map_err(|error| tg::error!(!error, "failed to unpack key"))
	}

	pub async fn get_transaction_id(&self) -> tg::Result<u64> {
		let txn = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;
		let version = txn
			.get_read_version()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the read version"))?
			.cast_unsigned();
		Ok(version)
	}

	pub async fn sync(&self) -> tg::Result<()> {
		Ok(())
	}
}

impl crate::Index for Index {
	async fn authorize_batch(
		&self,
		args: &[crate::authorize::Arg],
		principal: &tg::Principal,
	) -> tg::Result<Vec<Option<crate::authorize::Output>>> {
		self.authorize_batch(args, principal).await
	}

	async fn visible(&self, ids: &[tg::Id], principal: &tg::Principal) -> tg::Result<Vec<bool>> {
		self.visible(ids, principal).await
	}

	async fn batch(&self, arg: crate::batch::Arg) -> tg::Result<()> {
		self.batch(arg).await
	}

	async fn try_get_cache_entries(
		&self,
		ids: &[tg::artifact::Id],
	) -> tg::Result<Vec<Option<crate::cache::Entry>>> {
		self.try_get_cache_entries(ids).await
	}

	async fn touch_cache_entries(
		&self,
		ids: &[tg::artifact::Id],
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<Vec<Option<crate::cache::Entry>>> {
		self.touch_cache_entries(ids, touched_at, time_to_touch)
			.await
	}

	async fn try_get_objects(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<crate::object::Object>>> {
		self.try_get_objects(ids).await
	}

	async fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<Vec<Option<crate::object::Object>>> {
		self.touch_objects(ids, touched_at, time_to_touch).await
	}

	async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		self.try_get_processes(ids).await
	}

	async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		self.touch_processes(ids, touched_at, time_to_touch).await
	}

	async fn put_grants(&self, args: &[crate::grant::put::Arg]) -> tg::Result<()> {
		self.put_grants(args).await
	}

	async fn delete_grants(&self, args: &[crate::grant::delete::Arg]) -> tg::Result<()> {
		self.delete_grants(args).await
	}

	async fn put_groups(&self, args: &[crate::group::put::Arg]) -> tg::Result<()> {
		self.put_groups(args).await
	}

	async fn delete_groups(&self, ids: &[tg::group::Id]) -> tg::Result<()> {
		self.delete_groups(ids).await
	}

	async fn put_group_members(&self, args: &[crate::group::member::put::Arg]) -> tg::Result<()> {
		self.put_group_members(args).await
	}

	async fn delete_group_members(
		&self,
		args: &[crate::group::member::delete::Arg],
	) -> tg::Result<()> {
		self.delete_group_members(args).await
	}

	async fn put_organizations(&self, args: &[crate::organization::put::Arg]) -> tg::Result<()> {
		self.put_organizations(args).await
	}

	async fn delete_organizations(&self, ids: &[tg::organization::Id]) -> tg::Result<()> {
		self.delete_organizations(ids).await
	}

	async fn put_organization_members(
		&self,
		args: &[crate::organization::member::put::Arg],
	) -> tg::Result<()> {
		self.put_organization_members(args).await
	}

	async fn delete_organization_members(
		&self,
		args: &[crate::organization::member::delete::Arg],
	) -> tg::Result<()> {
		self.delete_organization_members(args).await
	}

	async fn put_tags(&self, args: &[crate::tag::put::Arg]) -> tg::Result<()> {
		self.put_tags(args).await
	}

	async fn delete_tags(&self, ids: &[tg::tag::Id]) -> tg::Result<()> {
		self.delete_tags(ids).await
	}

	async fn put_users(&self, args: &[crate::user::put::Arg]) -> tg::Result<()> {
		self.put_users(args).await
	}

	async fn delete_users(&self, ids: &[tg::user::Id]) -> tg::Result<()> {
		self.delete_users(ids).await
	}

	async fn updates_finished(&self, transaction_id: u64) -> tg::Result<bool> {
		self.updates_finished(transaction_id).await
	}

	async fn update_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<usize> {
		self.update_batch(batch_size, partition_start, partition_count)
			.await
	}

	async fn clean(
		&self,
		now: i64,
		max_object_touched_at: i64,
		max_process_touched_at: i64,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<crate::clean::Output> {
		self.clean(
			now,
			max_object_touched_at,
			max_process_touched_at,
			batch_size,
			partition_start,
			partition_count,
		)
		.await
	}

	async fn get_transaction_id(&self) -> tg::Result<u64> {
		self.get_transaction_id().await
	}

	async fn sync(&self) -> tg::Result<()> {
		self.sync().await
	}

	fn partition_total(&self) -> u64 {
		self.partition_total
	}
}
