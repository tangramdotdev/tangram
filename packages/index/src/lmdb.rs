use {
	self::{
		clean::ItemKind,
		request::{Clean, Request, TouchCacheEntries, TouchObjects, TouchProcesses, Update},
		response::Response,
	},
	crossbeam_channel as crossbeam, foundationdb_tuple as fdbt, heed as lmdb,
	std::path::PathBuf,
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
mod tag;
mod task;
mod update;
mod user;
mod visible;

pub(super) use key::{Key, Kind};

#[derive(Clone, Debug)]
pub struct Config {
	pub map_size: usize,
	pub max_items_per_transaction: usize,
	pub path: PathBuf,
}

pub struct Index {
	db: Db,
	env: lmdb::Env,
	sender_high: RequestSender,
	sender_medium: RequestSender,
	sender_low: RequestSender,
	subspace: fdbt::Subspace,
}

type Db = lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>;

type RequestSender = crossbeam::Sender<(Request, ResponseSender)>;
type RequestReceiver = crossbeam::Receiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;

impl Index {
	pub fn new(config: &Config) -> tg::Result<Self> {
		std::fs::OpenOptions::new()
			.create(true)
			.truncate(false)
			.read(true)
			.write(true)
			.open(&config.path)
			.map_err(
				|error| tg::error!(!error, path = %config.path.display(), "failed to open the lmdb file"),
			)?;
		let mut flags =
			lmdb::EnvFlags::NO_SUB_DIR | lmdb::EnvFlags::WRITE_MAP | lmdb::EnvFlags::MAP_ASYNC;
		if std::env::var_os("TANGRAM_MACOS_APP_SOCKET").is_some() {
			flags |= lmdb::EnvFlags::NO_LOCK;
		}
		let env = unsafe {
			lmdb::EnvOpenOptions::new()
				.map_size(config.map_size)
				.max_dbs(3)
				.max_readers(1_000)
				.flags(flags)
				.open(&config.path)
				.map_err(|error| {
					tg::error!(!error, path = %config.path.display(), "failed to open the lmdb environment")
				})?
		};
		let mut transaction = env.write_txn().unwrap();
		let db = env
			.create_database(&mut transaction, None)
			.map_err(|error| tg::error!(!error, "failed to create the database"))?;
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;

		let (sender_high, receiver_high) = crossbeam::bounded(256);
		let (sender_medium, receiver_medium) = crossbeam::bounded(256);
		let (sender_low, receiver_low) = crossbeam::bounded(256);

		let subspace = fdbt::Subspace::all();

		std::thread::spawn({
			let env = env.clone();
			let subspace = subspace.clone();
			let max_items_per_transaction = config.max_items_per_transaction;
			move || {
				Self::task(
					&env,
					&db,
					&subspace,
					&receiver_high,
					&receiver_medium,
					&receiver_low,
					max_items_per_transaction,
				);
			}
		});

		Ok(Self {
			db,
			env,
			sender_high,
			sender_medium,
			sender_low,
			subspace,
		})
	}

	fn pack<T: fdbt::TuplePack>(subspace: &fdbt::Subspace, key: &T) -> Vec<u8> {
		subspace.pack(key)
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
		let env = self.env.clone();
		tokio::task::spawn_blocking(move || {
			let transaction = env
				.read_txn()
				.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
			Ok(transaction.id() as u64)
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub async fn sync(&self) -> tg::Result<()> {
		tokio::task::spawn_blocking({
			let env = self.env.clone();
			move || {
				env.force_sync()
					.map_err(|error| tg::error!(!error, "failed to sync"))
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))??;
		Ok(())
	}
}

impl crate::Index for Index {
	async fn authorize_batch(
		&self,
		args: &[crate::authorize::Arg],
		principal: Option<&tg::Principal>,
	) -> tg::Result<Vec<Option<crate::authorize::Output>>> {
		self.authorize_batch(args, principal).await
	}

	async fn visible(
		&self,
		ids: &[tg::Id],
		principal: Option<&tg::Principal>,
	) -> tg::Result<Vec<bool>> {
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
		Index::update_batch(self, batch_size, partition_start, partition_count).await
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
		Index::clean(
			self,
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
		1
	}
}
