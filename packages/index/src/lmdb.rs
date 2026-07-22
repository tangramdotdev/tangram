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
mod finalization;
mod grant;
mod group;
mod key;
mod node;
mod object;
mod organization;
mod process;
mod request;
mod response;
mod runner;
mod sandbox;
mod tag;
mod task;
#[cfg(test)]
mod tests;
mod update;
mod user;
mod visible;

pub(super) use key::{Key, Kind};

#[derive(Clone, Debug)]
pub struct Config {
	pub authorize: AuthorizeConfig,
	pub map_size: usize,
	pub max_items_per_transaction: usize,
	pub max_process_depth: Option<u64>,
	pub path: PathBuf,
}

#[derive(Clone, Copy, Debug)]
pub struct AuthorizeConfig {
	pub object_subtree: crate::authorize::ObjectSubtreeConfig,
}

pub struct Index {
	config: AuthorizeConfig,
	db: Db,
	env: lmdb::Env,
	handle: Option<std::thread::JoinHandle<()>>,
	sender_high: Option<RequestSender>,
	sender_medium: Option<RequestSender>,
	sender_low: Option<RequestSender>,
	subspace: fdbt::Subspace,
}

#[derive(Clone, Copy)]
struct TaskArg<'a> {
	db: &'a Db,
	env: &'a lmdb::Env,
	max_items_per_transaction: usize,
	max_process_depth: Option<u64>,
	receiver_high: &'a RequestReceiver,
	receiver_low: &'a RequestReceiver,
	receiver_medium: &'a RequestReceiver,
	subspace: &'a fdbt::Subspace,
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
		let env = unsafe {
			lmdb::EnvOpenOptions::new()
				.map_size(config.map_size)
				.max_dbs(3)
				.max_readers(1_000)
				.flags(
					lmdb::EnvFlags::NO_SUB_DIR
						| lmdb::EnvFlags::WRITE_MAP
						| lmdb::EnvFlags::MAP_ASYNC,
				)
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

		let handle = std::thread::spawn({
			let env = env.clone();
			let subspace = subspace.clone();
			let max_items_per_transaction = config.max_items_per_transaction;
			let max_process_depth = config.max_process_depth;
			move || {
				Self::task(TaskArg {
					db: &db,
					env: &env,
					max_items_per_transaction,
					max_process_depth,
					receiver_high: &receiver_high,
					receiver_low: &receiver_low,
					receiver_medium: &receiver_medium,
					subspace: &subspace,
				});
			}
		});

		Ok(Self {
			config: config.authorize,
			db,
			env,
			handle: Some(handle),
			sender_high: Some(sender_high),
			sender_medium: Some(sender_medium),
			sender_low: Some(sender_low),
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

impl Drop for Index {
	fn drop(&mut self) {
		drop(self.sender_high.take());
		drop(self.sender_medium.take());
		drop(self.sender_low.take());
		if let Some(handle) = self.handle.take() {
			handle.join().ok();
		}
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

	async fn try_get_cached_processes(
		&self,
		command: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::Process)>> {
		self.try_get_cached_processes(command).await
	}

	async fn get_process_depth_detections(&self, limit: usize) -> tg::Result<Vec<tg::process::Id>> {
		self.get_process_depth_detections(limit).await
	}

	async fn get_requester_principals(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<Vec<tg::grant::Principal>> {
		self.get_requester_principals(principal).await
	}

	async fn list_sandboxes_for_creator(
		&self,
		creator: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>> {
		self.list_sandboxes_for_creator(creator).await
	}

	async fn list_sandboxes_for_owner(
		&self,
		owner: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>> {
		self.list_sandboxes_for_owner(owner).await
	}

	async fn get_runner_sandboxes(
		&self,
		runner: &tg::runner::Id,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		self.get_runner_sandboxes(runner).await
	}

	async fn get_sandbox_processes(
		&self,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::Process)>> {
		self.get_sandbox_processes(sandbox).await
	}

	async fn list_sandboxes(&self) -> tg::Result<Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>> {
		self.list_sandboxes().await
	}

	async fn get_scheduler_runners(
		&self,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<Vec<tg::runner::Id>> {
		self.get_scheduler_runners(scheduler).await
	}

	async fn process_has_ancestor(
		&self,
		process: &tg::process::Id,
		ancestor: &tg::process::Id,
	) -> tg::Result<bool> {
		self.process_has_ancestor(process, ancestor).await
	}

	async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		self.touch_processes(ids, touched_at, time_to_touch).await
	}

	async fn try_get_sandboxes(
		&self,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<Vec<Option<crate::sandbox::Sandbox>>> {
		self.try_get_sandboxes(ids).await
	}

	async fn try_get_runners(
		&self,
		ids: &[tg::runner::Id],
	) -> tg::Result<Vec<Option<crate::runner::Runner>>> {
		self.try_get_runners(ids).await
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

	async fn complete_finalization(&self, entry: &crate::finalization::Entry) -> tg::Result<()> {
		self.complete_finalization(entry).await
	}

	async fn enqueue_finalization(&self, item: &crate::finalization::Item) -> tg::Result<()> {
		self.enqueue_finalization(item).await
	}

	async fn finalization_batch(
		&self,
		kind: crate::finalization::Kind,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<Vec<crate::finalization::Entry>> {
		self.finalization_batch(kind, batch_size, partition_start, partition_count)
			.await
	}

	async fn try_get_oldest_finalization_transaction_id(
		&self,
		kind: crate::finalization::Kind,
	) -> tg::Result<Option<u64>> {
		self.try_get_oldest_finalization_transaction_id(kind).await
	}

	async fn try_get_oldest_update_transaction_id(&self) -> tg::Result<Option<u64>> {
		self.try_get_oldest_update_transaction_id().await
	}

	async fn update_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<usize> {
		Index::update_batch(self, batch_size, partition_start, partition_count).await
	}

	async fn clean(&self, arg: crate::clean::Arg) -> tg::Result<crate::clean::Output> {
		Index::clean(self, arg).await
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
