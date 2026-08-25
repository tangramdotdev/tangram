use {
	self::{
		request::{Clean, Request, TouchCheckouts, TouchObjects, TouchProcesses, Update},
		response::Response,
	},
	crossbeam_channel as crossbeam, foundationdb_tuple as fdbt, heed as lmdb,
	std::{path::PathBuf, sync::Arc},
	tangram_client::prelude::*,
};

mod ancestor;
mod authorize;
mod batch;
mod checkout;
mod clean;
mod grant;
mod group;
mod key;
mod log;
mod node;
mod object;
mod organization;
mod process;
mod reader;
mod request;
mod response;
mod runner;
mod sandbox;
mod tag;
#[cfg(test)]
mod tests;
mod update;
mod usage;
mod user;
mod visible;
mod writer;

pub(super) use key::{Key, Kind};

#[derive(Clone, Debug)]
pub struct Config {
	pub authorize: AuthorizeConfig,
	pub map_size: usize,
	pub max_process_depth: Option<u64>,
	pub path: PathBuf,
	pub read_request_batch_size: usize,
	pub read_transaction_concurrency: usize,
	pub usage_partition_total: u64,
	pub write_operation_batch_size: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct AuthorizeConfig {
	pub object_subtree: crate::authorize::ObjectSubtreeConfig,
	pub process_subtree: crate::authorize::ProcessSubtreeConfig,
}

pub struct Index {
	#[cfg_attr(not(test), allow(dead_code))]
	db: Db,
	env: lmdb::Env,
	reader_handles: Vec<std::thread::JoinHandle<()>>,
	reader_sender: Option<crate::read::Sender>,
	#[cfg_attr(not(test), allow(dead_code))]
	subspace: fdbt::Subspace,
	usage_partition_total: u64,
	writer_handle: Option<std::thread::JoinHandle<()>>,
	writer_sender_high: Option<writer::RequestSender>,
	writer_sender_low: Option<writer::RequestSender>,
	writer_sender_medium: Option<writer::RequestSender>,
}

type Db = lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>;

impl Index {
	pub fn new(config: &Config) -> tg::Result<Self> {
		Self::validate_config(config)?;

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

		let (writer_sender_high, writer_receiver_high) =
			crossbeam::bounded(writer::CHANNEL_CAPACITY);
		let (writer_sender_medium, writer_receiver_medium) =
			crossbeam::bounded(writer::CHANNEL_CAPACITY);
		let (writer_sender_low, writer_receiver_low) = crossbeam::bounded(writer::CHANNEL_CAPACITY);

		let subspace = fdbt::Subspace::all();

		// Spawn the reader tasks.
		let (reader_sender, reader_receiver) =
			tokio::sync::mpsc::channel(crate::read::CHANNEL_CAPACITY);
		let reader_receiver = Arc::new(std::sync::Mutex::new(reader_receiver));
		let reader_handles = (0..config.read_transaction_concurrency)
			.map(|_| {
				let env = env.clone();
				let reader_receiver = reader_receiver.clone();
				let subspace = subspace.clone();
				let authorize = config.authorize;
				let read_request_batch_size = config.read_request_batch_size;
				std::thread::spawn(move || {
					Self::reader_task(&reader::Arg {
						authorize,
						db,
						env,
						read_request_batch_size,
						receiver: reader_receiver,
						subspace,
						#[cfg(test)]
						test_hook: None,
					});
				})
			})
			.collect();

		// Spawn the writer task.
		let writer_handle = std::thread::spawn({
			let authorize = config.authorize;
			let env = env.clone();
			let subspace = subspace.clone();
			let max_process_depth = config.max_process_depth;
			let usage_partition_total = config.usage_partition_total;
			let write_operation_batch_size = config.write_operation_batch_size;
			move || {
				Self::writer_task(writer::Arg {
					authorize,
					db: &db,
					env: &env,
					max_process_depth,
					receiver_high: &writer_receiver_high,
					receiver_low: &writer_receiver_low,
					receiver_medium: &writer_receiver_medium,
					subspace: &subspace,
					usage_partition_total,
					write_operation_batch_size,
				});
			}
		});

		Ok(Self {
			db,
			env,
			reader_handles,
			reader_sender: Some(reader_sender),
			subspace,
			usage_partition_total: config.usage_partition_total,
			writer_handle: Some(writer_handle),
			writer_sender_high: Some(writer_sender_high),
			writer_sender_low: Some(writer_sender_low),
			writer_sender_medium: Some(writer_sender_medium),
		})
	}

	fn validate_config(config: &Config) -> tg::Result<()> {
		if config.read_request_batch_size == 0 {
			return Err(tg::error!(
				"the LMDB index read request batch size must be greater than zero"
			));
		}
		if config.read_transaction_concurrency == 0 {
			return Err(tg::error!(
				"the LMDB index read transaction concurrency must be greater than zero"
			));
		}
		if config.usage_partition_total == 0 {
			return Err(tg::error!(
				"the LMDB index usage partition total must be greater than zero"
			));
		}
		if config.write_operation_batch_size == 0 {
			return Err(tg::error!(
				"the LMDB index write operation batch size must be greater than zero"
			));
		}

		Ok(())
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
		let response = self
			.send_read_request(crate::read::Request::GetTransactionId)
			.await?;
		let crate::read::Response::GetTransactionId(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
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

	#[must_use]
	pub fn usage_partition_total(&self) -> u64 {
		self.usage_partition_total
	}
}

impl Drop for Index {
	fn drop(&mut self) {
		drop(self.reader_sender.take());
		drop(self.writer_sender_high.take());
		drop(self.writer_sender_low.take());
		drop(self.writer_sender_medium.take());
		for handle in self.reader_handles.drain(..) {
			handle.join().ok();
		}
		if let Some(handle) = self.writer_handle.take() {
			handle.join().ok();
		}
	}
}

impl crate::Index for Index {
	async fn get_usage(
		&self,
		account: &crate::usage::Account,
		period: crate::usage::Period,
		now: jiff::Timestamp,
	) -> tg::Result<crate::usage::Aggregate> {
		self.get_usage(account, period, now).await
	}

	async fn start_usage(&self, at: jiff::Timestamp) -> tg::Result<()> {
		self.start_usage(at).await
	}

	async fn authorize_batch(
		&self,
		args: &[crate::authorize::Arg],
		principal: &tg::Principal,
	) -> tg::Result<Vec<Option<crate::authorize::Output>>> {
		self.authorize_batch(args, principal).await
	}

	async fn contains_ids(&self, ids: &[tg::Id]) -> tg::Result<Vec<bool>> {
		self.contains_ids(ids).await
	}

	async fn clean_usage(
		&self,
		arg: crate::usage::clean::Arg,
	) -> tg::Result<crate::usage::clean::Output> {
		self.clean_usage(arg).await
	}

	fn usage_partition_total(&self) -> u64 {
		self.usage_partition_total()
	}

	async fn aggregate_usage(
		&self,
		arg: crate::usage::aggregate::Arg,
	) -> tg::Result<crate::usage::aggregate::Output> {
		self.aggregate_usage(arg).await
	}

	async fn visible(&self, ids: &[tg::Id], principal: &tg::Principal) -> tg::Result<Vec<bool>> {
		self.visible(ids, principal).await
	}

	async fn batch(&self, arg: crate::batch::Arg) -> tg::Result<()> {
		self.batch(arg).await
	}

	async fn try_get_ancestors(&self, id: &tg::Id) -> tg::Result<Option<Vec<tg::Id>>> {
		self.try_get_ancestors(id).await
	}

	async fn try_get_checkouts(
		&self,
		ids: &[tg::Id],
	) -> tg::Result<Vec<Option<crate::checkout::Checkout>>> {
		self.try_get_checkouts(ids).await
	}

	async fn try_get_groups(
		&self,
		ids: &[tg::group::Id],
	) -> tg::Result<Vec<Option<crate::group::Group>>> {
		self.try_get_groups(ids).await
	}

	async fn try_get_ids_for_specifiers(
		&self,
		specifiers: &[tg::Specifier],
	) -> tg::Result<Vec<Option<tg::Id>>> {
		self.try_get_ids_for_specifiers(specifiers).await
	}

	async fn try_get_organizations(
		&self,
		ids: &[tg::organization::Id],
	) -> tg::Result<Vec<Option<crate::organization::Organization>>> {
		self.try_get_organizations(ids).await
	}

	async fn touch_checkouts(
		&self,
		ids: &[tg::Id],
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<Vec<Option<crate::checkout::Checkout>>> {
		self.touch_checkouts(ids, touched_at, time_to_touch).await
	}

	async fn try_get_object_children(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<Vec<tg::object::Id>>> {
		self.try_get_object_children(id).await
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

	async fn touch_objects_with_account(
		&self,
		ids: &[tg::object::Id],
		account: Option<&crate::usage::Account>,
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<Vec<Option<crate::object::Object>>> {
		self.touch_objects_with_account(ids, account, touched_at, time_to_touch)
			.await
	}

	async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		self.try_get_processes(ids).await
	}

	async fn try_get_process_children(
		&self,
		id: &tg::process::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<Option<Vec<tg::process::data::Child>>> {
		self.try_get_process_children(id, position, length).await
	}

	async fn try_get_process_node_children(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<crate::process::NodeChildren>> {
		self.try_get_process_node_children(id).await
	}

	async fn try_get_cached_processes(
		&self,
		command: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::Process)>> {
		self.try_get_cached_processes(command).await
	}

	async fn get_requester_subjects(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<Vec<tg::authorization::Subject>> {
		self.get_requester_subjects(principal).await
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

	async fn touch_processes_and_put_account(
		&self,
		ids: &[tg::process::Id],
		account: &crate::usage::Account,
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		self.touch_processes_and_put_account(ids, account, touched_at, time_to_touch)
			.await
	}

	async fn touch_processes_with_account(
		&self,
		ids: &[tg::process::Id],
		account: Option<&crate::usage::Account>,
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		self.touch_processes_with_account(ids, account, touched_at, time_to_touch)
			.await
	}

	async fn try_get_sandboxes(
		&self,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<Vec<Option<crate::sandbox::Sandbox>>> {
		self.try_get_sandboxes(ids).await
	}

	async fn try_get_specifiers_for_ids(
		&self,
		ids: &[tg::Id],
	) -> tg::Result<Vec<Option<tg::Specifier>>> {
		self.try_get_specifiers_for_ids(ids).await
	}

	async fn try_get_tags(&self, ids: &[tg::tag::Id]) -> tg::Result<Vec<Option<crate::tag::Tag>>> {
		self.try_get_tags(ids).await
	}

	async fn try_get_users(
		&self,
		ids: &[tg::user::Id],
	) -> tg::Result<Vec<Option<crate::user::User>>> {
		self.try_get_users(ids).await
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

	async fn complete_log_compaction(&self, entry: &crate::log::Entry) -> tg::Result<()> {
		self.complete_log_compaction(entry).await
	}

	async fn enqueue_log_compaction(&self, process: &tg::process::Id) -> tg::Result<()> {
		self.enqueue_log_compaction(process).await
	}

	async fn log_compaction_batch(
		&self,
		batch_size: usize,
		_partition_start: u64,
		_partition_end: u64,
	) -> tg::Result<Vec<crate::log::Entry>> {
		Index::log_compaction_batch(self, batch_size).await
	}

	async fn try_get_oldest_log_compaction_transaction_id(&self) -> tg::Result<Option<u64>> {
		self.try_get_oldest_log_compaction_transaction_id().await
	}

	async fn try_get_oldest_update_transaction_id(
		&self,
		kind: crate::update::Kind,
	) -> tg::Result<Option<u64>> {
		self.try_get_oldest_update_transaction_id(kind).await
	}

	async fn update_batch(
		&self,
		kind: crate::update::Kind,
		batch_size: usize,
		_partition_start: u64,
		_partition_end: u64,
	) -> tg::Result<crate::update::Output> {
		Index::update_batch(self, kind, batch_size).await
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
