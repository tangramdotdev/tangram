use {
	self::{
		request::{Clean, Request, TouchCheckouts, TouchObjects, TouchProcesses, Update},
		response::Response,
	},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::sync::Arc,
	tangram_client::prelude::*,
};

mod ancestor;
mod authorize;
mod batch;
mod checkout;
mod clean;
mod error;
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
mod transaction;
mod update;
mod usage;
mod user;
mod visible;
mod writer;

pub(crate) use error::{propagate, retry};
pub(crate) use transaction::{Transaction, run};
pub(super) use {
	key::{Key, Kind},
	writer::Metrics,
};

pub struct Index {
	database: Arc<fdb::Database>,
	partition_total: u64,
	reader_sender: crate::read::Sender,
	subspace: fdbt::Subspace,
	usage_partition_total: u64,
	writer_sender_high: writer::RequestSender,
	writer_sender_low: writer::RequestSender,
	writer_sender_medium: writer::RequestSender,
}

pub struct Options {
	pub authorize: AuthorizeConfig,
	pub cluster: std::path::PathBuf,
	pub instance: Option<String>,
	pub max_process_depth: Option<u64>,
	pub partition_total: u64,
	pub read_request_batch_size: usize,
	pub read_transaction_concurrency: usize,
	pub usage_partition_total: u64,
	pub write_operation_batch_size: usize,
	pub write_transaction_concurrency: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct AuthorizeConfig {
	pub concurrency: usize,
	pub process_object_grant: crate::authorize::Config,
}

impl Index {
	pub fn new(options: &Options) -> tg::Result<Self> {
		Self::validate_options(options)?;

		let database = fdb::Database::new(Some(options.cluster.to_str().unwrap()))
			.map_err(|error| tg::error!(!error, "failed to open the foundationdb cluster"))?;
		let database = Arc::new(database);

		let subspace = match &options.instance {
			Some(instance) => fdbt::Subspace::from_bytes(instance.clone().into_bytes()),
			None => fdbt::Subspace::all(),
		};

		let partition_total = options.partition_total;
		let usage_partition_total = options.usage_partition_total;

		let metrics = Metrics::new();

		let (writer_sender_high, writer_receiver_high) = tokio::sync::mpsc::unbounded_channel();
		let (writer_sender_medium, writer_receiver_medium) = tokio::sync::mpsc::unbounded_channel();
		let (writer_sender_low, writer_receiver_low) = tokio::sync::mpsc::unbounded_channel();

		// Spawn the reader task.
		let (reader_sender, reader_receiver) =
			tokio::sync::mpsc::channel(crate::read::CHANNEL_CAPACITY);
		tokio::spawn({
			let database = database.clone();
			let subspace = subspace.clone();
			let authorize_concurrency = options.authorize.concurrency;
			let read_request_batch_size = options.read_request_batch_size;
			let read_transaction_concurrency = options.read_transaction_concurrency;
			async move {
				Self::reader_task(reader::Arg {
					authorize_concurrency,
					database,
					partition_total,
					read_request_batch_size,
					read_transaction_concurrency,
					receiver: reader_receiver,
					subspace,
				})
				.await;
			}
		});

		// Spawn the writer task.
		let authorize = options.authorize;
		let max_process_depth = options.max_process_depth;
		let write_operation_batch_size = options.write_operation_batch_size;
		let write_transaction_concurrency = options.write_transaction_concurrency;
		tokio::spawn({
			let database = database.clone();
			let metrics = metrics.clone();
			let subspace = subspace.clone();
			async move {
				let arg = writer::Arg {
					authorize,
					database,
					max_process_depth,
					metrics,
					partition_total,
					receiver_high: writer_receiver_high,
					receiver_low: writer_receiver_low,
					receiver_medium: writer_receiver_medium,
					subspace,
					usage_partition_total,
					write_operation_batch_size,
					write_transaction_concurrency,
				};
				Self::writer_task(arg).await;
			}
		});

		let index = Self {
			database,
			partition_total,
			reader_sender,
			subspace,
			usage_partition_total,
			writer_sender_high,
			writer_sender_low,
			writer_sender_medium,
		};

		Ok(index)
	}

	fn validate_options(options: &Options) -> tg::Result<()> {
		options.authorize.process_object_grant.validate()?;
		if options.authorize.concurrency == 0 {
			return Err(tg::error!(
				"the FDB index authorization concurrency must be greater than zero"
			));
		}
		if options.partition_total == 0 {
			return Err(tg::error!(
				"the FDB index partition total must be greater than zero"
			));
		}
		if options.read_request_batch_size == 0 {
			return Err(tg::error!(
				"the FDB index read request batch size must be greater than zero"
			));
		}
		if options.read_transaction_concurrency == 0 {
			return Err(tg::error!(
				"the FDB index read transaction concurrency must be greater than zero"
			));
		}
		if options.usage_partition_total == 0 {
			return Err(tg::error!(
				"the FDB index usage partition total must be greater than zero"
			));
		}
		if options.write_operation_batch_size == 0 {
			return Err(tg::error!(
				"the FDB index write operation batch size must be greater than zero"
			));
		}
		if options.write_transaction_concurrency == 0 {
			return Err(tg::error!(
				"the FDB index write transaction concurrency must be greater than zero"
			));
		}

		Ok(())
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
		let response = self
			.send_read_request(crate::read::Request::GetTransactionId)
			.await?;
		let crate::read::Response::GetTransactionId(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn sync(&self) -> tg::Result<()> {
		Ok(())
	}

	#[must_use]
	pub fn usage_partition_total(&self) -> u64 {
		self.usage_partition_total
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
		config: crate::authorize::Config,
		principal: &tg::Principal,
	) -> tg::Result<Vec<crate::authorize::Outcome>> {
		self.authorize_batch(args, config, principal).await
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
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<Vec<crate::log::Entry>> {
		self.log_compaction_batch(batch_size, partition_start, partition_end)
			.await
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
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<crate::update::Output> {
		self.update_batch(kind, batch_size, partition_start, partition_end)
			.await
	}

	async fn clean(&self, arg: crate::clean::Arg) -> tg::Result<crate::clean::Output> {
		self.clean(arg).await
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
