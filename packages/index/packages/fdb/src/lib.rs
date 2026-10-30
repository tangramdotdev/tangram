use {
	crate::{
		request::{Clean, Request, TouchCheckouts, TouchObjects, TouchProcesses, Update},
		response::Response,
	},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::sync::Arc,
	tangram_client::prelude::*,
};

mod ancestor;
mod batch;
mod checkout;
mod clean;
mod delegation;
mod error;
mod group;
mod indexer;
mod key;
mod node;
mod object;
mod organization;
mod page;
mod permission;
mod process;
mod reader;
mod request;
mod response;
mod runner;
mod sandbox;
mod sync;
mod tag;
#[cfg(test)]
mod tests;
mod transaction;
mod update;
mod usage;
mod user;
mod verify;
mod visible;
mod writer;

pub(crate) use error::{propagate, retry};
pub(crate) use transaction::run;
pub(crate) use {
	key::{Key, Kind},
	writer::Metrics,
};

pub struct Index {
	database: Arc<fdb::Database>,
	partition_totals: PartitionTotals,
	reader_sender: tangram_index::read::Sender,
	subspace: fdbt::Subspace,
	writer_sender_high: writer::RequestSender,
	writer_sender_low: writer::RequestSender,
	writer_sender_medium: writer::RequestSender,
}

pub struct Options {
	pub cleaning_partition_total: u64,
	pub cluster: std::path::PathBuf,
	pub permission_update_partition_total: u64,
	pub instance: Option<String>,
	pub max_process_depth: Option<u64>,
	pub object_permission_time_to_touch: std::time::Duration,
	pub process_data_fragment_size: usize,
	pub process_permission_time_to_touch: std::time::Duration,
	pub storage_and_metadata_update_partition_total: u64,
	pub read_channel_capacity: usize,
	pub read_request_batch_size: usize,
	/// The maximum number of concurrent read transactions per reader task.
	pub read_task_concurrency: usize,
	pub read_task_count: usize,
	pub storage_relation_batch_size: usize,
	pub usage_update_partition_total: u64,
	pub usage_partition_total: u64,
	pub max_write_operation_batch_size: usize,
	pub verification: VerificationConfig,
	pub write_operation_batch_size: usize,
	pub write_transaction_concurrency: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct VerificationConfig {
	pub concurrency: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct PartitionTotals {
	pub cleaning: u64,
	pub permission_update: u64,
	pub storage_and_metadata_update: u64,
	pub usage_update: u64,
	pub usage: u64,
}

impl PartitionTotals {
	#[must_use]
	fn update(self, kind: tangram_index::update::Kind) -> u64 {
		match kind {
			tangram_index::update::Kind::Permission => self.permission_update,
			tangram_index::update::Kind::StorageAndMetadata => self.storage_and_metadata_update,
			tangram_index::update::Kind::Usage => self.usage_update,
		}
	}
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

		let partition_totals = PartitionTotals {
			cleaning: options.cleaning_partition_total,
			permission_update: options.permission_update_partition_total,
			storage_and_metadata_update: options.storage_and_metadata_update_partition_total,
			usage_update: options.usage_update_partition_total,
			usage: options.usage_partition_total,
		};

		let metrics = Metrics::new();

		let (writer_sender_high, writer_receiver_high) = tokio::sync::mpsc::unbounded_channel();
		let (writer_sender_medium, writer_receiver_medium) = tokio::sync::mpsc::unbounded_channel();
		let (writer_sender_low, writer_receiver_low) = tokio::sync::mpsc::unbounded_channel();

		// Spawn the reader task.
		let (reader_sender, reader_receiver) =
			tokio::sync::mpsc::channel(options.read_channel_capacity);
		tokio::spawn({
			let database = database.clone();
			let subspace = subspace.clone();
			let verification_concurrency = options.verification.concurrency;
			let read_request_batch_size = options.read_request_batch_size;
			let read_task_concurrency = options.read_task_concurrency;
			let read_task_count = options.read_task_count;
			async move {
				let arg = reader::Arg {
					database,
					partition_totals,
					read_request_batch_size,
					read_task_concurrency,
					read_task_count,
					receiver: reader_receiver,
					subspace,
					verification_concurrency,
				};
				Self::reader_task(arg).await;
			}
		});

		// Spawn the writer task.
		let max_process_depth = options.max_process_depth;
		let max_write_operation_batch_size = options.max_write_operation_batch_size;
		let object_permission_time_to_touch = options.object_permission_time_to_touch;
		let process_data_fragment_size = options.process_data_fragment_size;
		let process_permission_time_to_touch = options.process_permission_time_to_touch;
		let storage_relation_batch_size = options.storage_relation_batch_size;
		let write_operation_batch_size = options.write_operation_batch_size;
		let write_transaction_concurrency = options.write_transaction_concurrency;
		tokio::spawn({
			let database = database.clone();
			let metrics = metrics.clone();
			let subspace = subspace.clone();
			async move {
				let arg = writer::Arg {
					database,
					max_process_depth,
					max_write_operation_batch_size,
					metrics,
					object_permission_time_to_touch,
					partition_totals,
					process_data_fragment_size,
					process_permission_time_to_touch,
					receiver_high: writer_receiver_high,
					receiver_low: writer_receiver_low,
					receiver_medium: writer_receiver_medium,
					storage_relation_batch_size,
					subspace,
					write_operation_batch_size,
					write_transaction_concurrency,
				};
				Self::writer_task(arg).await;
			}
		});

		let index = Self {
			database,
			partition_totals,
			reader_sender,
			subspace,
			writer_sender_high,
			writer_sender_low,
			writer_sender_medium,
		};

		Ok(index)
	}

	fn validate_options(options: &Options) -> tg::Result<()> {
		if options.verification.concurrency == 0 {
			return Err(tg::error!(
				"the FDB index verification concurrency must be greater than zero"
			));
		}
		for (name, partition_total) in [
			("cleaning", options.cleaning_partition_total),
			(
				"permission update",
				options.permission_update_partition_total,
			),
			(
				"storage and metadata update",
				options.storage_and_metadata_update_partition_total,
			),
			("usage update", options.usage_update_partition_total),
			("usage", options.usage_partition_total),
		] {
			if partition_total == 0 {
				return Err(tg::error!(
					"the FDB index {name} partition total must be greater than zero"
				));
			}
		}
		if options.process_data_fragment_size == 0 {
			return Err(tg::error!(
				"the FDB index process data fragment size must be greater than zero"
			));
		}
		if options.read_channel_capacity == 0 {
			return Err(tg::error!(
				"the FDB index read channel capacity must be greater than zero"
			));
		}
		if options.read_request_batch_size == 0 {
			return Err(tg::error!(
				"the FDB index read request batch size must be greater than zero"
			));
		}
		if options.storage_relation_batch_size == 0 {
			return Err(tg::error!(
				"the FDB index storage relation batch size must be greater than zero"
			));
		}
		if options.read_task_count == 0 {
			return Err(tg::error!(
				"the FDB index read task count must be greater than zero"
			));
		}
		if options.read_task_concurrency == 0 {
			return Err(tg::error!(
				"the FDB index read task concurrency must be greater than zero"
			));
		}
		if options.max_write_operation_batch_size == 0 {
			return Err(tg::error!(
				"the FDB index max write operation batch size must be greater than zero"
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
			.send_read_request(tangram_index::read::Request::GetTransactionId)
			.await?;
		let tangram_index::read::Response::GetTransactionId(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn sync(&self) -> tg::Result<()> {
		Ok(())
	}

	#[must_use]
	pub fn cleaning_partition_total(&self) -> u64 {
		self.partition_totals.cleaning
	}

	#[must_use]
	pub fn permission_update_partition_total(&self) -> u64 {
		self.partition_totals.permission_update
	}

	#[must_use]
	pub fn storage_and_metadata_update_partition_total(&self) -> u64 {
		self.partition_totals.storage_and_metadata_update
	}

	#[must_use]
	pub fn usage_update_partition_total(&self) -> u64 {
		self.partition_totals.usage_update
	}
}

impl tangram_index::Index for Index {
	async fn verify_batch(
		&self,
		args: &[tangram_index::verify::Arg],
		config: tangram_index::verify::Config,
		principal: &tg::Principal,
	) -> tg::Result<Vec<tangram_index::verify::Output>> {
		self.verify_batch(args, config, principal).await
	}

	async fn contains_ids(&self, ids: &[tg::Id]) -> tg::Result<Vec<bool>> {
		self.contains_ids(ids).await
	}

	async fn visible(&self, ids: &[tg::Id], principal: &tg::Principal) -> tg::Result<Vec<bool>> {
		self.visible(ids, principal).await
	}

	async fn batch(&self, arg: tangram_index::batch::Arg) -> tg::Result<tg::Result<()>> {
		self.batch(arg).await
	}

	async fn try_get_ancestors(&self, id: &tg::Id) -> tg::Result<Option<Vec<tg::Id>>> {
		self.try_get_ancestors(id).await
	}

	async fn try_get_ids_for_specifiers(
		&self,
		specifiers: &[tg::Specifier],
	) -> tg::Result<Vec<Option<tg::Id>>> {
		self.try_get_ids_for_specifiers(specifiers).await
	}

	async fn get_subjects_for_principal(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<Vec<tg::authorization::Subject>> {
		self.get_subjects_for_principal(principal).await
	}

	async fn try_get_specifiers_for_ids(
		&self,
		ids: &[tg::Id],
	) -> tg::Result<Vec<Option<tg::Specifier>>> {
		self.try_get_specifiers_for_ids(ids).await
	}

	async fn try_get_oldest_update_transaction_id(
		&self,
		kind: tangram_index::update::Kind,
	) -> tg::Result<Option<u64>> {
		self.try_get_oldest_update_transaction_id(kind).await
	}

	async fn update_batch(
		&self,
		kind: tangram_index::update::Kind,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<tangram_index::update::Output> {
		self.update_batch(kind, batch_size, partition_start, partition_end)
			.await
	}

	async fn clean(
		&self,
		arg: tangram_index::clean::Arg,
	) -> tg::Result<tangram_index::clean::Output> {
		self.clean(arg).await
	}

	async fn get_transaction_id(&self) -> tg::Result<u64> {
		self.get_transaction_id().await
	}

	async fn sync(&self) -> tg::Result<()> {
		self.sync().await
	}

	fn cleaning_partition_total(&self) -> u64 {
		self.cleaning_partition_total()
	}

	fn permission_update_partition_total(&self) -> u64 {
		self.permission_update_partition_total()
	}

	fn storage_and_metadata_update_partition_total(&self) -> u64 {
		self.storage_and_metadata_update_partition_total()
	}

	fn usage_update_partition_total(&self) -> u64 {
		self.usage_update_partition_total()
	}
}
