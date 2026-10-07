use {
	crate::{
		request::{Clean, Request, TouchCheckouts, TouchObjects, TouchProcesses, Update},
		response::Response,
	},
	crossbeam_channel as crossbeam, foundationdb_tuple as fdbt, heed as lmdb,
	std::{path::PathBuf, sync::Arc},
	tangram_client::prelude::*,
};

mod ancestor;
mod batch;
mod checkout;
mod clean;
mod delegation;
mod group;
mod indexer;
mod key;
mod node;
mod object;
mod organization;
mod permission;
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
mod verify;
mod visible;
mod writer;

pub(crate) use key::{Key, Kind};

#[derive(Clone, Debug)]
pub struct Config {
	pub map_size: usize,
	pub max_process_depth: Option<u64>,
	pub path: PathBuf,
	pub posix_sem_prefix: Option<String>,
	pub read_request_batch_size: usize,
	pub read_transaction_concurrency: usize,
	pub usage_partition_total: u64,
	pub write_operation_batch_size: usize,
}

pub struct Index {
	#[cfg_attr(not(test), allow(dead_code))]
	db: Db,
	env: lmdb::Env,
	reader_handles: Vec<std::thread::JoinHandle<()>>,
	reader_sender: Option<tangram_index::read::Sender>,
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
		let mut options = lmdb::EnvOpenOptions::new();
		options
			.map_size(config.map_size)
			.max_dbs(3)
			.max_readers(1_000);
		// SAFETY: The index writes to the mapped data only through LMDB transactions.
		unsafe {
			options.flags(
				lmdb::EnvFlags::NO_SUB_DIR | lmdb::EnvFlags::NO_SYNC | lmdb::EnvFlags::WRITE_MAP,
			);
		}
		if let Some(prefix) = &config.posix_sem_prefix {
			options.semaphore_name(prefix.clone());
		}
		// SAFETY: The index accesses the database and its lock file only through LMDB.
		let env = unsafe {
			options.open(&config.path).map_err(|error| {
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
			tokio::sync::mpsc::channel(tangram_index::read::CHANNEL_CAPACITY);
		let reader_receiver = Arc::new(std::sync::Mutex::new(reader_receiver));
		let reader_handles = (0..config.read_transaction_concurrency)
			.map(|_| {
				let env = env.clone();
				let reader_receiver = reader_receiver.clone();
				let subspace = subspace.clone();
				let read_request_batch_size = config.read_request_batch_size;
				std::thread::spawn(move || {
					Self::reader_task(&reader::Arg {
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
			let env = env.clone();
			let subspace = subspace.clone();
			let max_process_depth = config.max_process_depth;
			let usage_partition_total = config.usage_partition_total;
			let write_operation_batch_size = config.write_operation_batch_size;
			move || {
				Self::writer_task(writer::Arg {
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
			.send_read_request(tangram_index::read::Request::GetTransactionId)
			.await?;
		let tangram_index::read::Response::GetTransactionId(output) = response else {
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

	async fn get_requester_subjects(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<Vec<tg::authorization::Subject>> {
		self.get_requester_subjects(principal).await
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
		_partition_start: u64,
		_partition_end: u64,
	) -> tg::Result<tangram_index::update::Output> {
		self.update_batch(kind, batch_size).await
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
		1
	}

	fn permission_update_partition_total(&self) -> u64 {
		1
	}

	fn storage_and_metadata_update_partition_total(&self) -> u64 {
		1
	}

	fn usage_update_partition_total(&self) -> u64 {
		1
	}
}
