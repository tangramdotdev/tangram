use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_index::scheduler::process::{Change, SchedulerProcess},
};

impl Index {
	pub async fn get_scheduler_processes(
		&self,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<Vec<tangram_index::scheduler::process::QueuedProcess>> {
		let request = tangram_index::read::Request::GetSchedulerProcesses {
			scheduler: scheduler.clone(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::GetSchedulerProcesses(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn get_scheduler_processes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<Vec<tangram_index::scheduler::process::QueuedProcess>> {
		let scheduler = scheduler.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::SchedulerProcess.to_i32().unwrap(), scheduler.as_ref()),
		);
		let iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the scheduler processes"))?;
		let mut processes = Vec::new();
		for entry in iter {
			let (key, value) =
				entry.map_err(|error| tg::error!(!error, "failed to read a scheduler process"))?;
			let key = Self::unpack(subspace, key)?;
			let Key::Runner(crate::runner::Key::SchedulerProcess { process, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			let Some(request) = SchedulerProcess::deserialize(value)?.request else {
				continue;
			};
			let process = tangram_index::scheduler::process::QueuedProcess {
				id: process,
				request,
			};
			processes.push(process);
		}

		Ok(processes)
	}

	pub(crate) fn put_scheduler_process_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::scheduler::process::put::Arg,
	) -> tg::Result<()> {
		let key = Key::Runner(crate::runner::Key::SchedulerProcess {
			process: arg.process.clone(),
			scheduler: arg.scheduler.clone(),
		});
		let existing =
			Self::try_get_scheduler_process_with_transaction(db, subspace, transaction, &key)?;
		let change = SchedulerProcess::put(existing.as_ref(), &arg.request);
		Self::apply_scheduler_process_change(db, subspace, transaction, &key, &change)?;

		Ok(())
	}

	pub(crate) fn delete_scheduler_process_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::scheduler::process::delete::Arg,
	) -> tg::Result<()> {
		let key = Key::Runner(crate::runner::Key::SchedulerProcess {
			process: arg.process.clone(),
			scheduler: arg.scheduler.clone(),
		});
		let existing =
			Self::try_get_scheduler_process_with_transaction(db, subspace, transaction, &key)?;
		let change = SchedulerProcess::delete(existing.as_ref());
		Self::apply_scheduler_process_change(db, subspace, transaction, &key, &change)?;

		Ok(())
	}

	fn try_get_scheduler_process_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		key: &Key,
	) -> tg::Result<Option<SchedulerProcess>> {
		let bytes = db
			.get(transaction, &Self::pack(subspace, key))
			.map_err(|error| tg::error!(!error, "failed to get the scheduler process"))?;
		let record = bytes.map(SchedulerProcess::deserialize).transpose()?;

		Ok(record)
	}

	fn apply_scheduler_process_change(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		key: &Key,
		change: &Change,
	) -> tg::Result<()> {
		match change {
			Change::Clear => {
				db.delete(transaction, &Self::pack(subspace, key))
					.map_err(|error| {
						tg::error!(!error, "failed to delete the scheduler process")
					})?;
			},
			Change::Keep => (),
			Change::Store(record) => {
				let value = record.serialize()?;
				db.put(transaction, &Self::pack(subspace, key), &value)
					.map_err(|error| tg::error!(!error, "failed to put the scheduler process"))?;
			},
		}

		Ok(())
	}
}
