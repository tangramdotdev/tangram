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
			let SchedulerProcess::Queued(request) = SchedulerProcess::deserialize(value)? else {
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
		Self::apply_scheduler_process_change(
			db,
			subspace,
			transaction,
			&arg.scheduler,
			&arg.process,
			&change,
		)?;

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
		let change = SchedulerProcess::delete(existing.as_ref(), arg.deleted_at);
		Self::apply_scheduler_process_change(
			db,
			subspace,
			transaction,
			&arg.scheduler,
			&arg.process,
			&change,
		)?;

		Ok(())
	}

	/// Delete the records of the deletes of scheduler processes that are at least as old as the cutoff, in the order of their age.
	pub(crate) fn delete_expired_scheduler_processes(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		max_deleted_at: i64,
		limit: usize,
	) -> tg::Result<usize> {
		if limit == 0 {
			return Ok(0);
		}

		// Collect the expired deletes in the order of their age.
		let prefix = Self::pack(
			subspace,
			&(Kind::SchedulerProcessDeletedAt.to_i32().unwrap(),),
		);
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the scheduler process deletes"))?;
		let mut expired = Vec::new();
		for entry in entries.take(limit) {
			let (key, _) = entry
				.map_err(|error| tg::error!(!error, "failed to read a scheduler process delete"))?;
			let Key::Runner(crate::runner::Key::SchedulerProcessDeletedAt {
				deleted_at,
				process,
				scheduler,
			}) = Self::unpack(subspace, key)?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			if deleted_at > max_deleted_at {
				break;
			}
			expired.push((deleted_at, process, scheduler));
		}

		// Delete each expired record and its key.
		let count = expired.len();
		for (deleted_at, process, scheduler) in expired {
			let key = Key::Runner(crate::runner::Key::SchedulerProcess {
				process: process.clone(),
				scheduler: scheduler.clone(),
			});
			db.delete(transaction, &Self::pack(subspace, &key))
				.map_err(|error| tg::error!(!error, "failed to delete the scheduler process"))?;
			let key = Key::Runner(crate::runner::Key::SchedulerProcessDeletedAt {
				deleted_at,
				process,
				scheduler,
			});
			db.delete(transaction, &Self::pack(subspace, &key))
				.map_err(|error| {
					tg::error!(!error, "failed to delete the scheduler process delete")
				})?;
		}

		Ok(count)
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

	/// Apply a change to the entry of a process in a scheduler's queue, and record a delete by its time, so that the clean removes the record of the delete by its age.
	fn apply_scheduler_process_change(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		scheduler: &tg::scheduler::Id,
		process: &tg::process::Id,
		change: &Change,
	) -> tg::Result<()> {
		let Change::Store(record) = change else {
			return Ok(());
		};
		let key = Key::Runner(crate::runner::Key::SchedulerProcess {
			process: process.clone(),
			scheduler: scheduler.clone(),
		});
		let value = record.serialize()?;
		db.put(transaction, &Self::pack(subspace, &key), &value)
			.map_err(|error| tg::error!(!error, "failed to put the scheduler process"))?;
		if let SchedulerProcess::Deleted(deleted_at) = record {
			let key = Key::Runner(crate::runner::Key::SchedulerProcessDeletedAt {
				deleted_at: *deleted_at,
				process: process.clone(),
				scheduler: scheduler.clone(),
			});
			db.put(transaction, &Self::pack(subspace, &key), &[])
				.map_err(|error| {
					tg::error!(!error, "failed to put the scheduler process delete")
				})?;
		}

		Ok(())
	}
}
