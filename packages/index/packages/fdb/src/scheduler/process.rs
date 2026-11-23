use {
	crate::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
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

	pub(crate) async fn get_scheduler_processes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<ControlFlow<Vec<tangram_index::scheduler::process::QueuedProcess>, fdb::FdbError>>
	{
		let scheduler = scheduler.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::SchedulerProcess.to_i32().unwrap(), scheduler.as_ref()),
		);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let mut processes = Vec::new();
		for entry in &entries {
			let key = Self::unpack(subspace, entry.key())?;
			let Key::Runner(crate::runner::Key::SchedulerProcess { process, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			let SchedulerProcess::Queued(request) = SchedulerProcess::deserialize(entry.value())?
			else {
				continue;
			};
			let process = tangram_index::scheduler::process::QueuedProcess {
				id: process,
				request,
			};
			processes.push(process);
		}

		Ok(ControlFlow::Break(processes))
	}

	pub(crate) async fn put_scheduler_process_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		arg: &tangram_index::scheduler::process::put::Arg,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = Key::Runner(crate::runner::Key::SchedulerProcess {
			process: arg.process.clone(),
			scheduler: arg.scheduler.clone(),
		});
		let existing = crate::propagate!(
			Self::try_get_scheduler_process_with_transaction(txn, subspace, &key).await
		);
		let change = SchedulerProcess::put(existing.as_ref(), &arg.request);
		Self::apply_scheduler_process_change(txn, subspace, &arg.scheduler, &arg.process, &change)?;

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn delete_scheduler_process_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		arg: &tangram_index::scheduler::process::delete::Arg,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = Key::Runner(crate::runner::Key::SchedulerProcess {
			process: arg.process.clone(),
			scheduler: arg.scheduler.clone(),
		});
		let existing = crate::propagate!(
			Self::try_get_scheduler_process_with_transaction(txn, subspace, &key).await
		);
		let change = SchedulerProcess::delete(existing.as_ref(), arg.deleted_at);
		Self::apply_scheduler_process_change(txn, subspace, &arg.scheduler, &arg.process, &change)?;

		Ok(ControlFlow::Break(()))
	}

	/// Delete the records of the deletes of scheduler processes that are at least as old as the cutoff, in the order of their age.
	pub(crate) async fn delete_expired_scheduler_processes(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		max_deleted_at: i64,
		limit: usize,
	) -> tg::Result<ControlFlow<usize, fdb::FdbError>> {
		if limit == 0 {
			return Ok(ControlFlow::Break(0));
		}

		// Read the keys of the deletes in the order of their age.
		let prefix = Self::pack(
			subspace,
			&(Kind::SchedulerProcessDeletedAt.to_i32().unwrap(),),
		);
		let range = fdb::RangeOption {
			limit: Some(limit),
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let mut expired = Vec::new();
		for entry in &entries {
			let Key::Runner(crate::runner::Key::SchedulerProcessDeletedAt {
				deleted_at,
				process,
				scheduler,
			}) = Self::unpack(subspace, entry.key())?
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
			txn.clear(&Self::pack(subspace, &key));
			let key = Key::Runner(crate::runner::Key::SchedulerProcessDeletedAt {
				deleted_at,
				process,
				scheduler,
			});
			txn.clear(&Self::pack(subspace, &key));
		}

		Ok(ControlFlow::Break(count))
	}

	async fn try_get_scheduler_process_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		key: &Key,
	) -> tg::Result<ControlFlow<Option<SchedulerProcess>, fdb::FdbError>> {
		let result = txn.get(&Self::pack(subspace, key), false).await;
		let bytes = crate::retry!(result);
		let record = bytes
			.as_deref()
			.map(SchedulerProcess::deserialize)
			.transpose()?;

		Ok(ControlFlow::Break(record))
	}

	/// Apply a change to the entry of a process in a scheduler's queue, and record a delete by its time, so that the clean removes the record of the delete by its age.
	fn apply_scheduler_process_change(
		txn: &fdb::Transaction,
		subspace: &Subspace,
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
		txn.set(&Self::pack(subspace, &key), &value);
		if let SchedulerProcess::Deleted(deleted_at) = record {
			let key = Key::Runner(crate::runner::Key::SchedulerProcessDeletedAt {
				deleted_at: *deleted_at,
				process: process.clone(),
				scheduler: scheduler.clone(),
			});
			txn.set(&Self::pack(subspace, &key), &[]);
		}

		Ok(())
	}
}
