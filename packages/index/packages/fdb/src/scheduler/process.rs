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
		let entry = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		let result = txn
			.get_ranges_keyvalues(entry, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let mut processes = Vec::new();
		for entry in &entries {
			let key = Self::unpack(subspace, entry.key())?;
			let Key::Runner(crate::runner::Key::SchedulerProcess { process, .. }) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			let Some(request) = SchedulerProcess::deserialize(entry.value())?.request else {
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
		Self::apply_scheduler_process_change(txn, subspace, &key, &change)?;

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
		let change = SchedulerProcess::delete(existing.as_ref());
		Self::apply_scheduler_process_change(txn, subspace, &key, &change)?;

		Ok(ControlFlow::Break(()))
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

	fn apply_scheduler_process_change(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		key: &Key,
		change: &Change,
	) -> tg::Result<()> {
		match change {
			Change::Clear => {
				txn.clear(&Self::pack(subspace, key));
			},
			Change::Keep => (),
			Change::Store(record) => {
				let value = record.serialize()?;
				txn.set(&Self::pack(subspace, key), &value);
			},
		}

		Ok(())
	}
}
