use {
	crate::fdb::{Index, ItemKind, Key, Request, Response},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::future,
	std::time::Duration,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = Request::TouchProcesses(crate::fdb::TouchProcesses {
			ids: ids.to_vec(),
			time_to_touch,
			touched_at,
		});
		let response = self.send_write_request(request).await?;
		let Response::Processes(processes) = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(processes)
	}

	pub(crate) async fn touch_processes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		future::try_join_all(ids.iter().map(|id| {
			let subspace = subspace.clone();
			async move {
				Self::touch_process_with_transaction(
					txn,
					&subspace,
					id,
					touched_at,
					time_to_touch,
					partition_total,
				)
				.await
			}
		}))
		.await
	}

	async fn touch_process_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		touched_at: i64,
		time_to_touch: Duration,
		partition_total: u64,
	) -> tg::Result<Option<crate::process::Process>> {
		let key = Key::Process(crate::fdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
		let existing = existing
			.as_ref()
			.map(|bytes| crate::process::Process::deserialize(bytes))
			.transpose()?;
		let Some(mut process) = existing else {
			return Ok(None);
		};
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		if touched_at - process.touched_at < time_to_touch {
			return Ok(Some(process));
		}

		let mut key_end = key.clone();
		key_end.push(0x00);
		txn.add_conflict_range(&key, &key_end, fdb::options::ConflictRangeType::Read)
			.map_err(|error| tg::error!(!error, "failed to add read conflict range"))?;

		process.touched_at = process.touched_at.max(touched_at);
		let value = process
			.serialize()
			.map_err(|error| tg::error!(!error, "failed to serialize the process"))?;
		txn.set(&key, &value);
		if process.reference_count == 0 {
			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = crate::fdb::Key::Clean(crate::fdb::clean::Key::Clean {
				partition,
				touched_at: process.touched_at,
				kind: ItemKind::Process,
				id: id.clone().into(),
			});
			let key = Self::pack(subspace, &key);
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			txn.set(&key, &[]);
		}

		Ok(Some(process))
	}
}
