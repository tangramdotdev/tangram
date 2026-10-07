use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	std::time::Duration,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn touch_processes_with_account_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::TouchProcesses,
		usage_partition_total: u64,
	) -> tg::Result<Vec<Option<tangram_index::process::Process>>> {
		let crate::TouchProcesses {
			account,
			ids,
			put_account,
			time_to_touch,
			touched_at,
		} = arg;
		let processes = Self::touch_processes_with_transaction(
			db,
			subspace,
			transaction,
			ids,
			*touched_at,
			*time_to_touch,
		)?;
		if let Some(account) = account.as_ref() {
			for (id, process) in std::iter::zip(ids, &processes) {
				if process.is_none() {
					continue;
				}
				let arg = tangram_index::usage::storage::put::ProcessArg {
					account: account.clone(),
					process: id.clone(),
					touched_at: *touched_at,
				};
				if *put_account {
					let inserted = Self::put_account_process(
						db,
						subspace,
						transaction,
						&arg,
						usage_partition_total,
						Some(tg::authorization::permission::Set::Process(
							tg::authorization::permission::process::Set::all(),
						)),
						None,
					)?;
					if inserted {
						continue;
					}
				}
				Self::touch_account_process(db, subspace, transaction, &arg, *time_to_touch)?;
			}
		}

		Ok(processes)
	}

	pub(crate) fn touch_processes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<tangram_index::process::Process>>> {
		let mut outputs = Vec::with_capacity(ids.len());
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		for id in ids {
			let key = Key::Process(crate::process::Key::Process(id.clone()));
			let key = Self::pack(subspace, &key);
			let existing = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
			let existing = existing
				.map(tangram_index::process::Process::deserialize)
				.transpose()?;
			let Some(mut process) = existing else {
				outputs.push(None);
				continue;
			};
			if touched_at - process.touched_at < time_to_touch {
				outputs.push(Some(process));
				continue;
			}
			process.touched_at = process.touched_at.max(touched_at);
			let value = process.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, %id, "failed to put the process"))?;
			if process.reference_count == 0 {
				let key = crate::Key::Clean(crate::clean::Key::Process {
					id: id.clone(),
					touched_at: process.touched_at,
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the clean key"))?;
			}
			outputs.push(Some(process));
		}
		Ok(outputs)
	}
}
