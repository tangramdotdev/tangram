use {
	crate::lmdb::{Db, Index, ItemKind, Key, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
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
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::TouchProcesses(crate::lmdb::TouchProcesses {
			ids: ids.to_vec(),
			time_to_touch,
			touched_at,
		});
		self.sender_high
			.as_ref()
			.unwrap()
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::Processes(processes) = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(processes)
	}

	pub(crate) fn task_touch_processes(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<crate::process::Process>>> {
		let mut outputs = Vec::with_capacity(ids.len());
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap();
		for id in ids {
			let key = Key::Process(crate::lmdb::process::Key::Process(id.clone()));
			let key = Self::pack(subspace, &key);
			let existing = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
			let existing = existing
				.map(crate::process::Process::deserialize)
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
				let key = crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Clean {
					touched_at: process.touched_at,
					kind: ItemKind::Process,
					id: id.clone().into(),
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
