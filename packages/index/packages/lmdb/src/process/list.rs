use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn list_processes_for_principal_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		arg: &tangram_index::process::list::Arg,
		kind: Kind,
	) -> tg::Result<tangram_index::process::list::Output> {
		// Get the page of process keys for the principal.
		let principal = arg.principal.to_string();
		let prefix = if let Some(status) = arg.status {
			Self::pack(
				subspace,
				&(
					kind.to_i32().unwrap(),
					principal.as_str(),
					status.to_string(),
				),
			)
		} else {
			Self::pack(subspace, &(kind.to_i32().unwrap(), principal.as_str()))
		};
		let after = arg.after.as_ref().map(|position| {
			Self::pack(
				subspace,
				&(
					kind.to_i32().unwrap(),
					principal.as_str(),
					position.status.to_string(),
					position.process.to_bytes().as_ref(),
				),
			)
		});
		let (entries, cursor) = Self::get_entry_page_with_transaction(
			db,
			subspace,
			transaction,
			&prefix,
			after.as_deref(),
			arg.limit,
		)?;
		let positions = entries
			.into_iter()
			.map(|(key, _)| match key {
				Key::Process(crate::process::Key::CreatorProcess {
					process, status, ..
				}) if kind == Kind::CreatorProcess => {
					Ok(tangram_index::process::list::Position { process, status })
				},
				Key::Process(crate::process::Key::OwnerProcess {
					process, status, ..
				}) if kind == Kind::OwnerProcess => {
					Ok(tangram_index::process::list::Position { process, status })
				},
				_ => Err(tg::error!("unexpected key type")),
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let next = if cursor.is_some() {
			positions.last().cloned()
		} else {
			None
		};

		// Get each process and its data, skipping the processes that are missing.
		let mut data = Vec::with_capacity(positions.len());
		for position in positions {
			let Some(process) = Self::try_get_process_with_transaction(
				db,
				subspace,
				transaction,
				&position.process,
			)?
			else {
				continue;
			};
			let Some(process_data) = Self::try_get_process_data_with_transaction(
				db,
				subspace,
				transaction,
				&position.process,
			)?
			else {
				continue;
			};
			data.push((position.process, process, process_data));
		}
		let output = tangram_index::process::list::Output { data, next };

		Ok(output)
	}
}
