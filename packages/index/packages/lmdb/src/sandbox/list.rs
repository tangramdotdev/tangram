use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn list_sandbox_ids_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		after: Option<&tg::sandbox::Id>,
		limit: usize,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		let prefix = Self::pack(subspace, &(Kind::Sandbox.to_i32().unwrap(),));
		let after = after.map(|sandbox| {
			Self::pack(
				subspace,
				&(Kind::Sandbox.to_i32().unwrap(), sandbox.to_bytes().as_ref()),
			)
		});
		let (entries, _) = Self::get_entry_page_with_transaction(
			db,
			subspace,
			transaction,
			&prefix,
			after.as_deref(),
			limit,
		)?;
		let ids = entries
			.into_iter()
			.map(|(key, _)| {
				let Key::Sandbox(crate::sandbox::Key::Sandbox(id)) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(id)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ids)
	}

	pub(crate) fn list_sandboxes_for_principal_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		arg: &tangram_index::sandbox::list::Arg,
		kind: Kind,
	) -> tg::Result<tangram_index::sandbox::list::Output> {
		// Get the page of sandbox keys for the principal.
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
					position.sandbox.to_bytes().as_ref(),
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
				Key::Sandbox(crate::sandbox::Key::CreatorSandbox {
					sandbox, status, ..
				}) if kind == Kind::CreatorSandbox => {
					Ok(tangram_index::sandbox::list::Position { sandbox, status })
				},
				Key::Sandbox(crate::sandbox::Key::OwnerSandbox {
					sandbox, status, ..
				}) if kind == Kind::OwnerSandbox => {
					Ok(tangram_index::sandbox::list::Position { sandbox, status })
				},
				_ => Err(tg::error!("unexpected key type")),
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let next = if cursor.is_some() {
			positions.last().cloned()
		} else {
			None
		};

		// Get each sandbox, skipping the sandboxes that are missing.
		let mut data = Vec::with_capacity(positions.len());
		for position in positions {
			let Some(sandbox) = Self::try_get_sandbox_with_transaction(
				db,
				subspace,
				transaction,
				&position.sandbox,
			)?
			else {
				continue;
			};
			data.push((position.sandbox, sandbox));
		}
		let output = tangram_index::sandbox::list::Output { data, next };

		Ok(output)
	}
}
