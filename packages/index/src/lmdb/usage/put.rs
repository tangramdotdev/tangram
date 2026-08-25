use {
	crate::lmdb::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn add_usage_delta(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: crate::usage::DeltaArg<'_>,
	) -> tg::Result<()> {
		let hour = arg.at.div_euclid(60 * 60) * 60 * 60;
		let key = Key::Usage(crate::lmdb::usage::Key::Delta {
			account: arg.account.clone(),
			hour,
			kind: arg.kind,
			partition: arg.partition,
		});
		let key = Self::pack(subspace, &key);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the usage delta"))?
			.map(|value| {
				i64::from_le_bytes(
					value
						.try_into()
						.map_err(|_| tg::error!("invalid usage delta"))?,
				)
				.checked_add(arg.delta)
				.ok_or_else(|| tg::error!("the usage delta overflowed"))
			})
			.transpose()?
			.unwrap_or(arg.delta);
		db.put(transaction, &key, &value.to_le_bytes())
			.map_err(|error| tg::error!(!error, "failed to put the usage delta"))?;
		let key = Key::Usage(crate::lmdb::usage::Key::Aggregation {
			account: arg.account.clone(),
			hour,
			partition: arg.partition,
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the usage aggregation"))?;

		Ok(())
	}
}
