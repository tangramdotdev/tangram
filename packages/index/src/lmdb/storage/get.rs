use {
	crate::lmdb::{Db, Index, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn get_owner_usage(
		&self,
		owner: &crate::storage::Owner,
	) -> tg::Result<crate::storage::Usage> {
		let response = self
			.send_read_request(crate::read::Request::GetOwnerUsage {
				owner: owner.clone(),
			})
			.await?;
		let crate::read::Response::GetOwnerUsage(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn get_owner_usage_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		owner: &crate::storage::Owner,
	) -> tg::Result<crate::storage::Usage> {
		let owner = owner.id().to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::OwnerStorage.to_i32().unwrap(), owner.as_ref()),
		);
		let mut object_count = 0i128;
		let mut object_size = 0i128;
		let mut process_count = 0i128;
		for entry in db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the owner storage keys"))?
		{
			let (key, value) =
				entry.map_err(|error| tg::error!(!error, "failed to read an owner storage key"))?;
			let key = Self::unpack(subspace, key)?;
			let crate::lmdb::Key::Storage(crate::lmdb::storage::Key::OwnerStorage { kind, .. }) =
				key
			else {
				return Err(tg::error!("unexpected key type"));
			};
			let value = i64::from_le_bytes(
				value
					.try_into()
					.map_err(|_| tg::error!("invalid owner storage value"))?,
			);
			match kind {
				crate::storage::Kind::ObjectCount => object_count += i128::from(value),
				crate::storage::Kind::ObjectSize => object_size += i128::from(value),
				crate::storage::Kind::ProcessCount => process_count += i128::from(value),
			}
		}
		let object_count = u64::try_from(object_count)
			.map_err(|_| tg::error!("the owner object count is out of range"))?;
		let object_size = u64::try_from(object_size)
			.map_err(|_| tg::error!("the owner object size is out of range"))?;
		let process_count = u64::try_from(process_count)
			.map_err(|_| tg::error!("the owner process count is out of range"))?;
		let usage = crate::storage::Usage {
			object_count,
			object_size,
			process_count,
		};

		Ok(usage)
	}
}
