use {
	super::{Index, Kind},
	foundationdb as fdb,
	foundationdb_tuple::{self as fdbt, Subspace, TuplePack as _},
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn get_transaction_id(&self) -> tg::Result<u128> {
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create the transaction"))?;
		let version = txn
			.get_read_version()
			.await
			.map_err(|source| tg::error!(!source, "failed to get read version"))?;
		// Return the transaction version as a u128. The full versionstamp is 10 bytes (80 bits):
		// 8 bytes for the transaction version and 2 bytes for the batch order. We shift the
		// transaction version left by 16 bits to leave room for the batch order.
		Ok(u128::from(version.cast_unsigned()) << 16)
	}

	pub async fn get_queue_size(&self, transaction_id: u128) -> tg::Result<u64> {
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create the transaction"))?;

		// Convert transaction_id to a versionstamp. The transaction_id is a u128 representing
		// the transaction version shifted left by 16 bits. We extract the 8-byte transaction
		// version and create a versionstamp with max batch order (0xFFFF) to include all
		// entries with transaction version <= the given version, regardless of batch order.
		let version = (transaction_id >> 16) as u64;
		let mut stamp_bytes = [0u8; 10];
		stamp_bytes[0..8].copy_from_slice(&version.to_be_bytes());
		stamp_bytes[8..10].copy_from_slice(&0xFFFFu16.to_be_bytes());
		let end_versionstamp = fdbt::Versionstamp::complete(stamp_bytes, 0);

		// Scan from the beginning of UpdateVersion to the given transaction_id.
		let prefix = self.pack(&(Kind::UpdateVersion.to_i32().unwrap(),));

		// Pack the end key with the versionstamp.
		let mut end = prefix.clone();
		end.extend_from_slice(&(end_versionstamp,).pack_to_vec());

		let subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			begin: fdb::KeySelector::first_greater_or_equal(subspace.range().0),
			end: fdb::KeySelector::first_greater_or_equal(end),
			mode: fdb::options::StreamingMode::WantAll,
			..Default::default()
		};

		// Count the entries.
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get queue size range"))?;

		Ok(entries.len() as u64)
	}
}
