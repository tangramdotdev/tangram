use {
	crate::fdb::{Index, Kind, update::Key},
	foundationdb as fdb,
	foundationdb_tuple::{self as fdbt, Subspace},
	futures::future,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(super) async fn clean_update_propagated_versions(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
		partition_totals: crate::fdb::PartitionTotals,
	) -> tg::Result<ControlFlow<usize, fdb::FdbError>> {
		let mut candidates = Vec::new();
		for (kind, key_kind) in [
			(crate::update::Kind::Grant, Kind::GrantUpdateClean),
			(crate::update::Kind::Node, Kind::NodeUpdateClean),
		] {
			if candidates.len() == batch_size {
				break;
			}
			// An older update in any partition can still propagate to an item in this partition.
			let oldest = crate::fdb::propagate!(
				Self::try_get_oldest_update_transaction_id_with_transaction(
					txn,
					subspace,
					kind,
					partition_totals.update(kind),
				)
				.await
			);
			for partition in partition_start..partition_end {
				let remaining = batch_size.saturating_sub(candidates.len());
				if remaining == 0 {
					break;
				}
				let key_kind = key_kind.to_i32().unwrap();
				let begin = Self::pack(subspace, &(key_kind, partition));
				let end = if let Some(oldest) = oldest {
					// Exclude the entire oldest transaction, including its first versionstamp.
					let mut bytes = [0; 12];
					bytes[..8].copy_from_slice(&oldest.to_be_bytes());
					let version = fdbt::Versionstamp::from(bytes);
					Self::pack(subspace, &(key_kind, partition, version))
				} else {
					Subspace::from_bytes(begin.clone()).range().1
				};
				let range = fdb::RangeOption {
					begin: fdb::KeySelector::first_greater_or_equal(begin),
					end: fdb::KeySelector::first_greater_or_equal(end),
					limit: Some(remaining),
					mode: fdb::options::StreamingMode::WantAll,
					..Default::default()
				};
				let result = txn.get_range(&range, 1, false).await;
				let entries = crate::fdb::retry!(result);
				for entry in entries {
					let crate::fdb::Key::Update(Key::Clean {
						id, kind, version, ..
					}) = Self::unpack(subspace, entry.key())?
					else {
						return Err(tg::error!("expected an update clean key"));
					};
					let key = crate::fdb::Key::Update(Key::PropagatedVersion { id, kind });
					let key = Self::pack(subspace, &key);
					candidates.push((entry.key().to_vec(), key, version));
				}
			}
		}

		// Read the current versions together, retaining conflicts with concurrent propagation.
		let futures = candidates.iter().map(|(_, key, _)| txn.get(key, false));
		let result = future::try_join_all(futures).await;
		let values = crate::fdb::retry!(result);
		for ((clean_key, key, version), value) in std::iter::zip(&candidates, values) {
			// A stale cleanup entry must not delete a version recorded by a subsequent propagation.
			if value.is_some_and(|value| value.as_ref() == version.as_bytes()) {
				txn.clear(key);
			}
			txn.clear(clean_key);
		}
		let count = candidates.len();

		Ok(ControlFlow::Break(count))
	}
}
