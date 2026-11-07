use {
	crate::{Index, Key, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	num::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_index::permission::capture::{Captures, Entry, enqueue},
};

impl Index {
	/// Put the permissions, delegations, and capture that a resource's put carries, skipping a grant from a finished process to a process that it does not list as a child.
	pub(crate) async fn put_captures_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		captures: &Captures,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		// Put the permissions.
		crate::propagate!(
			Self::put_permissions_with_transaction(
				txn,
				subspace,
				&captures.permissions,
				partition_totals,
			)
			.await
		);

		// Put the delegations.
		for delegation in &captures.delegations {
			if let Some((source, destination)) = delegation.process_grant() {
				let accepts = crate::propagate!(
					Self::try_get_process_accepts_child_with_transaction(
						txn,
						subspace,
						&source,
						&destination
					)
					.await
				);
				if accepts == Some(false) {
					continue;
				}
			}
			crate::propagate!(
				Self::put_delegation_with_transaction(
					txn,
					subspace,
					delegation,
					partition_totals.cleaning
				)
				.await
			);
		}

		// Enqueue the captures.
		for enqueue in &captures.enqueues {
			if let Some((source, destination)) = enqueue.process_grant() {
				let accepts = crate::propagate!(
					Self::try_get_process_accepts_child_with_transaction(
						txn,
						subspace,
						&source,
						&destination
					)
					.await
				);
				if accepts == Some(false) {
					continue;
				}
			}
			crate::propagate!(
				Self::enqueue_permission_capture_with_transaction(
					txn,
					subspace,
					enqueue,
					partition_totals.permission_update
				)
				.await
			);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn enqueue_permission_capture_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &enqueue::Arg,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		arg.validate()?;
		if !crate::propagate!(Self::permission_capture_resource_current(txn, subspace, arg).await) {
			return Ok(ControlFlow::Break(()));
		}
		let partition = Self::partition_for_id(&arg.id, partition_total);
		let key = Self::pack(
			subspace,
			&Key::PermissionCapture {
				id: arg.id.clone(),
				partition,
			},
		);
		let bytes = tangram_serialize::to_vec(arg).map_err(|error| {
			tg::error!(!error, "failed to encode a permission capture argument")
		})?;
		txn.set(&key, &bytes);
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn permission_capture_batch_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<ControlFlow<Vec<Entry>, fdb::FdbError>> {
		if batch_size == 0 || partition_start >= partition_end {
			return Ok(ControlFlow::Break(Vec::new()));
		}
		let begin = subspace.pack(&(Kind::PermissionCapture.to_i32().unwrap(), partition_start));
		let end = subspace.pack(&(Kind::PermissionCapture.to_i32().unwrap(), partition_end));
		let range = fdb::RangeOption {
			limit: Some(batch_size),
			..fdb::RangeOption::from((begin, end))
		};
		let result = txn.get_range(&range, 1, true).await;
		let rows = crate::retry!(result);
		let entries = rows
			.iter()
			.map(|row| {
				let Key::PermissionCapture { partition, .. } = Self::unpack(subspace, row.key())
					.map_err(|error| {
						tg::error!(!error, "failed to decode a permission capture queue key")
					})?
				else {
					return Err(tg::error!("unexpected permission capture key"));
				};
				let arg = tangram_serialize::from_slice(row.value()).map_err(|error| {
					tg::error!(!error, "failed to decode a permission capture argument")
				})?;
				Ok(Entry { arg, partition })
			})
			.collect::<tg::Result<Vec<_>>>()?;
		Ok(ControlFlow::Break(entries))
	}

	pub(crate) fn complete_permission_capture_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		entry: &Entry,
	) {
		let key = Self::pack(
			subspace,
			&Key::PermissionCapture {
				id: entry.arg.id.clone(),
				partition: entry.partition,
			},
		);
		txn.clear(&key);
	}

	async fn permission_capture_resource_current(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &enqueue::Arg,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let Some(version) = &arg.version else {
			return Ok(ControlFlow::Break(true));
		};
		let id = tg::tag::Id::try_from(arg.resource.clone())?;
		let tag = crate::propagate!(Self::try_get_tag_with_transaction(txn, subspace, &id).await);
		Ok(ControlFlow::Break(
			tag.is_some_and(|tag| tag.version == *version),
		))
	}
}
