use {
	super::{Index, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn batch(&self, arg: tangram_index::batch::Arg) -> tg::Result<()> {
		if arg.is_empty() {
			return Ok(());
		}
		let request = Request::Batch(arg);
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}

	pub(crate) async fn batch_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::batch::Arg,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let partition_total = partition_totals.cleaning;
		let usage_partition_total = partition_totals.usage;
		for item in &arg.items {
			match item {
				tangram_index::batch::Item::DeleteDelegations(subject) => {
					crate::propagate!(
						Self::delete_delegations_for_subject_with_transaction(
							txn,
							subspace,
							subject,
							partition_totals.cleaning
						)
						.await
					);
				},

				tangram_index::batch::Item::EnqueuePermissionCapture(arg) => {
					crate::propagate!(
						Self::enqueue_permission_capture_with_transaction(
							txn,
							subspace,
							arg,
							partition_totals.permission_update
						)
						.await
					);
				},

				tangram_index::batch::Item::DeleteCheckout(id) => {
					crate::propagate!(
						Self::delete_checkout(txn, subspace, id, partition_total).await
					);
				},
				tangram_index::batch::Item::DeletePermission(arg) => {
					crate::propagate!(
						Self::delete_permissions_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(arg),
							partition_totals,
						)
						.await
					);
				},
				tangram_index::batch::Item::DeleteGroup(id) => {
					crate::propagate!(
						Self::delete_groups_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(id),
						)
						.await
					);
				},
				tangram_index::batch::Item::DeleteGroupMember(arg) => {
					crate::propagate!(Self::delete_group_members_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					));
				},
				tangram_index::batch::Item::DeleteOrganization(id) => {
					crate::propagate!(
						Self::delete_organizations_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(id),
						)
						.await
					);
				},
				tangram_index::batch::Item::DeleteOrganizationMember(arg) => {
					crate::propagate!(Self::delete_organization_members_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					));
				},
				tangram_index::batch::Item::DeleteSandbox(id) => {
					crate::propagate!(
						Self::delete_sandboxes_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(id),
							partition_totals.cleaning,
						)
						.await
					);
				},
				tangram_index::batch::Item::DeleteTag(id) => {
					crate::propagate!(
						Self::delete_tags_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(id),
							partition_totals,
						)
						.await
					);
				},
				tangram_index::batch::Item::DeleteUser(id) => {
					crate::propagate!(
						Self::delete_users_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(id),
						)
						.await
					);
				},
				tangram_index::batch::Item::EnqueueLogCompaction(process) => {
					crate::propagate!(
						Self::enqueue_log_compaction_with_transaction(
							txn,
							subspace,
							process,
							partition_totals.log_compaction,
						)
						.await
					);
				},
				tangram_index::batch::Item::PutCheckout(arg) => {
					crate::propagate!(Self::put_checkouts_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					));
				},
				tangram_index::batch::Item::PutDelegation(arg) => {
					crate::propagate!(
						Self::put_delegation_with_transaction(
							txn,
							subspace,
							arg,
							partition_totals.cleaning
						)
						.await
					);
				},
				tangram_index::batch::Item::PutPermission(arg) => {
					crate::propagate!(
						Self::put_permissions_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(arg),
							partition_totals,
						)
						.await
					);
				},
				tangram_index::batch::Item::PutGroup(arg) => {
					crate::propagate!(Self::put_groups_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					));
				},
				tangram_index::batch::Item::PutGroupMember(arg) => {
					crate::propagate!(Self::put_group_members_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					));
				},
				tangram_index::batch::Item::PutObject(arg) => {
					crate::propagate!(
						Self::put_objects_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(arg),
							partition_totals,
						)
						.await
					);
				},
				tangram_index::batch::Item::PutAccountObject(arg) => {
					crate::propagate!(
						Self::put_account_object(txn, subspace, arg, partition_totals, None, None,)
							.await
					);
				},
				tangram_index::batch::Item::PutAccountProcess(arg) => {
					crate::propagate!(
						Self::put_account_process(
							txn,
							subspace,
							arg,
							partition_totals,
							None,
							None,
						)
						.await
					);
				},
				tangram_index::batch::Item::PutOrganization(arg) => {
					crate::propagate!(
						Self::put_organizations_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(arg),
						)
						.await
					);
				},
				tangram_index::batch::Item::PutOrganizationMember(arg) => {
					crate::propagate!(Self::put_organization_members_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					));
				},
				tangram_index::batch::Item::PutProcess(arg) => {
					crate::propagate!(
						Self::put_processes_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(arg),
							partition_totals,
						)
						.await
					);
				},
				tangram_index::batch::Item::PutSandbox(arg) => {
					crate::propagate!(
						Self::put_sandboxes_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(arg),
							partition_total,
							usage_partition_total,
						)
						.await
					);
				},
				tangram_index::batch::Item::PutTag(arg) => {
					crate::propagate!(
						Self::put_tags_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(arg),
							partition_totals,
						)
						.await
					);
				},
				tangram_index::batch::Item::PutUser(arg) => {
					crate::propagate!(
						Self::put_users_with_transaction(txn, subspace, std::slice::from_ref(arg),)
							.await
					);
				},
			}
		}

		Ok(ControlFlow::Break(()))
	}
}
