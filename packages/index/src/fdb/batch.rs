use {
	super::{Index, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn batch(&self, arg: crate::batch::Arg) -> tg::Result<()> {
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
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::batch::Arg,
		partition_total: u64,
		usage_partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for item in &arg.items {
			match item {
				crate::batch::Item::DeleteGrant(arg) => {
					crate::fdb::propagate!(
						Self::delete_grants_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(arg),
							partition_total,
						)
						.await
					);
				},
				crate::batch::Item::DeleteGroup(id) => {
					crate::fdb::propagate!(
						Self::delete_groups_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(id),
						)
						.await
					);
				},
				crate::batch::Item::DeleteGroupMember(arg) => {
					crate::fdb::propagate!(Self::delete_group_members_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					));
				},
				crate::batch::Item::DeleteOrganization(id) => {
					crate::fdb::propagate!(
						Self::delete_organizations_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(id),
						)
						.await
					);
				},
				crate::batch::Item::DeleteOrganizationMember(arg) => {
					crate::fdb::propagate!(Self::delete_organization_members_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					));
				},
				crate::batch::Item::DeleteSandbox(id) => {
					crate::fdb::propagate!(Self::delete_sandboxes_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(id),
					));
				},
				crate::batch::Item::DeleteTag(id) => {
					crate::fdb::propagate!(
						Self::delete_tags_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(id),
							partition_total,
						)
						.await
					);
				},
				crate::batch::Item::DeleteUser(id) => {
					crate::fdb::propagate!(
						Self::delete_users_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(id),
						)
						.await
					);
				},
				crate::batch::Item::EnqueueLogCompaction(process) => {
					crate::fdb::propagate!(
						Self::enqueue_log_compaction_with_transaction(
							txn,
							subspace,
							process,
							partition_total,
						)
						.await
					);
				},
				crate::batch::Item::PutCacheEntry(arg) => {
					crate::fdb::propagate!(Self::put_cache_entries_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					));
				},
				crate::batch::Item::PutGrant(arg) => {
					crate::fdb::propagate!(
						Self::put_grants_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(arg),
							partition_total,
						)
						.await
					);
				},
				crate::batch::Item::PutGroup(arg) => {
					crate::fdb::propagate!(Self::put_groups_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					));
				},
				crate::batch::Item::PutGroupMember(arg) => {
					crate::fdb::propagate!(Self::put_group_members_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					));
				},
				crate::batch::Item::PutObject(arg) => {
					crate::fdb::propagate!(
						Self::put_objects_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(arg),
							partition_total,
						)
						.await
					);
				},
				crate::batch::Item::PutAccountObject(arg) => {
					crate::fdb::propagate!(
						Self::put_account_object(
							txn,
							subspace,
							arg,
							partition_total,
							usage_partition_total,
							true,
							None,
						)
						.await
					);
				},
				crate::batch::Item::PutAccountProcess(arg) => {
					crate::fdb::propagate!(
						Self::put_account_process(
							txn,
							subspace,
							arg,
							partition_total,
							usage_partition_total,
							true,
							None,
						)
						.await
					);
				},
				crate::batch::Item::PutOrganization(arg) => {
					crate::fdb::propagate!(
						Self::put_organizations_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(arg),
						)
						.await
					);
				},
				crate::batch::Item::PutOrganizationMember(arg) => {
					crate::fdb::propagate!(Self::put_organization_members_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					));
				},
				crate::batch::Item::PutProcess(arg) => {
					crate::fdb::propagate!(
						Self::put_processes_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(arg),
							partition_total,
						)
						.await
					);
				},
				crate::batch::Item::PutSandbox(arg) => {
					crate::fdb::propagate!(
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
				crate::batch::Item::PutTag(arg) => {
					crate::fdb::propagate!(
						Self::put_tags_with_transaction(
							txn,
							subspace,
							std::slice::from_ref(arg),
							partition_total,
						)
						.await
					);
				},
				crate::batch::Item::PutUser(arg) => {
					crate::fdb::propagate!(
						Self::put_users_with_transaction(txn, subspace, std::slice::from_ref(arg),)
							.await
					);
				},
			}
		}

		Ok(ControlFlow::Break(()))
	}
}
