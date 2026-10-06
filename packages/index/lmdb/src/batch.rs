use {
	super::{Db, Index, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn batch(&self, arg: tangram_index::batch::Arg) -> tg::Result<tg::Result<()>> {
		if arg.is_empty() {
			return Ok(Ok(()));
		}
		let request = Request::Batch(arg);
		let response = self.send_write_request(request).await?;
		let Response::Mutation(result) = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(result)
	}

	pub(crate) fn batch_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::batch::Arg,
		usage_partition_total: u64,
	) -> tg::Result<tg::Result<()>> {
		for item in &arg.items {
			match item {
				tangram_index::batch::Item::DeleteDelegations(subject) => {
					Self::delete_delegations_for_subject_with_transaction(
						db,
						subspace,
						transaction,
						subject,
					)?;
				},

				tangram_index::batch::Item::EnqueuePermissionCapture(arg) => {
					Self::enqueue_permission_capture_with_transaction(
						db,
						subspace,
						transaction,
						arg,
					)?;
				},

				tangram_index::batch::Item::DeleteCheckout(id) => {
					Self::delete_checkout(db, subspace, transaction, id)?;
				},
				tangram_index::batch::Item::DeletePermission(arg) => {
					Self::delete_permissions_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::batch::Item::DeleteGroup(id) => {
					Self::delete_groups_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(id),
					)?;
				},
				tangram_index::batch::Item::DeleteGroupMember(arg) => {
					Self::delete_group_members_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::batch::Item::DeleteOrganization(id) => {
					Self::delete_organizations_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(id),
					)?;
				},
				tangram_index::batch::Item::DeleteOrganizationMember(arg) => {
					Self::delete_organization_members_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::batch::Item::DeleteSandbox(id) => {
					Self::delete_sandboxes_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(id),
					)?;
				},
				tangram_index::batch::Item::DeleteTag(id) => {
					Self::delete_tags_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(id),
					)?;
				},
				tangram_index::batch::Item::DeleteUser(id) => {
					Self::delete_users_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(id),
					)?;
				},
				tangram_index::batch::Item::EnqueueLogCompaction(process) => {
					Self::enqueue_log_compaction_with_transaction(
						db,
						subspace,
						transaction,
						process,
					)?;
				},
				tangram_index::batch::Item::PutCheckout(arg) => {
					Self::put_checkouts_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::batch::Item::PutDelegation(arg) => {
					Self::put_delegation_with_transaction(db, subspace, transaction, arg)?;
				},
				tangram_index::batch::Item::PutPermission(arg) => {
					Self::put_permissions_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::batch::Item::PutGroup(arg) => {
					Self::put_groups_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::batch::Item::PutGroupMember(arg) => {
					Self::put_group_members_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::batch::Item::PutObject(arg) => {
					Self::put_objects_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::batch::Item::PutAccountObject(arg) => {
					Self::put_account_object(
						db,
						subspace,
						transaction,
						arg,
						usage_partition_total,
						None,
						None,
					)?;
				},
				tangram_index::batch::Item::PutAccountProcess(arg) => {
					Self::put_account_process(
						db,
						subspace,
						transaction,
						arg,
						usage_partition_total,
						None,
						None,
					)?;
				},
				tangram_index::batch::Item::PutOrganization(arg) => {
					Self::put_organizations_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::batch::Item::PutOrganizationMember(arg) => {
					Self::put_organization_members_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::batch::Item::PutProcess(arg) => {
					let result = Self::put_processes_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
					if let Err(error) = result {
						return Ok(Err(error));
					}
				},
				tangram_index::batch::Item::PutSandbox(arg) => {
					let result = Self::put_sandboxes_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
						usage_partition_total,
					)?;
					if let Err(error) = result {
						return Ok(Err(error));
					}
				},
				tangram_index::batch::Item::PutTag(arg) => {
					Self::put_tags_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::batch::Item::PutUser(arg) => {
					Self::put_users_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
			}
		}

		Ok(Ok(()))
	}
}
