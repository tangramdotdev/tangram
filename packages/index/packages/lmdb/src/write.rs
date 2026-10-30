use {
	super::{Db, Index, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn write(&self, arg: tangram_index::write::Arg) -> tg::Result<tg::Result<()>> {
		if arg.is_empty() {
			return Ok(Ok(()));
		}
		let request = Request::Write(arg);
		let response = self.send_write_request(request).await?;
		let Response::Mutation(result) = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(result)
	}

	pub(crate) fn write_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::write::Arg,
		process_data_fragment_size: usize,
		usage_partition_total: u64,
	) -> tg::Result<tg::Result<()>> {
		let mut rejection = None;
		for item in &arg.items {
			// Skip a grant from a finished process to a process that it does not list as a child.
			if let Some((source, destination)) = item.process_grant()
				&& Self::try_get_process_accepts_child_with_transaction(
					db,
					subspace,
					transaction,
					&source,
					&destination,
				)? == Some(false)
			{
				continue;
			}

			match item {
				tangram_index::Item::DeleteDelegations(subject) => {
					Self::delete_delegations_for_subject_with_transaction(
						db,
						subspace,
						transaction,
						subject,
					)?;
				},

				tangram_index::Item::EnqueuePermissionCapture(arg) => {
					Self::enqueue_permission_capture_with_transaction(
						db,
						subspace,
						transaction,
						arg,
					)?;
				},

				tangram_index::Item::DeleteCheckout(id) => {
					Self::delete_checkout(db, subspace, transaction, id)?;
				},
				tangram_index::Item::DeletePermission(arg) => {
					Self::delete_permissions_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::Item::DeleteGroup(id) => {
					Self::delete_groups_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(id),
					)?;
				},
				tangram_index::Item::DeleteGroupMember(arg) => {
					Self::delete_group_members_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::Item::DeleteOrganization(id) => {
					Self::delete_organizations_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(id),
					)?;
				},
				tangram_index::Item::DeleteOrganizationMember(arg) => {
					Self::delete_organization_members_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::Item::DeleteSandbox(id) => {
					Self::delete_sandboxes_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(id),
					)?;
				},
				tangram_index::Item::DeleteTag(id) => {
					Self::delete_tags_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(id),
					)?;
				},
				tangram_index::Item::DeleteUser(id) => {
					Self::delete_users_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(id),
					)?;
				},
				tangram_index::Item::PutCheckout(arg) => {
					Self::put_checkouts_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::Item::PutDelegation(arg) => {
					Self::put_delegation_with_transaction(db, subspace, transaction, arg)?;
				},
				tangram_index::Item::PutPermission(arg) => {
					Self::put_permissions_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::Item::PutGroup(arg) => {
					Self::put_groups_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::Item::PutGroupMember(arg) => {
					Self::put_group_members_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::Item::PutObject(arg) => {
					Self::put_objects_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
						usage_partition_total,
					)?;
				},
				tangram_index::Item::PutAccountObject(arg) => {
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
				tangram_index::Item::PutAccountProcess(arg) => {
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
				tangram_index::Item::PutOrganization(arg) => {
					Self::put_organizations_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::Item::PutOrganizationMember(arg) => {
					Self::put_organization_members_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::Item::PutProcess(arg) => {
					let result = Self::put_processes_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
						process_data_fragment_size,
						usage_partition_total,
					)?;
					if let Err(error) = result {
						rejection.get_or_insert(error);
					}
				},
				tangram_index::Item::PutSandbox(arg) => {
					let result = Self::put_sandboxes_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
						usage_partition_total,
					)?;
					if let Err(error) = result {
						rejection.get_or_insert(error);
					}
				},
				tangram_index::Item::PutSync(arg) => {
					Self::put_sync_with_transaction(db, subspace, transaction, arg)?;
				},
				tangram_index::Item::PutTag(arg) => {
					Self::put_tags_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				tangram_index::Item::PutUser(arg) => {
					Self::put_users_with_transaction(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
			}
		}

		// Return the first rejection, since a rejected item does not affect the other items.
		let result = rejection.map_or(Ok(()), Err);

		Ok(result)
	}
}
