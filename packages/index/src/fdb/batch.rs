use {
	super::{Index, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
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
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::batch::Arg,
		partition_total: u64,
	) -> tg::Result<()> {
		for item in &arg.items {
			match item {
				crate::batch::Item::DeleteGrant(arg) => {
					Self::delete_grants_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					)
					.await?;
				},
				crate::batch::Item::DeleteGroup(id) => {
					Self::delete_groups_with_transaction(txn, subspace, std::slice::from_ref(id))
						.await?;
				},
				crate::batch::Item::DeleteGroupMember(arg) => {
					Self::delete_group_members_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					)?;
				},
				crate::batch::Item::DeleteOrganization(id) => {
					Self::delete_organizations_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(id),
					)
					.await?;
				},
				crate::batch::Item::DeleteOrganizationMember(arg) => {
					Self::delete_organization_members_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					)?;
				},
				crate::batch::Item::DeleteSandbox(id) => {
					Self::delete_sandboxes_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(id),
					)?;
				},
				crate::batch::Item::DeleteTag(id) => {
					Self::delete_tags_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(id),
						partition_total,
					)
					.await?;
				},
				crate::batch::Item::DeleteUser(id) => {
					Self::delete_users_with_transaction(txn, subspace, std::slice::from_ref(id))
						.await?;
				},
				crate::batch::Item::PutCacheEntry(arg) => {
					Self::put_cache_entries_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					)?;
				},
				crate::batch::Item::PutGrant(arg) => {
					Self::put_grants_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					)
					.await?;
				},
				crate::batch::Item::PutGroup(arg) => {
					Self::put_groups_with_transaction(txn, subspace, std::slice::from_ref(arg))?;
				},
				crate::batch::Item::PutGroupMember(arg) => {
					Self::put_group_members_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					)?;
				},
				crate::batch::Item::PutObject(arg) => {
					Self::put_objects_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					)
					.await?;
				},
				crate::batch::Item::PutOrganization(arg) => {
					Self::put_organizations_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					)?;
				},
				crate::batch::Item::PutOrganizationMember(arg) => {
					Self::put_organization_members_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
					)?;
				},
				crate::batch::Item::PutProcess(arg) => {
					Self::put_processes_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					)
					.await?;
				},
				crate::batch::Item::PutRunner(arg) => {
					Self::put_runners_with_transaction(txn, subspace, std::slice::from_ref(arg))
						.await?;
				},
				crate::batch::Item::PutSandbox(arg) => {
					Self::put_sandboxes_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					)
					.await?;
				},
				crate::batch::Item::PutTag(arg) => {
					Self::put_tags_with_transaction(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					)
					.await?;
				},
				crate::batch::Item::PutUser(arg) => {
					Self::put_users_with_transaction(txn, subspace, std::slice::from_ref(arg))?;
				},
			}
		}

		Ok(())
	}
}
