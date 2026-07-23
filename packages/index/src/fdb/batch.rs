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
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Batch(arg);
		self.sender_medium
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected response"));
		};

		Ok(())
	}

	pub(crate) async fn task_batch(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::batch::Arg,
		partition_total: u64,
	) -> tg::Result<()> {
		for item in &arg.items {
			match item {
				crate::batch::Item::DeleteGrant(arg) => {
					Self::task_delete_grants(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					)
					.await?;
				},
				crate::batch::Item::DeleteGroup(id) => {
					Self::task_delete_groups(txn, subspace, std::slice::from_ref(id)).await?;
				},
				crate::batch::Item::DeleteGroupMember(arg) => {
					Self::task_delete_group_members(txn, subspace, std::slice::from_ref(arg))?;
				},
				crate::batch::Item::DeleteOrganization(id) => {
					Self::task_delete_organizations(txn, subspace, std::slice::from_ref(id))
						.await?;
				},
				crate::batch::Item::DeleteOrganizationMember(arg) => {
					Self::task_delete_organization_members(
						txn,
						subspace,
						std::slice::from_ref(arg),
					)?;
				},
				crate::batch::Item::DeleteSandbox(id) => {
					Self::task_delete_sandboxes(txn, subspace, std::slice::from_ref(id))?;
				},
				crate::batch::Item::DeleteTag(id) => {
					Self::task_delete_tags(
						txn,
						subspace,
						std::slice::from_ref(id),
						partition_total,
					)
					.await?;
				},
				crate::batch::Item::DeleteUser(id) => {
					Self::task_delete_users(txn, subspace, std::slice::from_ref(id)).await?;
				},
				crate::batch::Item::PutCacheEntry(arg) => {
					Self::task_put_cache_entries(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					)?;
				},
				crate::batch::Item::PutGrant(arg) => {
					Self::task_put_grants(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					)
					.await?;
				},
				crate::batch::Item::PutGroup(arg) => {
					Self::task_put_groups(txn, subspace, std::slice::from_ref(arg))?;
				},
				crate::batch::Item::PutGroupMember(arg) => {
					Self::task_put_group_members(txn, subspace, std::slice::from_ref(arg))?;
				},
				crate::batch::Item::PutObject(arg) => {
					Self::task_put_objects(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					)
					.await?;
				},
				crate::batch::Item::PutOrganization(arg) => {
					Self::task_put_organizations(txn, subspace, std::slice::from_ref(arg))?;
				},
				crate::batch::Item::PutOrganizationMember(arg) => {
					Self::task_put_organization_members(txn, subspace, std::slice::from_ref(arg))?;
				},
				crate::batch::Item::PutProcess(arg) => {
					Self::task_put_processes(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					)
					.await?;
				},
				crate::batch::Item::PutRunner(arg) => {
					Self::task_put_runners(txn, subspace, std::slice::from_ref(arg)).await?;
				},
				crate::batch::Item::PutSandbox(arg) => {
					Self::task_put_sandboxes(
						txn,
						subspace,
						std::slice::from_ref(arg),
						partition_total,
					)
					.await?;
				},
				crate::batch::Item::PutTag(arg) => {
					Self::task_put_tags(txn, subspace, std::slice::from_ref(arg), partition_total)
						.await?;
				},
				crate::batch::Item::PutUser(arg) => {
					Self::task_put_users(txn, subspace, std::slice::from_ref(arg))?;
				},
			}
		}

		Ok(())
	}
}
