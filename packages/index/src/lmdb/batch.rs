use {
	super::{Db, Index, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
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
			.as_ref()
			.unwrap()
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

	pub(crate) fn task_batch(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::batch::Arg,
	) -> tg::Result<()> {
		for item in &arg.items {
			match item {
				crate::batch::Item::DeleteGrant(arg) => {
					Self::task_delete_grants(db, subspace, transaction, std::slice::from_ref(arg))?;
				},
				crate::batch::Item::DeleteGroup(id) => {
					Self::task_delete_groups(db, subspace, transaction, std::slice::from_ref(id))?;
				},
				crate::batch::Item::DeleteGroupMember(arg) => {
					Self::task_delete_group_members(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				crate::batch::Item::DeleteOrganization(id) => {
					Self::task_delete_organizations(
						db,
						subspace,
						transaction,
						std::slice::from_ref(id),
					)?;
				},
				crate::batch::Item::DeleteOrganizationMember(arg) => {
					Self::task_delete_organization_members(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				crate::batch::Item::DeleteSandbox(id) => {
					Self::task_delete_sandboxes(
						db,
						subspace,
						transaction,
						std::slice::from_ref(id),
					)?;
				},
				crate::batch::Item::DeleteTag(id) => {
					Self::task_delete_tags(db, subspace, transaction, std::slice::from_ref(id))?;
				},
				crate::batch::Item::DeleteUser(id) => {
					Self::task_delete_users(db, subspace, transaction, std::slice::from_ref(id))?;
				},
				crate::batch::Item::PutCacheEntry(arg) => {
					Self::task_put_cache_entries(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				crate::batch::Item::PutGrant(arg) => {
					Self::task_put_grants(db, subspace, transaction, std::slice::from_ref(arg))?;
				},
				crate::batch::Item::PutGroup(arg) => {
					Self::task_put_groups(db, subspace, transaction, std::slice::from_ref(arg))?;
				},
				crate::batch::Item::PutGroupMember(arg) => {
					Self::task_put_group_members(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				crate::batch::Item::PutObject(arg) => {
					Self::task_put_objects(db, subspace, transaction, std::slice::from_ref(arg))?;
				},
				crate::batch::Item::PutOrganization(arg) => {
					Self::task_put_organizations(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				crate::batch::Item::PutOrganizationMember(arg) => {
					Self::task_put_organization_members(
						db,
						subspace,
						transaction,
						std::slice::from_ref(arg),
					)?;
				},
				crate::batch::Item::PutProcess(arg) => {
					Self::task_put_processes(db, subspace, transaction, std::slice::from_ref(arg))?;
				},
				crate::batch::Item::PutRunner(arg) => {
					Self::task_put_runners(db, subspace, transaction, std::slice::from_ref(arg))?;
				},
				crate::batch::Item::PutSandbox(arg) => {
					Self::task_put_sandboxes(db, subspace, transaction, std::slice::from_ref(arg))?;
				},
				crate::batch::Item::PutTag(arg) => {
					Self::task_put_tags(db, subspace, transaction, std::slice::from_ref(arg))?;
				},
				crate::batch::Item::PutUser(arg) => {
					Self::task_put_users(db, subspace, transaction, std::slice::from_ref(arg))?;
				},
			}
		}

		Ok(())
	}
}
