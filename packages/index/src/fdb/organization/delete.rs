#![allow(clippy::unnecessary_wraps)]

use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_organizations(&self, ids: &[tg::organization::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let request = Request::DeleteOrganizations(ids.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub async fn delete_organization_members(
		&self,
		args: &[crate::organization::member::delete::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::DeleteOrganizationMembers(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub(crate) async fn delete_organizations_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		ids: &[tg::organization::Id],
	) -> tg::Result<()> {
		for id in ids {
			let key = Key::Organization(crate::fdb::organization::Key::Organization(id.clone()));
			let key = Self::pack(subspace, &key);
			let organization = txn
				.get(&key, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the organization"))?
				.map(|bytes| crate::organization::Organization::deserialize(&bytes))
				.transpose()?;
			if let Some(organization) = organization {
				let node_key = Key::Node(crate::fdb::node::Key::Node(organization.specifier));
				let node_key = Self::pack(subspace, &node_key);
				txn.clear(&node_key);
			}
			txn.clear(&key);
		}
		Ok(())
	}

	pub(crate) fn delete_organization_members_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::organization::member::delete::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Organization(crate::fdb::organization::Key::OrganizationMember {
				organization: arg.organization.clone(),
				member: arg.member.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);

			let key = Key::Organization(crate::fdb::organization::Key::MemberOrganization {
				member: arg.member.clone(),
				organization: arg.organization.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
		Ok(())
	}
}
