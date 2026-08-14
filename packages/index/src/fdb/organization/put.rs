#![allow(clippy::unnecessary_wraps)]

use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_organizations(
		&self,
		args: &[crate::organization::put::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::PutOrganizations(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub async fn put_organization_members(
		&self,
		args: &[crate::organization::member::put::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::PutOrganizationMembers(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub(crate) async fn put_organizations_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::organization::put::Arg],
	) -> crate::fdb::Result<()> {
		for arg in args {
			let key =
				Key::Organization(crate::fdb::organization::Key::Organization(arg.id.clone()));
			let key = Self::pack(subspace, &key);
			let billing = match arg.billing {
				Some(billing) => billing,
				None => txn
					.get(&key, false)
					.await?
					.map_or(Ok(false), |bytes| {
						crate::organization::Organization::deserialize(&bytes)
							.map(|organization| organization.billing)
					})
					.map_err(crate::fdb::custom_error)?,
			};
			let value = crate::organization::Organization {
				billing,
				specifier: arg.specifier.clone(),
			}
			.serialize()
			.map_err(crate::fdb::custom_error)?;
			txn.set(&key, &value);

			let key = Key::Node(crate::fdb::node::Key::Node(arg.specifier.clone()));
			let key = Self::pack(subspace, &key);
			let value = tg::Id::from(arg.id.clone()).to_bytes();
			txn.set(&key, value.as_ref());
		}
		Ok(())
	}

	pub(crate) fn put_organization_members_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::organization::member::put::Arg],
	) -> crate::fdb::Result<()> {
		for arg in args {
			let key = Key::Organization(crate::fdb::organization::Key::OrganizationMember {
				organization: arg.organization.clone(),
				member: arg.member.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			let key = Key::Organization(crate::fdb::organization::Key::MemberOrganization {
				member: arg.member.clone(),
				organization: arg.organization.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		Ok(())
	}
}
