#![allow(clippy::unnecessary_wraps)]

use {
	crate::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_organizations(
		&self,
		args: &[tangram_index::organization::put::Arg],
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
		args: &[tangram_index::organization::member::put::Arg],
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
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		args: &[tangram_index::organization::put::Arg],
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for arg in args {
			let key = Key::Organization(crate::organization::Key::Organization(arg.id.clone()));
			let key = Self::pack(subspace, &key);
			let billing_ready = if let Some(billing_ready) = arg.billing_ready {
				billing_ready
			} else {
				let result = txn.get(&key, false).await;
				crate::retry!(result).map_or(Ok(false), |bytes| {
					tangram_index::organization::Organization::deserialize(&bytes)
						.map(|organization| organization.billing_ready)
				})?
			};
			let value = tangram_index::organization::Organization {
				billing_ready,
				specifier: arg.specifier.clone(),
			}
			.serialize()?;
			txn.set(&key, &value);

			let key = Key::Node(crate::node::Key::Node(arg.specifier.clone()));
			let key = Self::pack(subspace, &key);
			let value = tg::Id::from(arg.id.clone()).to_bytes();
			txn.set(&key, value.as_ref());
		}
		Ok(ControlFlow::Break(()))
	}

	pub(crate) fn put_organization_members_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		args: &[tangram_index::organization::member::put::Arg],
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for arg in args {
			let key = Key::Organization(crate::organization::Key::OrganizationMember {
				organization: arg.organization.clone(),
				member: arg.member.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			let key = Key::Organization(crate::organization::Key::MemberOrganization {
				member: arg.member.clone(),
				organization: arg.organization.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		Ok(ControlFlow::Break(()))
	}
}
