use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_organizations_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[tangram_index::organization::put::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Organization(crate::organization::Key::Organization(arg.id.clone()));
			let key = Self::pack(subspace, &key);
			let billing_ready = match arg.billing_ready {
				Some(billing_ready) => billing_ready,
				None => db
					.get(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to get the organization"))?
					.map_or(Ok(false), |bytes| {
						tangram_index::organization::Organization::deserialize(bytes)
							.map(|organization| organization.billing_ready)
					})?,
			};
			let value = tangram_index::organization::Organization {
				billing_ready,
				specifier: arg.specifier.clone(),
			}
			.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, "failed to put the organization"))?;

			let key = Key::Node(crate::node::Key::Node(arg.specifier.clone()));
			let key = Self::pack(subspace, &key);
			let value = tg::Id::from(arg.id.clone()).to_bytes();
			db.put(transaction, &key, value.as_ref())
				.map_err(|error| tg::error!(!error, "failed to put the node"))?;
		}
		Ok(())
	}

	pub(crate) fn put_organization_members_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[tangram_index::organization::member::put::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Organization(crate::organization::Key::OrganizationMember {
				organization: arg.organization.clone(),
				member: arg.member.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the organization member"))?;

			let key = Key::Organization(crate::organization::Key::MemberOrganization {
				member: arg.member.clone(),
				organization: arg.organization.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the member organization"))?;
		}
		Ok(())
	}
}
