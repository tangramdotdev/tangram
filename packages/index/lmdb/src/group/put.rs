use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_groups_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[tangram_index::group::put::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Group(crate::group::Key::Group(arg.id.clone()));
			let key = Self::pack(subspace, &key);
			let value = tangram_index::group::Group {
				parent: arg.parent.clone(),
				specifier: arg.specifier.clone(),
			}
			.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, "failed to put the group"))?;

			let key = Key::Node(crate::node::Key::Node(arg.specifier.clone()));
			let key = Self::pack(subspace, &key);
			let value = tg::Id::from(arg.id.clone()).to_bytes();
			db.put(transaction, &key, value.as_ref())
				.map_err(|error| tg::error!(!error, "failed to put the node"))?;
		}
		Ok(())
	}

	pub(crate) fn put_group_members_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[tangram_index::group::member::put::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Group(crate::group::Key::GroupMember {
				group: arg.group.clone(),
				member: arg.member.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the group member"))?;

			let key = Key::Group(crate::group::Key::MemberGroup {
				member: arg.member.clone(),
				group: arg.group.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the member group"))?;
		}
		Ok(())
	}
}
