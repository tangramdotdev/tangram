use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn delete_groups_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::group::Id],
	) -> tg::Result<()> {
		for id in ids {
			let key = Key::Group(crate::group::Key::Group(id.clone()));
			let key = Self::pack(subspace, &key);
			let group = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the group"))?
				.map(tangram_index::group::Group::deserialize)
				.transpose()?;
			if let Some(group) = group {
				let key = Key::Node(crate::node::Key::Node(group.specifier));
				let key = Self::pack(subspace, &key);
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete the node"))?;
			}
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the group"))?;
		}
		Ok(())
	}

	pub(crate) fn delete_group_members_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[tangram_index::group::member::delete::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Group(crate::group::Key::GroupMember {
				group: arg.group.clone(),
				member: arg.member.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the group member"))?;

			let key = Key::Group(crate::group::Key::MemberGroup {
				member: arg.member.clone(),
				group: arg.group.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the member group"))?;
		}
		Ok(())
	}
}
