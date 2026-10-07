#![allow(clippy::unnecessary_wraps)]

use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn delete_groups_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		ids: &[tg::group::Id],
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for id in ids {
			let key = Key::Group(crate::group::Key::Group(id.clone()));
			let key = Self::pack(subspace, &key);
			let result = txn.get(&key, false).await;
			let group = crate::retry!(result)
				.map(|bytes| tangram_index::group::Group::deserialize(&bytes))
				.transpose()?;
			if let Some(group) = group {
				let node_key = Key::Node(crate::node::Key::Node(group.specifier));
				let node_key = Self::pack(subspace, &node_key);
				txn.clear(&node_key);
			}
			txn.clear(&key);
		}
		Ok(ControlFlow::Break(()))
	}

	pub(crate) fn delete_group_members_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		args: &[tangram_index::group::member::delete::Arg],
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for arg in args {
			let key = Key::Group(crate::group::Key::GroupMember {
				group: arg.group.clone(),
				member: arg.member.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);

			let key = Key::Group(crate::group::Key::MemberGroup {
				member: arg.member.clone(),
				group: arg.group.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
		Ok(ControlFlow::Break(()))
	}
}
