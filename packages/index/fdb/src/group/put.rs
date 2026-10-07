#![allow(clippy::unnecessary_wraps)]

use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_groups_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		args: &[tangram_index::group::put::Arg],
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for arg in args {
			let key = Key::Group(crate::group::Key::Group(arg.id.clone()));
			let key = Self::pack(subspace, &key);
			let value = tangram_index::group::Group {
				parent: arg.parent.clone(),
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

	pub(crate) fn put_group_members_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		args: &[tangram_index::group::member::put::Arg],
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for arg in args {
			let key = Key::Group(crate::group::Key::GroupMember {
				group: arg.group.clone(),
				member: arg.member.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			let key = Key::Group(crate::group::Key::MemberGroup {
				member: arg.member.clone(),
				group: arg.group.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		Ok(ControlFlow::Break(()))
	}
}
