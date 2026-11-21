#![allow(clippy::unnecessary_wraps)]

use {
	crate::{Index, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_groups_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[tangram_index::group::put::Arg],
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		// Put each group after the ancestor groups that it carries.
		let mut groups = Vec::new();
		for arg in args {
			groups.extend(&arg.groups);
			groups.push(arg);
		}
		for arg in groups {
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

			// Put the permissions, which put the visibility entries of the group and its ancestors.
			crate::propagate!(
				Self::put_permissions_with_transaction(
					txn,
					subspace,
					&arg.permissions,
					partition_totals
				)
				.await
			);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) fn put_group_members_with_transaction(
		txn: &fdb::Transaction,
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
