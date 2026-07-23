#![allow(clippy::unnecessary_wraps)]

use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_groups(&self, ids: &[tg::group::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let request = Request::DeleteGroups(ids.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub async fn delete_group_members(
		&self,
		args: &[crate::group::member::delete::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::DeleteGroupMembers(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub(crate) async fn delete_groups_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		ids: &[tg::group::Id],
	) -> tg::Result<()> {
		for id in ids {
			let key = Key::Group(crate::fdb::group::Key::Group(id.clone()));
			let key = Self::pack(subspace, &key);
			let group = txn
				.get(&key, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the group"))?
				.map(|bytes| crate::group::Group::deserialize(&bytes))
				.transpose()?;
			if let Some(group) = group {
				let node_key = Key::Node(crate::fdb::node::Key::Node(group.specifier));
				let node_key = Self::pack(subspace, &node_key);
				txn.clear(&node_key);
			}
			txn.clear(&key);
		}
		Ok(())
	}

	pub(crate) fn delete_group_members_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::group::member::delete::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Group(crate::fdb::group::Key::GroupMember {
				group: arg.group.clone(),
				member: arg.member.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);

			let key = Key::Group(crate::fdb::group::Key::MemberGroup {
				member: arg.member.clone(),
				group: arg.group.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
		Ok(())
	}
}
