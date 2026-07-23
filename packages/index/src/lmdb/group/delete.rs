use {
	crate::lmdb::{Db, Index, Key, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
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

	pub(crate) fn delete_groups_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::group::Id],
	) -> tg::Result<()> {
		for id in ids {
			let key = Key::Group(crate::lmdb::group::Key::Group(id.clone()));
			let key = Self::pack(subspace, &key);
			let group = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the group"))?
				.map(crate::group::Group::deserialize)
				.transpose()?;
			if let Some(group) = group {
				let key = Key::Node(crate::lmdb::node::Key::Node(group.specifier));
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
		args: &[crate::group::member::delete::Arg],
	) -> tg::Result<()> {
		for arg in args {
			let key = Key::Group(crate::lmdb::group::Key::GroupMember {
				group: arg.group.clone(),
				member: arg.member.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the group member"))?;

			let key = Key::Group(crate::lmdb::group::Key::MemberGroup {
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
