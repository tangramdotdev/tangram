use {
	crate::fdb::{Index, Key, Request, Response},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_tags(&self, args: &[crate::tag::put::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::PutTags(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub(crate) async fn put_tags_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::tag::put::Arg],
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for arg in args {
			crate::fdb::propagate!(Self::put_tag(txn, subspace, arg, partition_total).await);
		}
		Ok(ControlFlow::Break(()))
	}

	async fn put_tag(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::tag::put::Arg,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = Key::Tag(crate::fdb::tag::Key::Tag(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let tag = crate::fdb::retry!(result)
			.map(|bytes| crate::tag::Tag::deserialize(&bytes))
			.transpose()?;
		if let Some(tag) = tag.as_ref()
			&& (tag.account != arg.account
				|| tag.specifier != arg.specifier
				|| tag.target != arg.target)
		{
			match &tag.target {
				tg::Either::Left(id) => {
					crate::fdb::propagate!(
						Self::schedule_object_accounts_for_cleaning(
							txn,
							subspace,
							id,
							partition_total,
						)
						.await
					);
				},
				tg::Either::Right(id) => {
					crate::fdb::propagate!(
						Self::schedule_process_accounts_for_cleaning(
							txn,
							subspace,
							id,
							partition_total,
						)
						.await
					);
				},
			}
		}
		if let Some(tag) = tag.as_ref()
			&& tag.target != arg.target
		{
			let target = match &tag.target {
				tg::Either::Left(id) => id.to_bytes().to_vec(),
				tg::Either::Right(id) => id.to_bytes().to_vec(),
			};
			let old_key = Key::Tag(crate::fdb::tag::Key::TargetTag {
				target,
				tag: arg.id.clone(),
			});
			let old_key = Self::pack(subspace, &old_key);
			txn.clear(&old_key);

			match &tag.target {
				tg::Either::Left(id) => {
					crate::fdb::propagate!(
						Self::decrement_object_reference_count(txn, subspace, id, partition_total)
							.await
					);
				},
				tg::Either::Right(id) => {
					crate::fdb::propagate!(
						Self::decrement_process_reference_count(txn, subspace, id, partition_total)
							.await
					);
				},
			}
		}
		if let Some(tag) = tag.as_ref()
			&& (tag.parent != arg.parent || tag.name != arg.name)
		{
			let parent_tag_key = Key::Tag(crate::fdb::tag::Key::ParentTag {
				parent: tag.parent.clone(),
				name: tag.name.clone(),
				tag: arg.id.clone(),
			});
			let parent_tag_key = Self::pack(subspace, &parent_tag_key);
			txn.clear(&parent_tag_key);

			let tag_parent_key = Key::Tag(crate::fdb::tag::Key::TagParent {
				tag: arg.id.clone(),
				parent: tag.parent.clone(),
				name: tag.name.clone(),
			});
			let tag_parent_key = Self::pack(subspace, &tag_parent_key);
			txn.clear(&tag_parent_key);
		}
		if let Some(tag) = tag.as_ref()
			&& tag.specifier != arg.specifier
		{
			let node_key = Key::Node(crate::fdb::node::Key::Node(tag.specifier.clone()));
			let node_key = Self::pack(subspace, &node_key);
			txn.clear(&node_key);
		}

		let key = Key::Tag(crate::fdb::tag::Key::Tag(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let value = crate::tag::Tag {
			account: arg.account.clone(),
			name: arg.name.clone(),
			parent: arg.parent.clone(),
			permissions: arg.permissions.clone(),
			specifier: arg.specifier.clone(),
			target: arg.target.clone(),
		}
		.serialize()?;
		txn.set(&key, &value);

		let node_key = Key::Node(crate::fdb::node::Key::Node(arg.specifier.clone()));
		let node_key = Self::pack(subspace, &node_key);
		let node_value = tg::Id::from(arg.id.clone()).to_bytes();
		txn.set(&node_key, node_value.as_ref());

		let target = match &arg.target {
			tg::Either::Left(id) => id.to_bytes().to_vec(),
			tg::Either::Right(id) => id.to_bytes().to_vec(),
		};
		let target_tag_key = Key::Tag(crate::fdb::tag::Key::TargetTag {
			target,
			tag: arg.id.clone(),
		});
		let target_tag_key = Self::pack(subspace, &target_tag_key);
		txn.set(&target_tag_key, &[]);

		let parent_tag_key = Key::Tag(crate::fdb::tag::Key::ParentTag {
			parent: arg.parent.clone(),
			name: arg.name.clone(),
			tag: arg.id.clone(),
		});
		let parent_tag_key = Self::pack(subspace, &parent_tag_key);
		txn.set(&parent_tag_key, &[]);

		let tag_parent_key = Key::Tag(crate::fdb::tag::Key::TagParent {
			tag: arg.id.clone(),
			parent: arg.parent.clone(),
			name: arg.name.clone(),
		});
		let tag_parent_key = Self::pack(subspace, &tag_parent_key);
		txn.set(&tag_parent_key, &[]);

		Ok(ControlFlow::Break(()))
	}
}
