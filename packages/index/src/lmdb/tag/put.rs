use {
	crate::lmdb::{Db, Index, Key, Request},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_tags(&self, args: &[crate::tag::put::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::PutTags(args.to_vec());
		self.sender_high
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub(crate) fn task_put_tags(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::tag::put::Arg],
	) -> tg::Result<()> {
		for arg in args {
			Self::put_tag(db, subspace, transaction, arg)?;
		}
		Ok(())
	}

	fn put_tag(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::tag::put::Arg,
	) -> tg::Result<()> {
		let key = Key::Tag(crate::lmdb::tag::Key::Tag(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let tag = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the tag"))?
			.map(crate::tag::Tag::deserialize)
			.transpose()?;
		if let Some(tag) = tag.as_ref()
			&& tag.item != arg.item
		{
			let item = match &tag.item {
				tg::Either::Left(id) => id.to_bytes().to_vec(),
				tg::Either::Right(id) => id.to_bytes().to_vec(),
			};
			let key = Key::Tag(crate::lmdb::tag::Key::ItemTag {
				item,
				tag: arg.id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the old item tag"))?;

			match &tag.item {
				tg::Either::Left(id) => {
					Self::decrement_object_reference_count(db, subspace, transaction, id)?;
				},
				tg::Either::Right(id) => {
					Self::decrement_process_reference_count(db, subspace, transaction, id)?;
				},
			}
		}
		if let Some(tag) = tag.as_ref()
			&& (tag.parent != arg.parent || tag.name != arg.name)
		{
			let key = Key::Tag(crate::lmdb::tag::Key::ParentTag {
				parent: tag.parent.clone(),
				name: tag.name.clone(),
				tag: arg.id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the old parent tag"))?;

			let key = Key::Tag(crate::lmdb::tag::Key::TagParent {
				tag: arg.id.clone(),
				parent: tag.parent.clone(),
				name: tag.name.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the old tag parent"))?;
		}
		if let Some(tag) = tag.as_ref()
			&& tag.specifier != arg.specifier
		{
			let key = Key::Node(crate::lmdb::node::Key::Node(tag.specifier.clone()));
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the old node"))?;
		}

		let key = Key::Tag(crate::lmdb::tag::Key::Tag(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let value = crate::tag::Tag {
			item: arg.item.clone(),
			name: arg.name.clone(),
			parent: arg.parent.clone(),
			specifier: arg.specifier.clone(),
		}
		.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the tag"))?;

		let key = Key::Node(crate::lmdb::node::Key::Node(arg.specifier.clone()));
		let key = Self::pack(subspace, &key);
		let value = tg::Id::from(arg.id.clone()).to_bytes();
		db.put(transaction, &key, value.as_ref())
			.map_err(|error| tg::error!(!error, "failed to put the node"))?;

		let item = match &arg.item {
			tg::Either::Left(id) => id.to_bytes().to_vec(),
			tg::Either::Right(id) => id.to_bytes().to_vec(),
		};
		let key = Key::Tag(crate::lmdb::tag::Key::ItemTag {
			item,
			tag: arg.id.clone(),
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the item tag"))?;

		let key = Key::Tag(crate::lmdb::tag::Key::ParentTag {
			parent: arg.parent.clone(),
			name: arg.name.clone(),
			tag: arg.id.clone(),
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the parent tag"))?;

		let key = Key::Tag(crate::lmdb::tag::Key::TagParent {
			tag: arg.id.clone(),
			parent: arg.parent.clone(),
			name: arg.name.clone(),
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the tag parent"))?;

		Ok(())
	}
}
