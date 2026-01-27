use {
	super::{Db, Index, Key, Request},
	crate::{PutTagArg, Tag},
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_tags(&self, args: &[PutTagArg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::PutTags(args.to_vec());
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub(super) fn task_put_tags(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[PutTagArg],
	) -> tg::Result<()> {
		for arg in args {
			Self::put_tag(db, transaction, arg)?;
		}
		Ok(())
	}

	fn put_tag(db: &Db, transaction: &mut lmdb::RwTxn<'_>, arg: &PutTagArg) -> tg::Result<()> {
		let key = Key::Tag(arg.tag.clone()).pack_to_vec();
		let tag = db
			.get(transaction, &key)
			.map_err(|source| tg::error!(!source, "failed to get the tag"))?
			.map(Tag::deserialize)
			.transpose()?;
		if let Some(tag) = tag
			&& tag.item != arg.item
		{
			let item = match &tag.item {
				tg::Either::Left(id) => id.to_bytes().to_vec(),
				tg::Either::Right(id) => id.to_bytes().to_vec(),
			};
			let key = Key::ItemTag {
				item,
				tag: arg.tag.clone(),
			}
			.pack_to_vec();
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete the old item tag"))?;

			match &tag.item {
				tg::Either::Left(id) => {
					Self::decrement_object_reference_count(db, transaction, id)?;
				},
				tg::Either::Right(id) => {
					Self::decrement_process_reference_count(db, transaction, id)?;
				},
			}
		}

		let key = Key::Tag(arg.tag.clone()).pack_to_vec();
		let value = Tag {
			item: arg.item.clone(),
		}
		.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|source| tg::error!(!source, "failed to put the tag"))?;

		let item = match &arg.item {
			tg::Either::Left(id) => id.to_bytes().to_vec(),
			tg::Either::Right(id) => id.to_bytes().to_vec(),
		};
		let key = Key::ItemTag {
			item,
			tag: arg.tag.clone(),
		}
		.pack_to_vec();
		db.put(transaction, &key, &[])
			.map_err(|source| tg::error!(!source, "failed to put the item tag"))?;

		Ok(())
	}

	pub async fn delete_tags(&self, tags: &[String]) -> tg::Result<()> {
		if tags.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteTags(tags.to_vec());
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub(super) fn task_delete_tags(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		tags: &[String],
	) -> tg::Result<()> {
		for tag in tags {
			let key = Key::Tag(tag.clone()).pack_to_vec();
			let value = db
				.get(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to get the tag"))?;
			// Skip tags that do not exist in the index. This can happen for "branch tags" which
			// exist in the database as parent nodes but have no associated item.
			let Some(bytes) = value else {
				continue;
			};
			let data = Tag::deserialize(bytes)?;
			let item = match &data.item {
				tg::Either::Left(id) => id.to_bytes().to_vec(),
				tg::Either::Right(id) => id.to_bytes().to_vec(),
			};
			let key = Key::ItemTag {
				item,
				tag: tag.clone(),
			}
			.pack_to_vec();
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete the item tag"))?;
			match &data.item {
				tg::Either::Left(id) => {
					Self::decrement_object_reference_count(db, transaction, id)?;
				},
				tg::Either::Right(id) => {
					Self::decrement_process_reference_count(db, transaction, id)?;
				},
			}
			let key = Key::Tag(tag.clone()).pack_to_vec();
			db.delete(transaction, &key)
				.map_err(|source| tg::error!(!source, "failed to delete the tag"))?;
		}
		Ok(())
	}
}
