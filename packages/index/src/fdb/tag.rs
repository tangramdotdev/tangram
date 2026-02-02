use {
	super::{Index, Key},
	crate::{PutTagArg, Tag},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_tags(&self, args: &[PutTagArg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let partition_total = self.partition_total;
		self.database
			.run(|txn, _| {
				let subspace = self.subspace.clone();
				let tags = args.to_vec();
				async move {
					Self::put_tags_inner(&txn, &subspace, &tags, partition_total)
						.await
						.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to put tags"))
	}

	async fn put_tags_inner(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		args: &[PutTagArg],
		partition_total: u64,
	) -> tg::Result<()> {
		for arg in args {
			Self::put_tag(txn, subspace, arg, partition_total).await?;
		}
		Ok(())
	}

	async fn put_tag(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		arg: &PutTagArg,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::Tag(arg.tag.clone());
		let key = Self::pack(subspace, &key);
		let tag = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the tag"))?
			.map(|bytes| Tag::deserialize(&bytes))
			.transpose()?;
		if let Some(tag) = tag
			&& tag.item != arg.item
		{
			let item = match &tag.item {
				tg::Either::Left(id) => id.to_bytes().to_vec(),
				tg::Either::Right(id) => id.to_bytes().to_vec(),
			};
			let old_key = Key::ItemTag {
				item,
				tag: arg.tag.clone(),
			};
			let old_key = Self::pack(subspace, &old_key);
			txn.clear(&old_key);

			match &tag.item {
				tg::Either::Left(id) => {
					Self::decrement_object_reference_count(txn, subspace, id, partition_total)
						.await?;
				},
				tg::Either::Right(id) => {
					Self::decrement_process_reference_count(txn, subspace, id, partition_total)
						.await?;
				},
			}
		}

		let key = Key::Tag(arg.tag.clone());
		let key = Self::pack(subspace, &key);
		let value = Tag {
			item: arg.item.clone(),
		}
		.serialize()?;
		txn.set(&key, &value);

		let item = match &arg.item {
			tg::Either::Left(id) => id.to_bytes().to_vec(),
			tg::Either::Right(id) => id.to_bytes().to_vec(),
		};
		let item_tag_key = Key::ItemTag {
			item,
			tag: arg.tag.clone(),
		};
		let item_tag_key = Self::pack(subspace, &item_tag_key);
		txn.set(&item_tag_key, &[]);

		Ok(())
	}

	pub async fn delete_tags(&self, tags: &[String]) -> tg::Result<()> {
		if tags.is_empty() {
			return Ok(());
		}
		let partition_total = self.partition_total;
		self.database
			.run(|txn, _| {
				let subspace = self.subspace.clone();
				let tags = tags.to_vec();
				async move {
					Self::delete_tags_inner(&txn, &subspace, &tags, partition_total)
						.await
						.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to delete tags"))
	}

	async fn delete_tags_inner(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		tags: &[String],
		partition_total: u64,
	) -> tg::Result<()> {
		for tag in tags {
			Self::delete_tag(txn, subspace, tag, partition_total).await?;
		}
		Ok(())
	}

	async fn delete_tag(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		tag: &str,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::Tag(tag.to_owned());
		let key = Self::pack(subspace, &key);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get tag"))?;
		let Some(bytes) = bytes else {
			return Ok(());
		};
		let data = Tag::deserialize(&bytes)?;
		let item = match &data.item {
			tg::Either::Left(id) => id.to_bytes().to_vec(),
			tg::Either::Right(id) => id.to_bytes().to_vec(),
		};
		let item_tag_key = Key::ItemTag {
			item,
			tag: tag.to_owned(),
		};
		let item_tag_key = Self::pack(subspace, &item_tag_key);
		txn.clear(&item_tag_key);

		match &data.item {
			tg::Either::Left(id) => {
				Self::decrement_object_reference_count(txn, subspace, id, partition_total).await?;
			},
			tg::Either::Right(id) => {
				Self::decrement_process_reference_count(txn, subspace, id, partition_total).await?;
			},
		}

		txn.clear(&key);

		Ok(())
	}
}
