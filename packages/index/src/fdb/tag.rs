use {
	super::{Index, Key, KeyKind, TagCoreField, TagField},
	crate::PutTagArg,
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_tags(&self, args: &[PutTagArg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		self.database
			.run(|txn, _| {
				let subspace = self.subspace.clone();
				let tags = args.to_vec();
				async move {
					Self::put_tags_inner(&txn, &subspace, &tags)
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
	) -> tg::Result<()> {
		for arg in args {
			Self::put_tag(txn, subspace, arg).await?;
		}
		Ok(())
	}

	async fn put_tag(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		arg: &PutTagArg,
	) -> tg::Result<()> {
		let key = Key::Tag {
			tag: arg.tag.clone(),
			field: TagField::Core(TagCoreField::Item),
		};
		let key = Self::pack(subspace, &key);
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the tag"))?
			.map(|item_bytes| {
				if let Ok(id) = tg::object::Id::from_slice(&item_bytes) {
					Ok(tg::Either::Left(id))
				} else if let Ok(id) = tg::process::Id::from_slice(&item_bytes) {
					Ok(tg::Either::Right(id))
				} else {
					Err(tg::error!("failed to parse the item"))
				}
			})
			.transpose()?;
		if let Some(item) = existing
			&& item != arg.item
		{
			let item_bytes = match &item {
				tg::Either::Left(id) => id.to_bytes().to_vec(),
				tg::Either::Right(id) => id.to_bytes().to_vec(),
			};
			let key = Key::ItemTag {
				item: item_bytes,
				tag: arg.tag.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.clear(&key);

			match &item {
				tg::Either::Left(id) => {
					Self::decrement_object_reference_count(txn, subspace, id).await?;
				},
				tg::Either::Right(id) => {
					Self::decrement_process_reference_count(txn, subspace, id).await?;
				},
			}
		}

		let key = Key::Tag {
			tag: arg.tag.clone(),
			field: TagField::Core(TagCoreField::Item),
		};
		let key = Self::pack(subspace, &key);
		let item = match &arg.item {
			tg::Either::Left(id) => id.to_bytes().to_vec(),
			tg::Either::Right(id) => id.to_bytes().to_vec(),
		};
		txn.set(&key, &item);

		let key = Self::pack(
			subspace,
			&Key::ItemTag {
				item,
				tag: arg.tag.clone(),
			},
		);
		txn.set(&key, &[]);

		Ok(())
	}

	pub async fn delete_tags(&self, tags: &[String]) -> tg::Result<()> {
		if tags.is_empty() {
			return Ok(());
		}
		self.database
			.run(|txn, _| {
				let subspace = self.subspace.clone();
				let tags = tags.to_vec();
				async move {
					Self::delete_tags_inner(&txn, &subspace, &tags)
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
	) -> tg::Result<()> {
		for tag in tags {
			Self::delete_tag(txn, subspace, tag).await?;
		}
		Ok(())
	}

	async fn delete_tag(txn: &fdb::Transaction, subspace: &Subspace, tag: &str) -> tg::Result<()> {
		let key = Self::pack(
			subspace,
			&Key::Tag {
				tag: tag.to_owned(),
				field: TagField::Core(TagCoreField::Item),
			},
		);
		let item = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get tag"))?;
		// Skip tags that do not exist in the index. This can happen for "branch tags" which
		// exist in the database as parent nodes but have no associated item.
		let Some(item) = item else {
			return Ok(());
		};
		let key = Self::pack(
			subspace,
			&Key::ItemTag {
				item: item.to_vec(),
				tag: tag.to_owned(),
			},
		);
		txn.clear(&key);

		if let Ok(id) = tg::object::Id::from_slice(&item) {
			Self::decrement_object_reference_count(txn, subspace, &id).await?;
		} else if let Ok(id) = tg::process::Id::from_slice(&item) {
			Self::decrement_process_reference_count(txn, subspace, &id).await?;
		} else {
			return Err(tg::error!("failed to parse tag item"));
		}

		let prefix = (KeyKind::Tag.to_i32().unwrap(), tag);
		let prefix = Self::pack(subspace, &prefix);
		let range_subspace = Subspace::from_bytes(prefix);
		let (begin, end) = range_subspace.range();
		txn.clear_range(&begin, &end);

		Ok(())
	}
}
