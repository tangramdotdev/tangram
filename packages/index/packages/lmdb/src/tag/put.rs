use {
	crate::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_tags_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[tangram_index::tag::put::Arg],
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
		arg: &tangram_index::tag::put::Arg,
	) -> tg::Result<()> {
		let key = Key::Tag(crate::tag::Key::Tag(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let tag = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the tag"))?
			.map(tangram_index::tag::Tag::deserialize)
			.transpose()?;
		if let Some(tag) = &tag {
			if tag.version == arg.version && tag.target != arg.target {
				return Err(tg::error!("the tag target changed without a new version"));
			}
			if tag.version != arg.version {
				let subject = tg::authorization::Subject::Tag(arg.id.clone());
				Self::delete_subject_permissions_with_transaction(
					db,
					subspace,
					transaction,
					&subject,
				)?;
				Self::delete_delegations_for_subject_with_transaction(
					db,
					subspace,
					transaction,
					&subject,
				)?;
			}
		}
		if let Some(tag) = tag.as_ref()
			&& (tag.account != arg.account
				|| tag.specifier != arg.specifier
				|| tag.target != arg.target)
		{
			match &tag.target {
				tg::Either::Left(id) => {
					Self::schedule_object_accounts_for_cleaning(db, subspace, transaction, id)?;
				},
				tg::Either::Right(id) => {
					Self::schedule_process_accounts_for_cleaning(db, subspace, transaction, id)?;
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
			let key = Key::Tag(crate::tag::Key::TargetTag {
				target,
				tag: arg.id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the old target tag"))?;

			match &tag.target {
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
			let key = Key::Tag(crate::tag::Key::ParentTag {
				parent: tag.parent.clone(),
				name: tag.name.clone(),
				tag: arg.id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the old parent tag"))?;

			let key = Key::Tag(crate::tag::Key::TagParent {
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
			let key = Key::Node(crate::node::Key::Node(tag.specifier.clone()));
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the old node"))?;
		}

		let key = Key::Tag(crate::tag::Key::Tag(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let value = tangram_index::tag::Tag {
			account: arg.account.clone(),
			name: arg.name.clone(),
			parent: arg.parent.clone(),
			specifier: arg.specifier.clone(),
			target: arg.target.clone(),
			version: arg.version.clone(),
		}
		.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to put the tag"))?;

		let key = Key::Node(crate::node::Key::Node(arg.specifier.clone()));
		let key = Self::pack(subspace, &key);
		let value = tg::Id::from(arg.id.clone()).to_bytes();
		db.put(transaction, &key, value.as_ref())
			.map_err(|error| tg::error!(!error, "failed to put the node"))?;

		let target = match &arg.target {
			tg::Either::Left(id) => id.to_bytes().to_vec(),
			tg::Either::Right(id) => id.to_bytes().to_vec(),
		};
		let key = Key::Tag(crate::tag::Key::TargetTag {
			target,
			tag: arg.id.clone(),
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the target tag"))?;

		let key = Key::Tag(crate::tag::Key::ParentTag {
			parent: arg.parent.clone(),
			name: arg.name.clone(),
			tag: arg.id.clone(),
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the parent tag"))?;

		let key = Key::Tag(crate::tag::Key::TagParent {
			tag: arg.id.clone(),
			parent: arg.parent.clone(),
			name: arg.name.clone(),
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the tag parent"))?;

		if tag.as_ref().is_none_or(|tag| tag.account != arg.account) {
			let resources = Self::get_tag_storage_resources_with_transaction(
				db,
				subspace,
				transaction,
				&arg.id,
			)?;
			for resource in resources {
				if let Ok(object) = tg::object::Id::try_from(resource.clone()) {
					Self::schedule_object_accounts_for_cleaning(
						db,
						subspace,
						transaction,
						&object,
					)?;
					Self::enqueue_account_object_from_parents(
						db,
						subspace,
						transaction,
						&object,
						arg.touched_at,
					)?;
				} else if let Ok(process) = tg::process::Id::try_from(resource) {
					Self::schedule_process_accounts_for_cleaning(
						db,
						subspace,
						transaction,
						&process,
					)?;
					Self::enqueue_account_process_from_parents(
						db,
						subspace,
						transaction,
						&process,
						arg.touched_at,
					)?;
				}
			}
		}

		Ok(())
	}
}
