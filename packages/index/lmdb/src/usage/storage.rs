use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
};

mod clean;
mod put;

impl Index {
	fn get_tag_storage_permissions_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
	) -> tg::Result<BTreeMap<tangram_index::usage::Account, tg::authorization::permission::Set>> {
		let subjects = Self::get_resource_permission_subjects_with_transaction(
			db,
			subspace,
			transaction,
			resource,
		)?;
		let mut accounts = BTreeMap::new();
		for subject in subjects {
			let tg::authorization::Subject::Tag(id) = &subject else {
				continue;
			};
			let Some(tag) = Self::try_get_tag_with_transaction(db, subspace, transaction, id)?
			else {
				continue;
			};
			let Some(account) = tag.account else {
				continue;
			};
			let entries = Self::get_resource_permission_entries_for_subject_with_transaction(
				db,
				subspace,
				transaction,
				resource,
				&subject,
			)?;
			let mut permissions = tg::authorization::permission::Set::empty_for_kind(
				if resource.kind().is_object() {
					tg::authorization::ResourceKind::Object
				} else {
					tg::authorization::ResourceKind::Process
				},
			);
			for entry in entries {
				if entry.direct == Some(None) {
					permissions.insert(entry.permission.into());
				}
			}
			if !permissions.is_empty() {
				accounts
					.entry(account)
					.and_modify(|current: &mut tg::authorization::permission::Set| {
						current.insert(permissions);
					})
					.or_insert(permissions);
			}
		}

		Ok(accounts)
	}

	fn get_account_storage_entry_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		account: &tangram_index::usage::Account,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
	) -> tg::Result<Option<tangram_index::usage::storage::Entry>> {
		let key = match id {
			tg::Either::Left(object) => crate::usage::Key::AccountObject {
				account: account.clone(),
				object: object.clone(),
			},
			tg::Either::Right(process) => crate::usage::Key::AccountProcess {
				account: account.clone(),
				process: process.clone(),
			},
		};
		let key = Self::pack(subspace, &Key::Usage(key));
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get a storage entry"))?;
		let entry = value
			.map(tangram_index::usage::storage::Entry::deserialize)
			.transpose()?;

		Ok(entry)
	}

	fn compute_account_storage_permissions_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		account: &tangram_index::usage::Account,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
	) -> tg::Result<(tg::authorization::permission::Set, u64)> {
		let resource = match id {
			tg::Either::Left(object) => tg::Id::from(object.clone()),
			tg::Either::Right(process) => tg::Id::from(process.clone()),
		};
		let mut permissions =
			tg::authorization::permission::Set::empty_for_kind(if resource.kind().is_object() {
				tg::authorization::ResourceKind::Object
			} else {
				tg::authorization::ResourceKind::Process
			});
		let mut count = 0;
		let parents = match id {
			tg::Either::Left(object) => {
				Self::get_object_parents_with_transaction(db, subspace, transaction, object)?
					.into_iter()
					.map(tg::Either::Left)
					.collect::<Vec<_>>()
			},
			tg::Either::Right(process) => {
				Self::get_process_parents_with_transaction(db, subspace, transaction, process)?
					.into_iter()
					.map(tg::Either::Right)
					.collect::<Vec<_>>()
			},
		};
		for parent in parents {
			if let Some(entry) = Self::get_account_storage_entry_with_transaction(
				db,
				subspace,
				transaction,
				account,
				&parent,
			)? {
				let inherited = tangram_index::usage::storage::child_permissions(entry.permissions);
				if !inherited.is_empty() {
					permissions.insert(inherited);
					count += 1;
				}
			}
		}
		if let tg::Either::Left(object) = id {
			let processes =
				Self::get_object_processes_with_transaction(db, subspace, transaction, object)?;
			for (process, kind) in processes {
				if let Some(entry) = Self::get_account_storage_entry_with_transaction(
					db,
					subspace,
					transaction,
					account,
					&tg::Either::Right(process),
				)? {
					let inherited =
						tangram_index::usage::storage::object_permissions(entry.permissions, kind);
					if !inherited.is_empty() {
						permissions.insert(inherited);
						count += 1;
					}
				}
			}
		}
		let accounts = Self::get_tag_storage_permissions_with_transaction(
			db,
			subspace,
			transaction,
			&resource,
		)?;
		if let Some(captured) = accounts.get(account) {
			permissions.insert(*captured);
			count += 1;
		}

		Ok((permissions, count))
	}
	pub(crate) fn get_tag_storage_resources_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::tag::Id,
	) -> tg::Result<BTreeSet<tg::Id>> {
		let subject = tg::authorization::Subject::Tag(id.clone());
		let prefix = Self::pack(
			subspace,
			&(
				Kind::SubjectPermission.to_i32().unwrap(),
				subject.to_string(),
			),
		);
		let keys = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to read the captured permissions"))?
			.map(|entry| {
				let (key, _) = entry
					.map_err(|error| tg::error!(!error, "failed to read a captured permission"))?;
				Self::unpack(subspace, key)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let mut resources = BTreeSet::new();
		for key in keys {
			let Key::Permission(crate::permission::Key::SubjectPermission { resource, .. }) = key
			else {
				return Err(tg::error!("expected a subject permission key"));
			};
			resources.insert(resource);
		}

		Ok(resources)
	}
}
