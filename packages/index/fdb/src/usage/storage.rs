use {
	crate::{Index, Key, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::ControlFlow,
	},
	tangram_client::prelude::*,
};

mod clean;
mod put;

impl Index {
	async fn get_tag_storage_permissions_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		resource: &tg::Id,
	) -> tg::Result<
		ControlFlow<
			BTreeMap<tangram_index::usage::Account, tg::authorization::permission::Set>,
			fdb::FdbError,
		>,
	> {
		let subjects = crate::propagate!(
			Self::get_resource_permission_subjects_with_transaction(txn, subspace, resource).await
		);
		let mut accounts = BTreeMap::new();
		for subject in subjects {
			let tg::authorization::Subject::Tag(id) = &subject else {
				continue;
			};
			let Some(tag) =
				crate::propagate!(Self::try_get_tag_with_transaction(txn, subspace, id).await)
			else {
				continue;
			};
			let Some(account) = tag.account else {
				continue;
			};
			let entries = crate::propagate!(
				Self::get_resource_permission_entries_for_subject_with_transaction(
					txn, subspace, resource, &subject
				)
				.await
			);
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

		Ok(ControlFlow::Break(accounts))
	}

	async fn get_account_storage_entry_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
	) -> tg::Result<ControlFlow<Option<tangram_index::usage::storage::Entry>, fdb::FdbError>> {
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
		let result = txn.get(&key, false).await;
		let value = crate::retry!(result);
		let entry = value
			.map(|value| tangram_index::usage::storage::Entry::deserialize(&value))
			.transpose()?;

		Ok(ControlFlow::Break(entry))
	}

	async fn compute_account_storage_permissions_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		account: &tangram_index::usage::Account,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
	) -> tg::Result<ControlFlow<(tg::authorization::permission::Set, u64), fdb::FdbError>> {
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
			tg::Either::Left(object) => crate::propagate!(
				Self::get_object_parents_with_transaction(txn, subspace, object).await
			)
			.into_iter()
			.map(tg::Either::Left)
			.collect::<Vec<_>>(),
			tg::Either::Right(process) => crate::propagate!(
				Self::get_process_parents_with_transaction(txn, subspace, process).await
			)
			.into_iter()
			.map(tg::Either::Right)
			.collect::<Vec<_>>(),
		};
		for parent in parents {
			if let Some(entry) = crate::propagate!(
				Self::get_account_storage_entry_with_transaction(txn, subspace, account, &parent)
					.await
			) {
				let inherited = tangram_index::usage::storage::child_permissions(entry.permissions);
				if !inherited.is_empty() {
					permissions.insert(inherited);
					count += 1;
				}
			}
		}
		if let tg::Either::Left(object) = id {
			let processes = crate::propagate!(
				Self::get_object_processes_with_transaction(txn, subspace, object).await
			);
			for (process, kind) in processes {
				if let Some(entry) = crate::propagate!(
					Self::get_account_storage_entry_with_transaction(
						txn,
						subspace,
						account,
						&tg::Either::Right(process)
					)
					.await
				) {
					let inherited =
						tangram_index::usage::storage::object_permissions(entry.permissions, kind);
					if !inherited.is_empty() {
						permissions.insert(inherited);
						count += 1;
					}
				}
			}
		}
		let accounts = crate::propagate!(
			Self::get_tag_storage_permissions_with_transaction(txn, subspace, &resource).await
		);
		if let Some(captured) = accounts.get(account) {
			permissions.insert(*captured);
			count += 1;
		}

		Ok(ControlFlow::Break((permissions, count)))
	}
	pub(crate) async fn get_tag_storage_resources_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::tag::Id,
	) -> tg::Result<ControlFlow<BTreeSet<tg::Id>, fdb::FdbError>> {
		let subject = tg::authorization::Subject::Tag(id.clone());
		let prefix = Self::pack(
			subspace,
			&(
				Kind::SubjectPermission.to_i32().unwrap(),
				subject.to_string(),
			),
		);
		let range = fdb::RangeOption::from(&fdbt::Subspace::from_bytes(prefix));
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let keys = entries
			.iter()
			.map(|entry| Self::unpack(subspace, entry.key()))
			.collect::<tg::Result<Vec<_>>>()?;
		let mut resources = BTreeSet::new();
		for key in keys {
			let Key::Permission(crate::permission::Key::SubjectPermission { resource, .. }) = key
			else {
				return Err(tg::error!("expected a subject permission key"));
			};
			resources.insert(resource);
		}

		Ok(ControlFlow::Break(resources))
	}
}
