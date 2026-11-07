use {
	crate::{Index, Key, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{TryStreamExt as _, future},
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
		txn: &fdb::Transaction,
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

		// Read the tag and the permission entries of every tag subject.
		let subjects = subjects
			.into_iter()
			.filter_map(|subject| match &subject {
				tg::authorization::Subject::Tag(id) => Some((id.clone(), subject)),
				_ => None,
			})
			.collect::<Vec<_>>();
		let results = future::try_join_all(subjects.iter().map(|(id, subject)| async move {
			let (tag, entries) = futures::try_join!(
				Self::try_get_tag_with_transaction(txn, subspace, id),
				Self::get_resource_permission_entries_for_subject_with_transaction(
					txn, subspace, resource, subject,
				),
			)?;
			Ok::<_, tg::Error>((tag, entries))
		}))
		.await?;

		// Merge the permissions by account.
		let mut accounts = BTreeMap::new();
		for (tag, entries) in results {
			let tag = match tag {
				ControlFlow::Break(value) => value,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			let entries = match entries {
				ControlFlow::Break(value) => value,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			let Some(tag) = tag else {
				continue;
			};
			let Some(account) = tag.account else {
				continue;
			};
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
		txn: &fdb::Transaction,
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
		txn: &fdb::Transaction,
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

		// Read the parents, the processes, and the tag storage permissions.
		let parents_future = async {
			match id {
				tg::Either::Left(object) => {
					let parents = crate::propagate!(
						crate::page::collect(|after| async move {
							Self::get_object_parents_page_with_transaction(
								txn,
								subspace,
								object,
								after.as_deref(),
								crate::page::PAGE_SIZE,
							)
							.await
						})
						.await
					);
					let parents = parents
						.into_iter()
						.map(tg::Either::Left)
						.collect::<Vec<_>>();
					Ok(ControlFlow::Break(parents))
				},
				tg::Either::Right(process) => {
					let parents = crate::propagate!(
						crate::page::collect(|after| async move {
							Self::get_process_parents_page_with_transaction(
								txn,
								subspace,
								process,
								after.as_deref(),
								crate::page::PAGE_SIZE,
							)
							.await
						})
						.await
					);
					let parents = parents
						.into_iter()
						.map(tg::Either::Right)
						.collect::<Vec<_>>();
					Ok(ControlFlow::Break(parents))
				},
			}
		};
		let processes_future = async {
			let tg::Either::Left(object) = id else {
				return Ok(ControlFlow::Break(Vec::new()));
			};
			crate::page::collect(|after| async move {
				Self::get_object_processes_page_with_transaction(
					txn,
					subspace,
					object,
					after.as_deref(),
					crate::page::PAGE_SIZE,
				)
				.await
			})
			.await
		};
		let (parents, processes, accounts) = futures::try_join!(
			parents_future,
			processes_future,
			Self::get_tag_storage_permissions_with_transaction(txn, subspace, &resource),
		)?;
		let parents = match parents {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let processes = match processes {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let accounts = match accounts {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};

		// Read the storage entries of the parents and the processes.
		let relatives = parents
			.into_iter()
			.map(|parent| (parent, None))
			.chain(
				processes
					.into_iter()
					.map(|(process, kind)| (tg::Either::Right(process), Some(kind))),
			)
			.collect::<Vec<_>>();
		let result = future::try_join_all(relatives.iter().map(|(relative, _)| {
			Self::get_account_storage_entry_with_transaction(txn, subspace, account, relative)
		}))
		.await;

		// Merge the inherited permissions.
		for ((_, kind), entry) in relatives.into_iter().zip(result?) {
			let entry = match entry {
				ControlFlow::Break(value) => value,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			let Some(entry) = entry else {
				continue;
			};
			let inherited = match kind {
				None => tangram_index::usage::storage::child_permissions(entry.permissions),
				Some(kind) => {
					tangram_index::usage::storage::object_permissions(entry.permissions, kind)
				},
			};
			if !inherited.is_empty() {
				permissions.insert(inherited);
				count += 1;
			}
		}
		if let Some(captured) = accounts.get(account) {
			permissions.insert(*captured);
			count += 1;
		}

		Ok(ControlFlow::Break((permissions, count)))
	}

	pub(crate) async fn get_tag_storage_resources_with_transaction(
		txn: &fdb::Transaction,
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
