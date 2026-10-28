mod key;

pub(super) use key::{Key, Kind, UsageKind};

use {
	super::{Db, Index, Kind as KeyKind, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
};

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) struct PermissionUpdate {
	#[tangram_serialize(id = 0)]
	pub source: Source,
	#[tangram_serialize(id = 1)]
	pub version: u64,
}

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) struct StorageAndMetadataUpdate {
	#[tangram_serialize(id = 0)]
	pub source: Source,
	#[tangram_serialize(id = 1)]
	pub version: u64,
}

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) struct UsageUpdate {
	#[tangram_serialize(id = 0)]
	pub version: u64,
}

#[derive(
	Clone, Copy, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) enum Source {
	#[tangram_serialize(id = 0)]
	Put,

	#[tangram_serialize(id = 1)]
	Propagate,
}

struct ProcessPermissionInputs<'a> {
	child_covers: &'a BTreeMap<tg::authorization::Permission, Option<i64>>,
	entries: &'a [crate::permission::PermissionEntry],
	object_covers: &'a BTreeMap<tg::authorization::Permission, Option<i64>>,
	resource: &'a tg::Id,
	subject: &'a tg::authorization::Subject,
	time_to_touch: std::time::Duration,
}

struct ProcessOutput {
	changed: bool,
	depth_exceeded: bool,
}

#[derive(Clone, Copy)]
struct PermissionCover {
	expires_at: Option<i64>,
}

#[derive(Clone, Copy)]
enum Relation<'a> {
	ObjectParents(&'a tg::object::Id),
	ObjectProcesses(&'a tg::object::Id),
	ProcessParents(&'a tg::process::Id),
}

impl PermissionUpdate {
	pub fn new(source: Source, version: u64) -> Self {
		Self { source, version }
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the permission update"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the permission update"))
	}
}

impl StorageAndMetadataUpdate {
	pub fn new(source: Source, version: u64) -> Self {
		Self { source, version }
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self).map_err(|error| {
			tg::error!(
				!error,
				"failed to serialize the storage and metadata update"
			)
		})
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes).map_err(|error| {
			tg::error!(
				!error,
				"failed to deserialize the storage and metadata update"
			)
		})
	}
}

impl UsageUpdate {
	pub fn new(version: u64) -> Self {
		Self { version }
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the usage update"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the usage update"))
	}
}

impl Index {
	pub async fn try_get_oldest_update_transaction_id(
		&self,
		kind: tangram_index::update::Kind,
	) -> tg::Result<Option<u64>> {
		let response = self
			.send_read_request(
				tangram_index::read::Request::TryGetOldestUpdateTransactionId { kind },
			)
			.await?;
		let tangram_index::read::Response::TryGetOldestUpdateTransactionId(output) = response
		else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn try_get_oldest_update_transaction_id_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		kind: tangram_index::update::Kind,
	) -> tg::Result<Option<u64>> {
		let prefix = &(update_version_key_kind(kind).to_i32().unwrap(),);
		let prefix = Self::pack(subspace, prefix);
		let entry = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get update version range"))?
			.next()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to read update version entry"))?;
		let Some((key, _)) = entry else {
			return Ok(None);
		};
		let key = Self::unpack(subspace, key)?;
		let crate::Key::Update(crate::update::Key::UpdateVersion { version, .. }) = key else {
			return Err(tg::error!("unexpected key type"));
		};

		Ok(Some(version))
	}

	pub async fn update_batch(
		&self,
		kind: tangram_index::update::Kind,
		batch_size: usize,
	) -> tg::Result<tangram_index::update::Output> {
		let request = Request::Update(crate::Update { batch_size, kind });
		let response = self.send_write_request(request).await?;
		let Response::UpdateOutput(output) = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(output)
	}

	#[allow(clippy::too_many_arguments)]
	pub(super) fn update_batch_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		batch_size: usize,
		kind: tangram_index::update::Kind,
		max_process_depth: Option<u64>,
		object_permission_time_to_touch: std::time::Duration,
		process_permission_time_to_touch: std::time::Duration,
		usage_partition_total: u64,
	) -> tg::Result<tangram_index::update::Output> {
		let prefix = &(update_version_key_kind(kind).to_i32().unwrap(),);
		let prefix = Self::pack(subspace, prefix);
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get update version range"))?
			.take(batch_size)
			.map(|entry| {
				let (key, _) = entry
					.map_err(|error| tg::error!(!error, "failed to read update version entry"))?;
				let key = Self::unpack(subspace, key)?;
				let crate::Key::Update(crate::update::Key::UpdateVersion { id, kind, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((id, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		let mut output = tangram_index::update::Output::default();
		for (id, kind) in entries {
			let key = crate::Key::Update(crate::update::Key::Update {
				id: id.clone(),
				kind: kind.clone(),
			});
			let key = Self::pack(subspace, &key);
			let value = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get update key"))?
				.ok_or_else(|| tg::error!("expected an update key for the update version key"))?;

			// A preceding item can lower the pending version after this batch selected its queue entry.
			let (source, version) = match &kind {
				Kind::Permission(_) | Kind::StorageAndMetadata => {
					let (source, version) = deserialize_source_update(&kind, value)?;
					(Some(source), version)
				},
				Kind::Usage(_) => {
					let update = UsageUpdate::deserialize(value)?;
					(None, update.version)
				},
			};

			let changed = match &kind {
				Kind::Permission(subject) => match &id {
					tg::Either::Left(id) => Self::update_object_permissions_for_subject(
						db,
						subspace,
						transaction,
						id,
						subject,
						object_permission_time_to_touch,
					)?,
					tg::Either::Right(id) => Self::update_process_permissions_for_subject(
						db,
						subspace,
						transaction,
						id,
						subject,
						process_permission_time_to_touch,
					)?,
				},
				Kind::StorageAndMetadata => match &id {
					tg::Either::Left(id) => Self::update_object(db, subspace, transaction, id)?,
					tg::Either::Right(id) => {
						let process_output =
							Self::update_process(db, subspace, transaction, id, max_process_depth)?;
						if process_output.depth_exceeded {
							output.processes_with_depth_exceeded.push(id.clone());
						}
						process_output.changed
					},
				},
				Kind::Usage(
					UsageKind::Clean(_) | UsageKind::CleanAll | UsageKind::Propagate { .. },
				) => return Err(tg::error!("unsupported LMDB usage update kind")),
				Kind::Usage(UsageKind::Put {
					account,
					permissions,
					touched_at,
				}) => match &id {
					tg::Either::Left(object) => {
						let entry = tangram_index::usage::storage::put::ObjectArg {
							account: account.clone(),
							object: object.clone(),
							touched_at: *touched_at,
						};
						Self::put_account_object(
							db,
							subspace,
							transaction,
							&entry,
							usage_partition_total,
							Some(*permissions),
							Some(version),
						)
					}?,
					tg::Either::Right(process) => {
						let entry = tangram_index::usage::storage::put::ProcessArg {
							account: account.clone(),
							process: process.clone(),
							touched_at: *touched_at,
						};
						Self::put_account_process(
							db,
							subspace,
							transaction,
							&entry,
							usage_partition_total,
							Some(*permissions),
							Some(version),
						)
					}?,
				},
			};

			if let Some(source) = source {
				let key = crate::Key::Update(Key::PropagatedVersion {
					id: id.clone(),
					kind: kind.clone(),
				});
				let key = Self::pack(subspace, &key);
				let propagated_version = db
					.get(transaction, &key)
					.map_err(|error| {
						tg::error!(!error, "failed to get the propagated update version")
					})?
					.map(|bytes| bytes.try_into().map(u64::from_be_bytes))
					.transpose()
					.map_err(|error| {
						tg::error!(
							!error,
							"failed to deserialize the propagated update version"
						)
					})?;
				// Propagate an older version even when the item's metadata or permissions are unchanged.
				if source == Source::Put
					|| changed || propagated_version
					.is_some_and(|propagated_version| version < propagated_version)
				{
					match &kind {
						Kind::StorageAndMetadata => {
							Self::enqueue_waiting_parents(db, subspace, transaction, &id, version)?;
						},
						Kind::Permission(_) | Kind::Usage(_) => {
							Self::enqueue_parents(db, subspace, transaction, &id, &kind, version)?;
						},
					}
					let item = match &id {
						tg::Either::Left(id) => {
							crate::Key::Object(crate::object::Key::Object(id.clone()))
						},
						tg::Either::Right(id) => {
							crate::Key::Process(crate::process::Key::Process(id.clone()))
						},
					};
					if db
						.get(transaction, &Self::pack(subspace, &item))
						.map_err(|error| tg::error!(!error, "failed to get the update item"))?
						.is_some()
					{
						// Keep one cleanup entry for the retained propagated version.
						if let Some(previous) =
							propagated_version.filter(|previous| *previous != version)
						{
							let key = crate::Key::Update(Key::Clean {
								id: id.clone(),
								kind: kind.clone(),
								version: previous,
							});
							db.delete(transaction, &Self::pack(subspace, &key))
								.map_err(|error| {
									tg::error!(!error, "failed to delete the update clean key")
								})?;
						}
						db.put(transaction, &key, &version.to_be_bytes())
							.map_err(|error| {
								tg::error!(!error, "failed to put the propagated update version")
							})?;
						let key = crate::Key::Update(Key::Clean {
							id: id.clone(),
							kind: kind.clone(),
							version,
						});
						db.put(transaction, &Self::pack(subspace, &key), &[])
							.map_err(|error| {
								tg::error!(!error, "failed to put the update clean key")
							})?;
					}
				}
			}

			let key = crate::Key::Update(crate::update::Key::Update {
				id: id.clone(),
				kind: kind.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete update key"))?;
			let key = crate::Key::Update(crate::update::Key::UpdateVersion {
				id: id.clone(),
				kind,
				version,
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete update version key"))?;

			output.count += 1;
		}

		Ok(output)
	}

	fn update_object(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<bool> {
		let key = crate::Key::Object(crate::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?;
		let Some(bytes) = bytes else {
			return Ok(false);
		};
		let mut object = tangram_index::object::Object::deserialize(bytes)?;

		// Skip reading the children of an object that an update can no longer change.
		if object.complete() {
			return Ok(false);
		}

		let children = crate::page::collect(|after| {
			Self::get_object_children_page_with_transaction(
				db,
				subspace,
				transaction,
				id,
				after,
				crate::page::PAGE_SIZE,
			)
		})?;

		let child_objects: Vec<Option<tangram_index::object::Object>> = children
			.iter()
			.map(|child| Self::try_get_object_with_transaction(db, subspace, transaction, child))
			.collect::<tg::Result<_>>()?;

		let mut changed = false;

		if object.storage.contains(tg::object::storage::Set::NODE)
			&& !object.storage.contains(tg::object::storage::Set::SUBTREE)
		{
			let value = child_objects.iter().all(|child| {
				child.as_ref().is_some_and(|object| {
					object.storage.contains(tg::object::storage::Set::SUBTREE)
				})
			});
			if value {
				object.storage.insert(tg::object::storage::Set::SUBTREE);
				changed = true;
			}
		}

		if object.metadata.subtree.count.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.count)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				let value = 1 + value;
				object.metadata.subtree.count = Some(value);
				changed = true;
			}
		}

		if object.metadata.subtree.depth.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.depth)
				})
				.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
			if let Some(value) = value {
				let value = 1 + value;
				object.metadata.subtree.depth = Some(value);
				changed = true;
			}
		}

		if object.metadata.subtree.size.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.size)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				let value = object.metadata.node.size + value;
				object.metadata.subtree.size = Some(value);
				changed = true;
			}
		}

		if object.metadata.subtree.solvable.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.solvable)
				})
				.try_fold(object.metadata.node.solvable, |output, value| {
					value.map(|value| output || value)
				});
			if let Some(value) = value {
				object.metadata.subtree.solvable = Some(value);
				changed = true;
			}
		}

		if object.metadata.subtree.solved.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.solved)
				})
				.try_fold(object.metadata.node.solved, |output, value| {
					value.map(|value| output && value)
				});
			if let Some(value) = value {
				object.metadata.subtree.solved = Some(value);
				changed = true;
			}
		}

		if changed {
			let value = object.serialize()?;
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, %id, "failed to put the object"))?;
		}

		Ok(changed)
	}

	fn update_object_permissions_for_subject(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
		subject: &tg::authorization::Subject,
		time_to_touch: std::time::Duration,
	) -> tg::Result<bool> {
		let resource = tg::Id::from(id.clone());
		let indexed =
			Self::try_get_object_with_transaction(db, subspace, transaction, id)?.is_some();
		let entries = Self::get_resource_permission_entries_for_subject_with_transaction(
			db,
			subspace,
			transaction,
			&resource,
			subject,
		)?;
		let node = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let mut expected = BTreeSet::new();
		let prefix = Self::pack(
			subspace,
			&(
				crate::Kind::Delegation.to_i32().unwrap(),
				id.to_bytes().as_ref(),
				subject.to_string(),
			),
		);
		let delegations =
			Self::get_delegations_with_prefix(db, subspace, transaction, &prefix, usize::MAX)?;
		for delegation in delegations {
			let source_entries =
				Self::get_resource_permission_entries_for_subject_with_transaction(
					db,
					subspace,
					transaction,
					&resource,
					&delegation.source,
				)?;
			for permission in [node, subtree] {
				let Some(cover) = Self::permission_entries_cover_expires_at(
					&source_entries,
					&delegation.source,
					permission,
				) else {
					continue;
				};
				let expires_at =
					Self::min_expires_at(cover.expires_at, Some(delegation.expires_at));
				if !Self::has_non_materialized_cover(&entries, subject, permission, expires_at) {
					expected.insert((subject.clone(), permission, expires_at));
				}
			}
		}
		// Recompute node proofs so a removed delegation cannot sustain its own subtree permission.
		let mut nodes = entries
			.iter()
			.filter(|entry| entry.permission == node)
			.filter_map(|entry| {
				let mut entry = entry.clone();
				entry.materialized = None;
				entry
					.effective_expires_at()
					.map(|expires_at| (entry.subject, expires_at))
			})
			.collect::<Vec<_>>();
		nodes.extend(
			expected
				.iter()
				.filter(|(_, permission, _)| *permission == node)
				.map(|(subject, _, expires_at)| (subject.clone(), *expires_at)),
		);
		// Derive subtree permissions only after the object children are indexed, reading the children only when there is a node proof.
		let child_cover = if indexed && !nodes.is_empty() {
			Self::get_resources_covers_with_transaction(
				db,
				subspace,
				transaction,
				|after| {
					let (children, cursor) = Self::get_object_children_page_with_transaction(
						db,
						subspace,
						transaction,
						id,
						after,
						crate::page::PAGE_SIZE,
					)?;
					let children = children.into_iter().map(tg::Id::from).collect();
					Ok((children, cursor))
				},
				subject,
				[subtree],
			)?
			.0
			.remove(&subtree)
		} else {
			None
		};
		if let Some(child_cover) = child_cover {
			for (subject, entry_expires_at) in nodes {
				let expires_at = Self::min_expires_at(entry_expires_at, child_cover);
				if !Self::has_non_materialized_cover(&entries, &subject, subtree, expires_at) {
					expected.insert((subject, subtree, expires_at));
				}
			}
		}

		// Keep the longest proof when multiple sources establish the same permission.
		let mut covers = BTreeMap::new();
		for (subject, permission, expires_at) in expected {
			covers
				.entry((subject, permission))
				.and_modify(|current: &mut Option<i64>| {
					*current = match (*current, expires_at) {
						(Some(a), Some(b)) => Some(a.max(b)),
						_ => None,
					};
				})
				.or_insert(expires_at);
		}
		let expected = covers
			.into_iter()
			.map(|((subject, permission), expires_at)| (subject, permission, expires_at))
			.collect();
		let managed = BTreeSet::from([node, subtree]);

		// Keep the current materialized expirations that the expected expirations extend by less than the time to touch.
		let expected = Self::retain_materialized_expirations(&entries, expected, time_to_touch);

		let materialized_changed = Self::reconcile_materialized_permissions(
			db,
			subspace,
			transaction,
			&resource,
			&entries,
			&expected,
			&managed,
		)?;
		let entries = Self::get_resource_permission_entries_for_subject_with_transaction(
			db,
			subspace,
			transaction,
			&resource,
			subject,
		)?;
		let direct_changed = Self::promote_process_direct_permissions_for_subject(
			db,
			subspace,
			transaction,
			id,
			subject,
			&entries,
		)?;

		Ok(direct_changed || materialized_changed)
	}

	fn promote_process_direct_permissions_for_subject(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
		subject: &tg::authorization::Subject,
		entries: &[crate::permission::PermissionEntry],
	) -> tg::Result<bool> {
		let tg::authorization::Subject::Process(process) = subject else {
			return Ok(false);
		};
		let permissions = entries
			.iter()
			.filter(|entry| entry.grant || entry.direct.is_some() || entry.materialized.is_some())
			.map(|entry| entry.permission)
			.collect::<BTreeSet<_>>();
		if permissions.is_empty() {
			return Ok(false);
		}
		let direct =
			Self::has_object_process_with_transaction(db, subspace, transaction, id, process)?;
		let node = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let mut anchored = direct;
		if !anchored {
			let parents = crate::page::collect(|after| {
				Self::get_object_parents_page_with_transaction(
					db,
					subspace,
					transaction,
					id,
					after,
					crate::page::PAGE_SIZE,
				)
			})?;
			for parent in parents {
				let resource = tg::Id::from(parent);
				let entries = Self::get_resource_permission_entries_for_subject_with_transaction(
					db,
					subspace,
					transaction,
					&resource,
					subject,
				)?;
				if entries.iter().any(|entry| {
					entry.is_non_expiring_process_direct() && entry.permission.implies(node)
				}) {
					anchored = true;
					break;
				}
			}
		}
		if !anchored {
			return Ok(false);
		}

		let creator = tg::Principal::Process(process.clone());
		let resource = tg::Id::from(id.clone());
		let mut changed = false;
		for permission in permissions {
			let entry = crate::permission::PermissionIndexEntry {
				creator: Some(&creator),
				expires_at: None,
				permission,
				resource: &resource,
				subject,
			};
			if Self::put_permission_index_entry(
				db,
				subspace,
				transaction,
				&entry,
				crate::permission::PermissionSource::Direct,
				None,
			)? {
				changed = true;
			}
		}

		Ok(changed)
	}

	fn has_object_process_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		object: &tg::object::Id,
		process: &tg::process::Id,
	) -> tg::Result<bool> {
		for kind in [
			tangram_index::process::object::Kind::Command,
			tangram_index::process::object::Kind::Error,
			tangram_index::process::object::Kind::Log,
			tangram_index::process::object::Kind::Output,
		] {
			let key = crate::Key::Object(crate::object::Key::ObjectProcess {
				object: object.clone(),
				kind,
				process: process.clone(),
			});
			let key = Self::pack(subspace, &key);
			let value = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the object process"))?;
			if value.is_some() {
				return Ok(true);
			}
		}
		Ok(false)
	}

	fn reconcile_materialized_permissions(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		resource: &tg::Id,
		entries: &[crate::permission::PermissionEntry],
		expected: &BTreeSet<(
			tg::authorization::Subject,
			tg::authorization::Permission,
			Option<i64>,
		)>,
		managed: &BTreeSet<tg::authorization::Permission>,
	) -> tg::Result<bool> {
		let mut changed = false;
		let current = entries
			.iter()
			.filter(|entry| managed.contains(&entry.permission))
			.filter_map(|entry| {
				entry
					.materialized
					.map(|expires_at| (entry.subject.clone(), entry.permission, expires_at))
			})
			.collect::<BTreeSet<_>>();
		for (subject, permission, expires_at) in current.difference(expected) {
			let entry = crate::permission::PermissionIndexEntry {
				creator: None,
				expires_at: *expires_at,
				permission: *permission,
				subject,
				resource,
			};
			if Self::delete_permission_index_entry(
				db,
				subspace,
				transaction,
				&entry,
				crate::permission::PermissionSource::Materialized,
			)? {
				changed = true;
			}
		}
		for (subject, permission, expires_at) in expected.difference(&current) {
			let entry = crate::permission::PermissionIndexEntry {
				creator: None,
				expires_at: *expires_at,
				permission: *permission,
				subject,
				resource,
			};
			if Self::put_permission_index_entry(
				db,
				subspace,
				transaction,
				&entry,
				crate::permission::PermissionSource::Materialized,
				None,
			)? {
				changed = true;
			}
		}
		Ok(changed)
	}

	fn update_process_permissions(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		input: &ProcessPermissionInputs<'_>,
	) -> tg::Result<bool> {
		let mut expected = BTreeSet::new();

		// Derive the node object aspect permissions from the object covers, unless a non-materialized permission implies them.
		let mut aspects = BTreeMap::new();
		for (permission, expires_at) in input.object_covers {
			if !Self::has_implying_non_materialized_cover(
				input.entries,
				input.subject,
				*permission,
				*expires_at,
			) {
				expected.insert((input.subject.clone(), *permission, *expires_at));
				aspects.insert(*permission, *expires_at);
			}
		}

		// Derive the subtree permissions from the non-materialized node permissions, the node object aspect permissions derived above, and the child covers.
		for (source, target) in process_subtree_permissions() {
			let Some(child_cover) = input.child_covers.get(&target) else {
				continue;
			};
			let cover =
				Self::non_materialized_cover_expires_at(input.entries, input.subject, source)
					.map(|cover| cover.expires_at);
			let cover = match (cover, aspects.get(&source).copied()) {
				(Some(left), Some(right)) => Some(Self::max_expires_at(left, right)),
				(cover, None) | (None, cover) => cover,
			};
			let Some(cover) = cover else {
				continue;
			};
			let expires_at = Self::min_expires_at(cover, *child_cover);
			if !Self::has_non_materialized_cover(input.entries, input.subject, target, expires_at) {
				expected.insert((input.subject.clone(), target, expires_at));
			}
		}

		// Keep the current materialized expirations that the expected expirations extend by less than the time to touch.
		let expected =
			Self::retain_materialized_expirations(input.entries, expected, input.time_to_touch);

		// Reconcile the materialized permissions.
		let process = tg::authorization::Permission::Process;
		let managed = BTreeSet::from([
			process(tg::authorization::permission::process::Permission::NodeCommandObjects),
			process(tg::authorization::permission::process::Permission::NodeErrorObjects),
			process(tg::authorization::permission::process::Permission::NodeLogObjects),
			process(tg::authorization::permission::process::Permission::NodeOutputObjects),
			process(tg::authorization::permission::process::Permission::Subtree),
			process(tg::authorization::permission::process::Permission::SubtreeCommandObjects),
			process(tg::authorization::permission::process::Permission::SubtreeErrorObjects),
			process(tg::authorization::permission::process::Permission::SubtreeLogObjects),
			process(tg::authorization::permission::process::Permission::SubtreeOutputObjects),
		]);
		let changed = Self::reconcile_materialized_permissions(
			db,
			subspace,
			transaction,
			input.resource,
			input.entries,
			&expected,
			&managed,
		)?;

		Ok(changed)
	}

	fn retain_materialized_expirations(
		entries: &[crate::permission::PermissionEntry],
		expected: BTreeSet<(
			tg::authorization::Subject,
			tg::authorization::Permission,
			Option<i64>,
		)>,
		time_to_touch: std::time::Duration,
	) -> BTreeSet<(
		tg::authorization::Subject,
		tg::authorization::Permission,
		Option<i64>,
	)> {
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap_or(i64::MAX);
		expected
			.into_iter()
			.map(|(subject, permission, expires_at)| {
				let current = entries
					.iter()
					.filter(|entry| entry.subject == subject && entry.permission == permission)
					.find_map(|entry| entry.materialized);
				let expires_at = match (current, expires_at) {
					(Some(Some(current)), Some(expires_at))
						if expires_at > current && expires_at - current < time_to_touch =>
					{
						Some(current)
					},
					_ => expires_at,
				};
				(subject, permission, expires_at)
			})
			.collect()
	}

	fn has_non_materialized_cover(
		entries: &[crate::permission::PermissionEntry],
		subject: &tg::authorization::Subject,
		permission: tg::authorization::Permission,
		expires_at: Option<i64>,
	) -> bool {
		entries.iter().any(|entry| {
			entry.subject == *subject
				&& entry.permission == permission
				&& entry.has_non_materialized_cover(expires_at)
		})
	}

	fn has_implying_non_materialized_cover(
		entries: &[crate::permission::PermissionEntry],
		subject: &tg::authorization::Subject,
		permission: tg::authorization::Permission,
		expires_at: Option<i64>,
	) -> bool {
		entries.iter().any(|entry| {
			entry.subject == *subject
				&& entry.permission.implies(permission)
				&& entry.has_non_materialized_cover(expires_at)
		})
	}

	fn non_materialized_cover_expires_at(
		entries: &[crate::permission::PermissionEntry],
		subject: &tg::authorization::Subject,
		permission: tg::authorization::Permission,
	) -> Option<PermissionCover> {
		entries
			.iter()
			.filter(|entry| entry.subject == *subject && entry.permission == permission)
			.filter_map(|entry| {
				let mut entry = entry.clone();
				entry.materialized = None;
				entry
					.effective_expires_at()
					.map(|expires_at| PermissionCover { expires_at })
			})
			.reduce(|left, right| PermissionCover {
				expires_at: Self::max_expires_at(left.expires_at, right.expires_at),
			})
	}

	fn permission_entries_cover_expires_at(
		entries: &[crate::permission::PermissionEntry],
		subject: &tg::authorization::Subject,
		permission: tg::authorization::Permission,
	) -> Option<PermissionCover> {
		entries
			.iter()
			.filter(|entry| entry.subject == *subject && entry.permission == permission)
			.filter_map(|entry| {
				entry
					.effective_expires_at()
					.map(|expires_at| PermissionCover { expires_at })
			})
			.reduce(|left, right| PermissionCover {
				expires_at: Self::max_expires_at(left.expires_at, right.expires_at),
			})
	}

	fn max_expires_at(left: Option<i64>, right: Option<i64>) -> Option<i64> {
		match (left, right) {
			(None, _) | (_, None) => None,
			(Some(left), Some(right)) => Some(left.max(right)),
		}
	}

	fn min_expires_at(left: Option<i64>, right: Option<i64>) -> Option<i64> {
		match (left, right) {
			(None, expires_at) | (expires_at, None) => expires_at,
			(Some(left), Some(right)) => Some(left.min(right)),
		}
	}

	fn update_process_permissions_for_subject(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::process::Id,
		subject: &tg::authorization::Subject,
		time_to_touch: std::time::Duration,
	) -> tg::Result<bool> {
		// Get the process.
		let key = crate::Key::Process(crate::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
		let Some(bytes) = bytes else {
			return Ok(false);
		};
		let process = tangram_index::process::Process::deserialize(bytes)?;

		// Get the subject's permission entries, which every materialized permission requires.
		let resource = tg::Id::from(id.clone());
		let entries = Self::get_resource_permission_entries_for_subject_with_transaction(
			db,
			subspace,
			transaction,
			&resource,
			subject,
		)?;
		if entries.is_empty() {
			return Ok(false);
		}

		// Get the object covers of the object aspects, only for a subject with a direct or grant permission on the process.
		let object_subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let anchored = entries
			.iter()
			.any(|entry| entry.grant || entry.direct.is_some());
		let mut aspects = if anchored {
			process_object_aspect_permissions(&process.set)
		} else {
			Vec::new()
		};

		// Skip the object aspects for a subject that has them inherently, as the process itself or one of its parents.
		if !aspects.is_empty()
			&& let tg::authorization::Subject::Process(subject_process) = subject
			&& (subject_process == id
				|| Self::is_related_with_transaction(
					db,
					subspace,
					transaction,
					Relation::ProcessParents(id),
					&subject_process.clone().into(),
				)?) {
			aspects.clear();
		}

		// Get the object covers of the object aspects, reading the objects of each kind only while every object read so far is covered.
		let mut object_covers = BTreeMap::new();
		for (kind, permission) in aspects {
			let (mut covers, any) = Self::get_resources_covers_with_transaction(
				db,
				subspace,
				transaction,
				|after| {
					let (objects, cursor) = Self::get_process_objects_page_with_transaction(
						db,
						subspace,
						transaction,
						id,
						Some(kind),
						after,
						crate::page::PAGE_SIZE,
					)?;
					let objects = objects
						.into_iter()
						.map(|(object, _)| tg::Id::from(object))
						.collect();
					Ok((objects, cursor))
				},
				subject,
				[object_subtree],
			)?;
			if any && let Some(cover) = covers.remove(&object_subtree) {
				object_covers.insert(permission, cover);
			}
		}

		// Get the child covers of the subtree permissions whose node permissions the subject has, stopping once no child cover remains.
		let targets = process_subtree_permissions()
			.into_iter()
			.filter(|(source, _)| {
				Self::non_materialized_cover_expires_at(&entries, subject, *source).is_some()
					|| object_covers.contains_key(source)
			})
			.map(|(_, target)| target)
			.collect::<Vec<_>>();
		let child_covers = if targets.is_empty() {
			BTreeMap::new()
		} else {
			Self::get_resources_covers_with_transaction(
				db,
				subspace,
				transaction,
				|after| {
					let (children, cursor) = Self::get_process_children_page_with_transaction(
						db,
						subspace,
						transaction,
						id,
						after,
						crate::page::PAGE_SIZE,
					)?;
					let children = children.into_iter().map(tg::Id::from).collect();
					Ok((children, cursor))
				},
				subject,
				targets,
			)?
			.0
		};

		// Update the materialized permissions.
		let entry = ProcessPermissionInputs {
			child_covers: &child_covers,
			entries: &entries,
			object_covers: &object_covers,
			resource: &resource,
			subject,
			time_to_touch,
		};
		let changed = Self::update_process_permissions(db, subspace, transaction, &entry)?;

		Ok(changed)
	}

	fn get_resources_covers_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		mut get_page: impl FnMut(Option<&[u8]>) -> tg::Result<crate::page::Page<tg::Id>>,
		subject: &tg::authorization::Subject,
		permissions: impl IntoIterator<Item = tg::authorization::Permission>,
	) -> tg::Result<(BTreeMap<tg::authorization::Permission, Option<i64>>, bool)> {
		let mut covers = permissions
			.into_iter()
			.map(|permission| (permission, None))
			.collect::<BTreeMap<_, _>>();
		let mut any = false;
		let mut after = None;
		while !covers.is_empty() {
			let (resources, cursor) = get_page(after.as_deref())?;
			for resource in resources {
				if covers.is_empty() {
					break;
				}
				any = true;
				let entries = Self::get_resource_permission_entries_for_subject_with_transaction(
					db,
					subspace,
					transaction,
					&resource,
					subject,
				)?;
				Self::retain_permission_entries_covers(&mut covers, &entries, subject);
			}
			let Some(cursor) = cursor else {
				break;
			};
			after = Some(cursor);
		}
		Ok((covers, any))
	}

	fn retain_permission_entries_covers(
		covers: &mut BTreeMap<tg::authorization::Permission, Option<i64>>,
		entries: &[crate::permission::PermissionEntry],
		subject: &tg::authorization::Subject,
	) {
		covers.retain(|permission, cover| {
			let Some(entry_cover) =
				Self::permission_entries_cover_expires_at(entries, subject, *permission)
			else {
				return false;
			};
			*cover = Self::min_expires_at(*cover, entry_cover.expires_at);
			true
		});
	}

	fn update_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::process::Id,
		max_process_depth: Option<u64>,
	) -> tg::Result<ProcessOutput> {
		let process_key = crate::Key::Process(crate::process::Key::Process(id.clone()));
		let process_key = Self::pack(subspace, &process_key);
		let bytes = db
			.get(transaction, &process_key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
		let Some(bytes) = bytes else {
			let output = ProcessOutput {
				changed: false,
				depth_exceeded: false,
			};
			return Ok(output);
		};
		let mut process = tangram_index::process::Process::deserialize(bytes)?;

		let children = crate::page::collect(|after| {
			Self::get_process_children_page_with_transaction(
				db,
				subspace,
				transaction,
				id,
				after,
				crate::page::PAGE_SIZE,
			)
		})?;
		let children = children
			.iter()
			.map(|child| Self::try_get_process_with_transaction(db, subspace, transaction, child))
			.collect::<tg::Result<Vec<_>>>()?;

		let objects = crate::page::collect(|after| {
			Self::get_process_objects_page_with_transaction(
				db,
				subspace,
				transaction,
				id,
				None,
				after,
				crate::page::PAGE_SIZE,
			)
		})?;
		let mut command_objects: Vec<Option<tangram_index::object::Object>> = Vec::new();
		let mut error_objects: Vec<Option<tangram_index::object::Object>> = Vec::new();
		let mut log_objects: Vec<Option<tangram_index::object::Object>> = Vec::new();
		let mut output_objects: Vec<Option<tangram_index::object::Object>> = Vec::new();
		for (id, kind) in &objects {
			let object = Self::try_get_object_with_transaction(db, subspace, transaction, id)?;
			match kind {
				tangram_index::process::object::Kind::Command => {
					command_objects.push(object);
				},
				tangram_index::process::object::Kind::Error => {
					error_objects.push(object);
				},
				tangram_index::process::object::Kind::Log => {
					log_objects.push(object);
				},
				tangram_index::process::object::Kind::Output => {
					output_objects.push(object);
				},
			}
		}

		let mut changed = false;

		let depth = children
			.iter()
			.map(|option| {
				option
					.as_ref()
					.and_then(|child| child.metadata.subtree.depth)
			})
			.try_fold(0u64, |output, value| value.map(|value| output.max(value)))
			.map(|depth| depth + 1);
		if let Some(depth) = depth
			&& process
				.metadata
				.subtree
				.depth
				.is_none_or(|current| depth > current)
		{
			process.metadata.subtree.depth = Some(depth);
			changed = true;
		}

		let depth_exceeded = max_process_depth.is_some_and(|max_depth| {
			process
				.metadata
				.subtree
				.depth
				.is_some_and(|depth| depth > max_depth)
				&& process.status.is_some_and(|status| !status.is_finished())
		});

		if process.set.command_objects {
			if process.metadata.node.command_objects.count.is_none() {
				let value = command_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.count)
					})
					.sum::<Option<u64>>();
				if let Some(value) = value {
					process.metadata.node.command_objects.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.command_objects.depth.is_none() {
				let value = command_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.depth)
					})
					.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
				if let Some(value) = value {
					process.metadata.node.command_objects.depth = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.command_objects.size.is_none() {
				let value = command_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.size)
					})
					.sum::<Option<u64>>();
				if let Some(value) = value {
					process.metadata.node.command_objects.size = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.command_objects.solvable.is_none() {
				let value = command_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.solvable)
					})
					.try_fold(false, |output, value| value.map(|value| output || value));
				if let Some(value) = value {
					process.metadata.node.command_objects.solvable = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.command_objects.solved.is_none() {
				let value = command_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.solved)
					})
					.try_fold(true, |output, value| value.map(|value| output && value));
				if let Some(value) = value {
					process.metadata.node.command_objects.solved = Some(value);
					changed = true;
				}
			}
		}

		if process.set.error_objects {
			if process.metadata.node.error_objects.count.is_none() {
				let value = error_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.count)
					})
					.sum::<Option<u64>>();
				if let Some(value) = value {
					process.metadata.node.error_objects.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.error_objects.depth.is_none() {
				let value = error_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.depth)
					})
					.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
				if let Some(value) = value {
					process.metadata.node.error_objects.depth = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.error_objects.size.is_none() {
				let value = error_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.size)
					})
					.sum::<Option<u64>>();
				if let Some(value) = value {
					process.metadata.node.error_objects.size = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.error_objects.solvable.is_none() {
				let value = error_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.solvable)
					})
					.try_fold(false, |output, value| value.map(|value| output || value));
				if let Some(value) = value {
					process.metadata.node.error_objects.solvable = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.error_objects.solved.is_none() {
				let value = error_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.solved)
					})
					.try_fold(true, |output, value| value.map(|value| output && value));
				if let Some(value) = value {
					process.metadata.node.error_objects.solved = Some(value);
					changed = true;
				}
			}
		}

		if process.set.log_objects {
			if process.metadata.node.log_objects.count.is_none() {
				let value = log_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.count)
					})
					.sum::<Option<u64>>();
				if let Some(value) = value {
					process.metadata.node.log_objects.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.log_objects.depth.is_none() {
				let value = log_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.depth)
					})
					.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
				if let Some(value) = value {
					process.metadata.node.log_objects.depth = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.log_objects.size.is_none() {
				let value = log_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.size)
					})
					.sum::<Option<u64>>();
				if let Some(value) = value {
					process.metadata.node.log_objects.size = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.log_objects.solvable.is_none() {
				let value = log_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.solvable)
					})
					.try_fold(false, |output, value| value.map(|value| output || value));
				if let Some(value) = value {
					process.metadata.node.log_objects.solvable = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.log_objects.solved.is_none() {
				let value = log_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.solved)
					})
					.try_fold(true, |output, value| value.map(|value| output && value));
				if let Some(value) = value {
					process.metadata.node.log_objects.solved = Some(value);
					changed = true;
				}
			}
		}

		if process.set.output_objects {
			if process.metadata.node.output_objects.count.is_none() {
				let value = output_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.count)
					})
					.sum::<Option<u64>>();
				if let Some(value) = value {
					process.metadata.node.output_objects.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.output_objects.depth.is_none() {
				let value = output_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.depth)
					})
					.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
				if let Some(value) = value {
					process.metadata.node.output_objects.depth = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.output_objects.size.is_none() {
				let value = output_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.size)
					})
					.sum::<Option<u64>>();
				if let Some(value) = value {
					process.metadata.node.output_objects.size = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.output_objects.solvable.is_none() {
				let value = output_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.solvable)
					})
					.try_fold(false, |output, value| value.map(|value| output || value));
				if let Some(value) = value {
					process.metadata.node.output_objects.solvable = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.output_objects.solved.is_none() {
				let value = output_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.solved)
					})
					.try_fold(true, |output, value| value.map(|value| output && value));
				if let Some(value) = value {
					process.metadata.node.output_objects.solved = Some(value);
					changed = true;
				}
			}
		}

		if process.set.children {
			if process.metadata.subtree.count.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.count)
					})
					.sum::<Option<u64>>();
				if let Some(value) = value {
					let value = 1 + value;
					process.metadata.subtree.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.command_objects.count.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.command_objects.count)
					})
					.fold(
						process.metadata.node.command_objects.count,
						|output, value| output.and_then(|output| value.map(|value| output + value)),
					);
				if let Some(value) = value {
					process.metadata.subtree.command_objects.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.command_objects.depth.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.command_objects.depth)
					})
					.fold(
						process.metadata.node.command_objects.depth,
						|output, value| {
							output.and_then(|output| value.map(|value| output.max(value)))
						},
					);
				if let Some(value) = value {
					process.metadata.subtree.command_objects.depth = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.command_objects.size.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.command_objects.size)
					})
					.fold(
						process.metadata.node.command_objects.size,
						|output, value| output.and_then(|output| value.map(|value| output + value)),
					);
				if let Some(value) = value {
					process.metadata.subtree.command_objects.size = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.command_objects.solvable.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.command_objects.solvable)
					})
					.fold(
						process.metadata.node.command_objects.solvable,
						|output, value| {
							output.and_then(|output| value.map(|value| output || value))
						},
					);
				if let Some(value) = value {
					process.metadata.subtree.command_objects.solvable = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.command_objects.solved.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.command_objects.solved)
					})
					.fold(
						process.metadata.node.command_objects.solved,
						|output, value| {
							output.and_then(|output| value.map(|value| output && value))
						},
					);
				if let Some(value) = value {
					process.metadata.subtree.command_objects.solved = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.error_objects.count.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.error_objects.count)
					})
					.fold(
						process.metadata.node.error_objects.count,
						|output, value| output.and_then(|output| value.map(|value| output + value)),
					);
				if let Some(value) = value {
					process.metadata.subtree.error_objects.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.error_objects.depth.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.error_objects.depth)
					})
					.fold(
						process.metadata.node.error_objects.depth,
						|output, value| {
							output.and_then(|output| value.map(|value| output.max(value)))
						},
					);
				if let Some(value) = value {
					process.metadata.subtree.error_objects.depth = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.error_objects.size.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.error_objects.size)
					})
					.fold(process.metadata.node.error_objects.size, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					process.metadata.subtree.error_objects.size = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.error_objects.solvable.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.error_objects.solvable)
					})
					.fold(
						process.metadata.node.error_objects.solvable,
						|output, value| {
							output.and_then(|output| value.map(|value| output || value))
						},
					);
				if let Some(value) = value {
					process.metadata.subtree.error_objects.solvable = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.error_objects.solved.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.error_objects.solved)
					})
					.fold(
						process.metadata.node.error_objects.solved,
						|output, value| {
							output.and_then(|output| value.map(|value| output && value))
						},
					);
				if let Some(value) = value {
					process.metadata.subtree.error_objects.solved = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.log_objects.count.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.log_objects.count)
					})
					.fold(process.metadata.node.log_objects.count, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					process.metadata.subtree.log_objects.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.log_objects.depth.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.log_objects.depth)
					})
					.fold(process.metadata.node.log_objects.depth, |output, value| {
						output.and_then(|output| value.map(|value| output.max(value)))
					});
				if let Some(value) = value {
					process.metadata.subtree.log_objects.depth = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.log_objects.size.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.log_objects.size)
					})
					.fold(process.metadata.node.log_objects.size, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					process.metadata.subtree.log_objects.size = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.log_objects.solvable.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.log_objects.solvable)
					})
					.fold(
						process.metadata.node.log_objects.solvable,
						|output, value| {
							output.and_then(|output| value.map(|value| output || value))
						},
					);
				if let Some(value) = value {
					process.metadata.subtree.log_objects.solvable = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.log_objects.solved.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.log_objects.solved)
					})
					.fold(process.metadata.node.log_objects.solved, |output, value| {
						output.and_then(|output| value.map(|value| output && value))
					});
				if let Some(value) = value {
					process.metadata.subtree.log_objects.solved = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.output_objects.count.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.output_objects.count)
					})
					.fold(
						process.metadata.node.output_objects.count,
						|output, value| output.and_then(|output| value.map(|value| output + value)),
					);
				if let Some(value) = value {
					process.metadata.subtree.output_objects.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.output_objects.depth.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.output_objects.depth)
					})
					.fold(
						process.metadata.node.output_objects.depth,
						|output, value| {
							output.and_then(|output| value.map(|value| output.max(value)))
						},
					);
				if let Some(value) = value {
					process.metadata.subtree.output_objects.depth = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.output_objects.size.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.output_objects.size)
					})
					.fold(
						process.metadata.node.output_objects.size,
						|output, value| output.and_then(|output| value.map(|value| output + value)),
					);
				if let Some(value) = value {
					process.metadata.subtree.output_objects.size = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.output_objects.solvable.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.output_objects.solvable)
					})
					.fold(
						process.metadata.node.output_objects.solvable,
						|output, value| {
							output.and_then(|output| value.map(|value| output || value))
						},
					);
				if let Some(value) = value {
					process.metadata.subtree.output_objects.solvable = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.output_objects.solved.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.output_objects.solved)
					})
					.fold(
						process.metadata.node.output_objects.solved,
						|output, value| {
							output.and_then(|output| value.map(|value| output && value))
						},
					);
				if let Some(value) = value {
					process.metadata.subtree.output_objects.solved = Some(value);
					changed = true;
				}
			}
		}

		if process.set.command_objects
			&& !process
				.storage
				.contains(tg::process::storage::Set::NODE_COMMAND_OBJECTS)
		{
			let value = command_objects.iter().all(|option| {
				option.as_ref().is_some_and(|object| {
					object.storage.contains(tg::object::storage::Set::SUBTREE)
				})
			});
			if value {
				process
					.storage
					.insert(tg::process::storage::Set::NODE_COMMAND_OBJECTS);
				changed = true;
			}
		}

		if process.set.error_objects
			&& !process
				.storage
				.contains(tg::process::storage::Set::NODE_ERROR_OBJECTS)
		{
			let value = error_objects.iter().all(|option| {
				option.as_ref().is_some_and(|object| {
					object.storage.contains(tg::object::storage::Set::SUBTREE)
				})
			});
			if value {
				process
					.storage
					.insert(tg::process::storage::Set::NODE_ERROR_OBJECTS);
				changed = true;
			}
		}

		if process.set.log_objects
			&& !process
				.storage
				.contains(tg::process::storage::Set::NODE_LOG_OBJECTS)
		{
			let value = log_objects.iter().all(|option| {
				option.as_ref().is_some_and(|object| {
					object.storage.contains(tg::object::storage::Set::SUBTREE)
				})
			});
			if value {
				process
					.storage
					.insert(tg::process::storage::Set::NODE_LOG_OBJECTS);
				changed = true;
			}
		}

		if process.set.output_objects
			&& !process
				.storage
				.contains(tg::process::storage::Set::NODE_OUTPUT_OBJECTS)
		{
			let value = output_objects.iter().all(|option| {
				option.as_ref().is_some_and(|object| {
					object.storage.contains(tg::object::storage::Set::SUBTREE)
				})
			});
			if value {
				process
					.storage
					.insert(tg::process::storage::Set::NODE_OUTPUT_OBJECTS);
				changed = true;
			}
		}

		if process.set.children
			&& process.storage.contains(tg::process::storage::Set::NODE)
			&& !process.storage.contains(tg::process::storage::Set::SUBTREE)
		{
			let value = children.iter().all(|child| {
				child
					.as_ref()
					.is_some_and(|child| child.storage.contains(tg::process::storage::Set::SUBTREE))
			});
			if value {
				process.storage.insert(tg::process::storage::Set::SUBTREE);
				changed = true;
			}
		}

		if process.set.children {
			if !process
				.storage
				.contains(tg::process::storage::Set::SUBTREE_COMMAND_OBJECTS)
				&& process
					.storage
					.contains(tg::process::storage::Set::NODE_COMMAND_OBJECTS)
			{
				let value = children.iter().all(|child| {
					child.as_ref().is_some_and(|child| {
						child
							.storage
							.contains(tg::process::storage::Set::SUBTREE_COMMAND_OBJECTS)
					})
				});
				if value {
					process
						.storage
						.insert(tg::process::storage::Set::SUBTREE_COMMAND_OBJECTS);
					changed = true;
				}
			}

			if !process
				.storage
				.contains(tg::process::storage::Set::SUBTREE_ERROR_OBJECTS)
				&& process
					.storage
					.contains(tg::process::storage::Set::NODE_ERROR_OBJECTS)
			{
				let value = children.iter().all(|child| {
					child.as_ref().is_some_and(|child| {
						child
							.storage
							.contains(tg::process::storage::Set::SUBTREE_ERROR_OBJECTS)
					})
				});
				if value {
					process
						.storage
						.insert(tg::process::storage::Set::SUBTREE_ERROR_OBJECTS);
					changed = true;
				}
			}

			if !process
				.storage
				.contains(tg::process::storage::Set::SUBTREE_LOG_OBJECTS)
				&& process
					.storage
					.contains(tg::process::storage::Set::NODE_LOG_OBJECTS)
			{
				let value = children.iter().all(|child| {
					child.as_ref().is_some_and(|child| {
						child
							.storage
							.contains(tg::process::storage::Set::SUBTREE_LOG_OBJECTS)
					})
				});
				if value {
					process
						.storage
						.insert(tg::process::storage::Set::SUBTREE_LOG_OBJECTS);
					changed = true;
				}
			}

			if !process
				.storage
				.contains(tg::process::storage::Set::SUBTREE_OUTPUT_OBJECTS)
				&& process
					.storage
					.contains(tg::process::storage::Set::NODE_OUTPUT_OBJECTS)
			{
				let value = children.iter().all(|child| {
					child.as_ref().is_some_and(|child| {
						child
							.storage
							.contains(tg::process::storage::Set::SUBTREE_OUTPUT_OBJECTS)
					})
				});
				if value {
					process
						.storage
						.insert(tg::process::storage::Set::SUBTREE_OUTPUT_OBJECTS);
					changed = true;
				}
			}
		}

		if changed {
			let value = process.serialize()?;
			db.put(transaction, &process_key, &value)
				.map_err(|error| tg::error!(!error, %id, "failed to put the process"))?;
		}

		let output = ProcessOutput {
			changed,
			depth_exceeded,
		};

		Ok(output)
	}

	fn enqueue_parents(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		kind: &Kind,
		version: u64,
	) -> tg::Result<()> {
		match id {
			tg::Either::Left(id) => {
				if let Kind::Permission(source) = kind {
					let prefix = Self::pack(
						subspace,
						&(
							crate::Kind::DelegationSource.to_i32().unwrap(),
							id.to_bytes().as_ref(),
							source.to_string(),
						),
					);
					let delegations = Self::get_delegations_with_prefix(
						db,
						subspace,
						transaction,
						&prefix,
						usize::MAX,
					)?;
					for delegation in delegations {
						Self::enqueue_update_with_kind(
							db,
							subspace,
							transaction,
							tg::Either::Left(id.clone()),
							Kind::Permission(delegation.subject),
							Source::Propagate,
							Some(version),
						)?;
					}
				}

				let parents = match kind {
					Kind::Permission(subject) => {
						Self::get_related_resources_for_subject_with_transaction(
							db,
							subspace,
							transaction,
							Relation::ObjectParents(id),
							subject,
						)?
						.into_iter()
						.map(tg::object::Id::try_from)
						.collect::<tg::Result<Vec<_>>>()?
					},
					Kind::StorageAndMetadata | Kind::Usage(_) => crate::page::collect(|after| {
						Self::get_object_parents_page_with_transaction(
							db,
							subspace,
							transaction,
							id,
							after,
							crate::page::PAGE_SIZE,
						)
					})?,
				};
				for parent in parents {
					Self::enqueue_update_with_kind(
						db,
						subspace,
						transaction,
						tg::Either::Left(parent),
						kind.clone(),
						Source::Propagate,
						Some(version),
					)?;
				}
				match kind {
					Kind::Permission(subject) => {
						let processes = Self::get_related_resources_for_subject_with_transaction(
							db,
							subspace,
							transaction,
							Relation::ObjectProcesses(id),
							subject,
						)?
						.into_iter()
						.map(tg::process::Id::try_from)
						.collect::<tg::Result<Vec<_>>>()?;
						for process in processes {
							Self::enqueue_update_with_kind(
								db,
								subspace,
								transaction,
								tg::Either::Right(process),
								kind.clone(),
								Source::Propagate,
								Some(version),
							)?;
						}
					},
					Kind::StorageAndMetadata | Kind::Usage(_) => {
						let mut after = None;
						loop {
							let (processes, cursor) =
								Self::get_object_processes_page_with_transaction(
									db,
									subspace,
									transaction,
									id,
									after.as_deref(),
									crate::page::PAGE_SIZE,
								)?;
							for (process, _) in processes {
								Self::enqueue_update_with_kind(
									db,
									subspace,
									transaction,
									tg::Either::Right(process),
									kind.clone(),
									Source::Propagate,
									Some(version),
								)?;
							}
							let Some(cursor) = cursor else {
								break;
							};
							after = Some(cursor);
						}
					},
				}
			},
			tg::Either::Right(id) => {
				let parents = match kind {
					Kind::Permission(subject) => {
						Self::get_related_resources_for_subject_with_transaction(
							db,
							subspace,
							transaction,
							Relation::ProcessParents(id),
							subject,
						)?
						.into_iter()
						.map(tg::process::Id::try_from)
						.collect::<tg::Result<Vec<_>>>()?
					},
					Kind::StorageAndMetadata | Kind::Usage(_) => crate::page::collect(|after| {
						Self::get_process_parents_page_with_transaction(
							db,
							subspace,
							transaction,
							id,
							after,
							crate::page::PAGE_SIZE,
						)
					})?,
				};
				for parent in parents {
					Self::enqueue_update_with_kind(
						db,
						subspace,
						transaction,
						tg::Either::Right(parent),
						kind.clone(),
						Source::Propagate,
						Some(version),
					)?;
				}
			},
		}
		Ok(())
	}

	fn enqueue_waiting_parents(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		version: u64,
	) -> tg::Result<()> {
		// Read the propagations from the item to the parents that are waiting on it.
		let id_bytes = match id {
			tg::Either::Left(id) => id.to_bytes(),
			tg::Either::Right(id) => id.to_bytes(),
		};
		let prefix = Self::pack(
			subspace,
			&(KeyKind::Propagation.to_i32().unwrap(), id_bytes.as_ref()),
		);
		let parents = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the propagations"))?
			.map(|entry| {
				let (key, _) =
					entry.map_err(|error| tg::error!(!error, "failed to read a propagation"))?;
				let crate::Key::Update(Key::Propagation { parent, .. }) =
					Self::unpack(subspace, key)?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(parent)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		// Enqueue an update on each waiting parent. A child that completes before a parent writes its propagation is seen by the parent's own update, which the parent's put enqueues.
		for parent in parents {
			Self::enqueue_update(
				db,
				subspace,
				transaction,
				parent,
				Source::Propagate,
				Some(version),
			)?;
		}

		Ok(())
	}

	fn get_related_resources_for_subject_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		relation: Relation<'_>,
		subject: &tg::authorization::Subject,
	) -> tg::Result<Vec<tg::Id>> {
		// Read the related resources and the resources on which the subject has a permission in turns, so that the reads are bounded by the shorter of the two.
		let prefix = match relation {
			Relation::ObjectParents(id) => Self::pack(
				subspace,
				&(
					crate::Kind::ChildObject.to_i32().unwrap(),
					id.to_bytes().as_ref(),
				),
			),
			Relation::ObjectProcesses(id) => Self::pack(
				subspace,
				&(
					crate::Kind::ObjectProcess.to_i32().unwrap(),
					id.to_bytes().as_ref(),
				),
			),
			Relation::ProcessParents(id) => Self::pack(
				subspace,
				&(
					crate::Kind::ChildProcess.to_i32().unwrap(),
					id.to_bytes().as_ref(),
				),
			),
		};
		let mut related_iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the related resources"))?;
		let prefix = Self::pack(
			subspace,
			&(
				crate::Kind::SubjectPermission.to_i32().unwrap(),
				subject.to_string(),
			),
		);
		let mut subject_iter = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to get the subject permissions"))?;
		let mut related = Vec::new();
		let mut resources = BTreeSet::new();
		let related_exhausted = loop {
			let Some(entry) = related_iter.next() else {
				break true;
			};
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to read the related resource"))?;
			let resource = match (relation, Self::unpack(subspace, key)?) {
				(
					Relation::ObjectParents(_),
					crate::Key::Object(crate::object::Key::ChildObject { object, .. }),
				) => tg::Id::from(object),
				(
					Relation::ObjectProcesses(_),
					crate::Key::Object(crate::object::Key::ObjectProcess { process, .. }),
				)
				| (
					Relation::ProcessParents(_),
					crate::Key::Process(crate::process::Key::ChildProcess {
						parent: process, ..
					}),
				) => tg::Id::from(process),
				_ => return Err(tg::error!("unexpected key type")),
			};
			related.push(resource);
			let Some(entry) = subject_iter.next() else {
				break false;
			};
			let (key, _) = entry
				.map_err(|error| tg::error!(!error, "failed to read the subject permission"))?;
			let crate::Key::Permission(crate::permission::Key::SubjectPermission {
				resource, ..
			}) = Self::unpack(subspace, key)?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			resources.insert(resource);
		};
		drop(related_iter);
		drop(subject_iter);

		// Keep the related resources on which the subject has a permission.
		let mut output = Vec::new();
		if related_exhausted {
			for resource in related {
				if Self::has_permissions_for_subject(db, subspace, transaction, &resource, subject)?
				{
					output.push(resource);
				}
			}
		} else {
			for resource in resources {
				if Self::is_related_with_transaction(
					db,
					subspace,
					transaction,
					relation,
					&resource,
				)? {
					output.push(resource);
				}
			}
		}

		Ok(output)
	}

	fn is_related_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		relation: Relation<'_>,
		resource: &tg::Id,
	) -> tg::Result<bool> {
		let key = match relation {
			Relation::ObjectParents(id) => {
				let Ok(object) = tg::object::Id::try_from(resource.clone()) else {
					return Ok(false);
				};
				crate::Key::Object(crate::object::Key::ChildObject {
					child: id.clone(),
					object,
				})
			},
			Relation::ObjectProcesses(id) => {
				let Ok(process) = tg::process::Id::try_from(resource.clone()) else {
					return Ok(false);
				};
				let output = Self::has_object_process_with_transaction(
					db,
					subspace,
					transaction,
					id,
					&process,
				)?;
				return Ok(output);
			},
			Relation::ProcessParents(id) => {
				let Ok(parent) = tg::process::Id::try_from(resource.clone()) else {
					return Ok(false);
				};
				crate::Key::Process(crate::process::Key::ChildProcess {
					child: id.clone(),
					parent,
				})
			},
		};
		let key = Self::pack(subspace, &key);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the relation"))?;
		let output = value.is_some();
		Ok(output)
	}

	fn has_permissions_for_subject(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		subject: &tg::authorization::Subject,
	) -> tg::Result<bool> {
		let entries = Self::get_resource_permission_entries_for_subject_with_transaction(
			db,
			subspace,
			transaction,
			resource,
			subject,
		)?;
		let output = !entries.is_empty();
		Ok(output)
	}

	pub(super) fn enqueue_update(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: tg::Either<tg::object::Id, tg::process::Id>,
		source: Source,
		version: Option<u64>,
	) -> tg::Result<()> {
		Self::enqueue_update_with_kind(
			db,
			subspace,
			transaction,
			id,
			Kind::StorageAndMetadata,
			source,
			version,
		)
	}

	pub(super) fn enqueue_update_with_kind(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: tg::Either<tg::object::Id, tg::process::Id>,
		kind: Kind,
		source: Source,
		version: Option<u64>,
	) -> tg::Result<()> {
		let key = crate::Key::Update(crate::update::Key::Update {
			id: id.clone(),
			kind: kind.clone(),
		});
		let key = Self::pack(subspace, &key);
		let mut source = source;
		let mut version = version.unwrap_or_else(|| transaction.id() as u64);
		if let Some(existing) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get update key"))?
		{
			let (existing_source, existing_version) = match &kind {
				Kind::Permission(_) | Kind::StorageAndMetadata => {
					deserialize_source_update(&kind, existing)?
				},
				Kind::Usage(_) => (source, UsageUpdate::deserialize(existing)?.version),
			};
			if existing_source == Source::Put {
				source = Source::Put;
			}
			version = version.min(existing_version);
			if source == existing_source && version == existing_version {
				return Ok(());
			}
			if version != existing_version {
				let key = crate::Key::Update(Key::UpdateVersion {
					id: id.clone(),
					kind: kind.clone(),
					version: existing_version,
				});
				db.delete(transaction, &Self::pack(subspace, &key))
					.map_err(|error| {
						tg::error!(!error, "failed to delete the update version key")
					})?;
			}
		}

		let value = serialize_update(&kind, source, version)?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to put update key"))?;

		let key = crate::Key::Update(crate::update::Key::UpdateVersion { id, kind, version });
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put update version key"))?;

		Ok(())
	}

	pub(super) fn lower_usage_update_put_version(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		account: &tangram_index::usage::Account,
		version: u64,
	) -> tg::Result<bool> {
		let key = Key::UsageUpdatePutVersion {
			account: account.clone(),
			id: id.clone(),
		};
		let key = Self::pack(subspace, &crate::Key::Update(key));
		let previous = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get the usage update put version"))?
			.map(|bytes| bytes.try_into().map(u64::from_be_bytes))
			.transpose()
			.map_err(|error| {
				tg::error!(!error, "failed to deserialize the usage update put version")
			})?;
		if previous.is_some_and(|previous| version >= previous) {
			return Ok(false);
		}
		db.put(transaction, &key, &version.to_be_bytes())
			.map_err(|error| tg::error!(!error, "failed to put the usage update put version"))?;
		Ok(true)
	}

	pub(super) fn clear_usage_update_versions(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		account: &tangram_index::usage::Account,
	) -> tg::Result<()> {
		let key = Key::UsageUpdatePutVersion {
			account: account.clone(),
			id: id.clone(),
		};
		db.delete(transaction, &Self::pack(subspace, &crate::Key::Update(key)))
			.map_err(|error| tg::error!(!error, "failed to delete the usage update put version"))?;
		Ok(())
	}

	pub(super) fn clear_update_propagated_versions(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &[u8],
	) -> tg::Result<()> {
		for kind in [
			KeyKind::PermissionUpdatePropagatedVersion,
			KeyKind::StorageAndMetadataUpdatePropagatedVersion,
			KeyKind::UsageUpdatePutVersion,
		] {
			let prefix = Self::pack(subspace, &(kind.to_i32().unwrap(), id));
			let (_, end) = fdbt::Subspace::from_bytes(prefix.clone()).range();
			let range = (
				std::ops::Bound::Included(prefix.as_slice()),
				std::ops::Bound::Excluded(end.as_slice()),
			);
			db.delete_range(transaction, &range).map_err(|error| {
				tg::error!(!error, "failed to delete the propagated update versions")
			})?;
		}
		Ok(())
	}
}

fn update_version_key_kind(kind: tangram_index::update::Kind) -> KeyKind {
	match kind {
		tangram_index::update::Kind::Permission => KeyKind::PermissionUpdateVersion,
		tangram_index::update::Kind::StorageAndMetadata => KeyKind::StorageAndMetadataUpdateVersion,
		tangram_index::update::Kind::Usage => KeyKind::UsageUpdateVersion,
	}
}

fn deserialize_source_update(kind: &Kind, bytes: &[u8]) -> tg::Result<(Source, u64)> {
	let output = match kind {
		Kind::Permission(_) => {
			let update = PermissionUpdate::deserialize(bytes)?;
			(update.source, update.version)
		},
		Kind::StorageAndMetadata => {
			let update = StorageAndMetadataUpdate::deserialize(bytes)?;
			(update.source, update.version)
		},
		Kind::Usage(_) => return Err(tg::error!("expected a source update")),
	};

	Ok(output)
}

fn serialize_update(kind: &Kind, source: Source, version: u64) -> tg::Result<Vec<u8>> {
	let value = match kind {
		Kind::Permission(_) => PermissionUpdate::new(source, version).serialize()?,
		Kind::StorageAndMetadata => StorageAndMetadataUpdate::new(source, version).serialize()?,
		Kind::Usage(_) => UsageUpdate::new(version).serialize()?,
	};

	Ok(value)
}

fn process_object_aspect_permissions(
	set: &tangram_index::process::Set,
) -> Vec<(
	tangram_index::process::object::Kind,
	tg::authorization::Permission,
)> {
	let process = tg::authorization::Permission::Process;
	[
		(
			set.command_objects,
			tangram_index::process::object::Kind::Command,
			process(tg::authorization::permission::process::Permission::NodeCommandObjects),
		),
		(
			set.error_objects,
			tangram_index::process::object::Kind::Error,
			process(tg::authorization::permission::process::Permission::NodeErrorObjects),
		),
		(
			set.log_objects,
			tangram_index::process::object::Kind::Log,
			process(tg::authorization::permission::process::Permission::NodeLogObjects),
		),
		(
			set.output_objects,
			tangram_index::process::object::Kind::Output,
			process(tg::authorization::permission::process::Permission::NodeOutputObjects),
		),
	]
	.into_iter()
	.filter(|(set, _, _)| *set)
	.map(|(_, kind, permission)| (kind, permission))
	.collect()
}

fn process_subtree_permissions()
-> [(tg::authorization::Permission, tg::authorization::Permission); 5] {
	let process = tg::authorization::Permission::Process;
	[
		(
			process(tg::authorization::permission::process::Permission::Node),
			process(tg::authorization::permission::process::Permission::Subtree),
		),
		(
			process(tg::authorization::permission::process::Permission::NodeCommandObjects),
			process(tg::authorization::permission::process::Permission::SubtreeCommandObjects),
		),
		(
			process(tg::authorization::permission::process::Permission::NodeErrorObjects),
			process(tg::authorization::permission::process::Permission::SubtreeErrorObjects),
		),
		(
			process(tg::authorization::permission::process::Permission::NodeLogObjects),
			process(tg::authorization::permission::process::Permission::SubtreeLogObjects),
		),
		(
			process(tg::authorization::permission::process::Permission::NodeOutputObjects),
			process(tg::authorization::permission::process::Permission::SubtreeOutputObjects),
		),
	]
}
