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
	resource: &'a tg::Id,
	entries: &'a [crate::permission::PermissionEntry],
	child_entries: &'a [Vec<crate::permission::PermissionEntry>],
	command_object_entries: &'a [Vec<crate::permission::PermissionEntry>],
	error_object_entries: &'a [Vec<crate::permission::PermissionEntry>],
	log_object_entries: &'a [Vec<crate::permission::PermissionEntry>],
	output_object_entries: &'a [Vec<crate::permission::PermissionEntry>],
	set: ProcessPermissionSet,
}

#[derive(Clone, Copy)]
struct ProcessPermissionSet {
	command_objects: bool,
	error_objects: bool,
	log_objects: bool,
	output_objects: bool,
}

struct ProcessOutput {
	changed: bool,
	depth_exceeded: bool,
}

#[derive(Clone, Copy)]
struct PermissionCover {
	expires_at: Option<i64>,
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

	pub(super) fn update_batch_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		batch_size: usize,
		kind: tangram_index::update::Kind,
		max_process_depth: Option<u64>,
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
					)?,
					tg::Either::Right(id) => Self::update_process_permissions_for_subject(
						db,
						subspace,
						transaction,
						id,
						subject,
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
							false,
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
							false,
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
					Self::enqueue_parents(db, subspace, transaction, &id, &kind, version)?;
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

		let children = Self::get_object_children_with_transaction(db, subspace, transaction, id)?;

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
	) -> tg::Result<bool> {
		let resource = tg::Id::from(id.clone());
		let children =
			Self::try_get_object_children_with_transaction(db, subspace, transaction, id)?;
		let entries = Self::get_resource_permission_entries_for_subject_with_transaction(
			db,
			subspace,
			transaction,
			&resource,
			subject,
		)?;
		let child_entries = children
			.iter()
			.flatten()
			.map(|child| {
				let resource = tg::Id::from(child.clone());
				Self::get_resource_permission_entries_for_subject_with_transaction(
					db,
					subspace,
					transaction,
					&resource,
					subject,
				)
			})
			.collect::<tg::Result<Vec<_>>>()?;
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
		// Derive subtree permissions only after the object children are indexed.
		for (subject, entry_expires_at) in nodes.into_iter().filter(|_| children.is_some()) {
			let expires_at = child_entries
				.iter()
				.try_fold(entry_expires_at, |output, entries| {
					Self::permission_entries_cover_expires_at(entries, &subject, subtree)
						.map(|cover| Self::min_expires_at(output, cover.expires_at))
				});
			if let Some(expires_at) = expires_at
				&& !Self::has_non_materialized_cover(&entries, &subject, subtree, expires_at)
			{
				expected.insert((subject, subtree, expires_at));
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
		let direct = Self::get_object_processes_with_transaction(db, subspace, transaction, id)?
			.into_iter()
			.any(|(candidate, _)| candidate == *process);
		let node = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let mut anchored = direct;
		if !anchored {
			let parents = Self::get_object_parents_with_transaction(db, subspace, transaction, id)?;
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

		let permissions = entries
			.iter()
			.filter(|entry| entry.grant || entry.direct.is_some() || entry.materialized.is_some())
			.map(|entry| entry.permission)
			.collect::<BTreeSet<_>>();
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
		let object_subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let process_permission = |permission| tg::authorization::Permission::Process(permission);
		let node = process_permission(tg::authorization::permission::process::Permission::Node);
		let node_command_objects = process_permission(
			tg::authorization::permission::process::Permission::NodeCommandObjects,
		);
		let node_error_objects = process_permission(
			tg::authorization::permission::process::Permission::NodeErrorObjects,
		);
		let node_log_objects =
			process_permission(tg::authorization::permission::process::Permission::NodeLogObjects);
		let node_output_objects = process_permission(
			tg::authorization::permission::process::Permission::NodeOutputObjects,
		);
		let subtree =
			process_permission(tg::authorization::permission::process::Permission::Subtree);
		let subtree_command_objects = process_permission(
			tg::authorization::permission::process::Permission::SubtreeCommandObjects,
		);
		let subtree_error_objects = process_permission(
			tg::authorization::permission::process::Permission::SubtreeErrorObjects,
		);
		let subtree_log_objects = process_permission(
			tg::authorization::permission::process::Permission::SubtreeLogObjects,
		);
		let subtree_output_objects = process_permission(
			tg::authorization::permission::process::Permission::SubtreeOutputObjects,
		);

		let mut expected = BTreeSet::new();

		if input.set.command_objects {
			let command_object_entries = input
				.command_object_entries
				.iter()
				.map(Vec::as_slice)
				.collect::<Vec<_>>();
			Self::insert_object_aspect_permissions(
				&mut expected,
				input.entries,
				command_object_entries.iter().flat_map(|entries| *entries),
				&command_object_entries,
				object_subtree,
				node_command_objects,
			);
		}
		if input.set.error_objects {
			let error_object_entries = input
				.error_object_entries
				.iter()
				.map(Vec::as_slice)
				.collect::<Vec<_>>();
			Self::insert_object_aspect_permissions(
				&mut expected,
				input.entries,
				error_object_entries.iter().flat_map(|entries| *entries),
				&error_object_entries,
				object_subtree,
				node_error_objects,
			);
		}
		if input.set.log_objects {
			let log_object_entries = input
				.log_object_entries
				.iter()
				.map(Vec::as_slice)
				.collect::<Vec<_>>();
			Self::insert_object_aspect_permissions(
				&mut expected,
				input.entries,
				log_object_entries.iter().flat_map(|entries| *entries),
				&log_object_entries,
				object_subtree,
				node_log_objects,
			);
		}
		if input.set.output_objects {
			let output_object_entries = input
				.output_object_entries
				.iter()
				.map(Vec::as_slice)
				.collect::<Vec<_>>();
			Self::insert_object_aspect_permissions(
				&mut expected,
				input.entries,
				output_object_entries.iter().flat_map(|entries| *entries),
				&output_object_entries,
				object_subtree,
				node_output_objects,
			);
		}

		for (source, target) in [
			(node, subtree),
			(node_command_objects, subtree_command_objects),
			(node_error_objects, subtree_error_objects),
			(node_log_objects, subtree_log_objects),
			(node_output_objects, subtree_output_objects),
		] {
			for entry in input
				.entries
				.iter()
				.filter(|entry| entry.permission == source)
			{
				let Some(entry_expires_at) = entry.effective_expires_at() else {
					continue;
				};
				let expires_at =
					input
						.child_entries
						.iter()
						.try_fold(entry_expires_at, |output, entries| {
							Self::permission_entries_cover_expires_at(
								entries,
								&entry.subject,
								target,
							)
							.map(|cover| Self::min_expires_at(output, cover.expires_at))
						});
				if let Some(expires_at) = expires_at {
					if Self::has_non_materialized_cover(
						input.entries,
						&entry.subject,
						target,
						expires_at,
					) {
						continue;
					}
					expected.insert((entry.subject.clone(), target, expires_at));
				}
			}
		}

		let managed = BTreeSet::from([
			node_command_objects,
			node_error_objects,
			node_log_objects,
			node_output_objects,
			subtree,
			subtree_command_objects,
			subtree_error_objects,
			subtree_log_objects,
			subtree_output_objects,
		]);
		Self::reconcile_materialized_permissions(
			db,
			subspace,
			transaction,
			input.resource,
			input.entries,
			&expected,
			&managed,
		)
	}

	fn insert_object_aspect_permissions<'a>(
		expected: &mut BTreeSet<(
			tg::authorization::Subject,
			tg::authorization::Permission,
			Option<i64>,
		)>,
		target_entries: &[crate::permission::PermissionEntry],
		sources: impl IntoIterator<Item = &'a crate::permission::PermissionEntry>,
		required: &[&[crate::permission::PermissionEntry]],
		source_permission: tg::authorization::Permission,
		target_permission: tg::authorization::Permission,
	) {
		for entry in sources
			.into_iter()
			.filter(|entry| entry.permission == source_permission)
		{
			let Some(entry_expires_at) = entry.effective_expires_at() else {
				continue;
			};
			let expires_at = required
				.iter()
				.try_fold(entry_expires_at, |output, entries| {
					Self::permission_entries_cover_expires_at(
						entries,
						&entry.subject,
						source_permission,
					)
					.map(|cover| Self::min_expires_at(output, cover.expires_at))
				});
			if let Some(expires_at) = expires_at {
				if Self::has_non_materialized_cover(
					target_entries,
					&entry.subject,
					target_permission,
					expires_at,
				) {
					continue;
				}
				expected.insert((entry.subject.clone(), target_permission, expires_at));
			}
		}
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
	) -> tg::Result<bool> {
		let key = crate::Key::Process(crate::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
		let Some(bytes) = bytes else {
			return Ok(false);
		};
		let process = tangram_index::process::Process::deserialize(bytes)?;
		let resource = tg::Id::from(id.clone());
		let entries = Self::get_resource_permission_entries_for_subject_with_transaction(
			db,
			subspace,
			transaction,
			&resource,
			subject,
		)?;
		let children = Self::get_process_children_with_transaction(db, subspace, transaction, id)?;
		let child_entries = children
			.iter()
			.map(|child| {
				let resource = tg::Id::from(child.clone());
				Self::get_resource_permission_entries_for_subject_with_transaction(
					db,
					subspace,
					transaction,
					&resource,
					subject,
				)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let objects = Self::get_process_objects_with_transaction(db, subspace, transaction, id)?;
		let mut command_object_entries: Vec<Vec<crate::permission::PermissionEntry>> = Vec::new();
		let mut error_object_entries: Vec<Vec<crate::permission::PermissionEntry>> = Vec::new();
		let mut log_object_entries: Vec<Vec<crate::permission::PermissionEntry>> = Vec::new();
		let mut output_object_entries: Vec<Vec<crate::permission::PermissionEntry>> = Vec::new();
		for (object, kind) in objects {
			let resource = tg::Id::from(object);
			let entries = Self::get_resource_permission_entries_for_subject_with_transaction(
				db,
				subspace,
				transaction,
				&resource,
				subject,
			)?;
			match kind {
				tangram_index::process::object::Kind::Command => {
					command_object_entries.push(entries);
				},
				tangram_index::process::object::Kind::Error => {
					error_object_entries.push(entries);
				},
				tangram_index::process::object::Kind::Log => {
					log_object_entries.push(entries);
				},
				tangram_index::process::object::Kind::Output => {
					output_object_entries.push(entries);
				},
			}
		}
		let entry = ProcessPermissionInputs {
			resource: &resource,
			entries: &entries,
			child_entries: &child_entries,
			command_object_entries: &command_object_entries,
			error_object_entries: &error_object_entries,
			log_object_entries: &log_object_entries,
			output_object_entries: &output_object_entries,
			set: ProcessPermissionSet {
				command_objects: process.set.command_objects,
				error_objects: process.set.error_objects,
				log_objects: process.set.log_objects,
				output_objects: process.set.output_objects,
			},
		};
		Self::update_process_permissions(db, subspace, transaction, &entry)
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

		let children = Self::get_process_children_with_transaction(db, subspace, transaction, id)?;
		let children = children
			.iter()
			.map(|child| Self::try_get_process_with_transaction(db, subspace, transaction, child))
			.collect::<tg::Result<Vec<_>>>()?;

		let objects = Self::get_process_objects_with_transaction(db, subspace, transaction, id)?;
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
				&& process
					.data
					.as_ref()
					.is_some_and(|data| !data.status.is_finished())
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

				let parents =
					Self::get_object_parents_with_transaction(db, subspace, transaction, id)?;
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
				let process_parents =
					Self::get_object_processes_with_transaction(db, subspace, transaction, id)?;
				for (process, _kind) in process_parents {
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
			tg::Either::Right(id) => {
				let parents =
					Self::get_process_parents_with_transaction(db, subspace, transaction, id)?;
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
