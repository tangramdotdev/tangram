mod key;

pub(super) use key::{Key, Kind, StorageKind};

use {
	super::{Db, Index, Kind as KeyKind, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) struct GrantUpdate {
	#[tangram_serialize(id = 0)]
	pub source: Source,
}

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) struct NodeUpdate {
	#[tangram_serialize(id = 0)]
	pub source: Source,
}

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) struct StorageUpdate {}

#[derive(
	Clone, Copy, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) enum Source {
	#[tangram_serialize(id = 0)]
	Put,

	#[tangram_serialize(id = 1)]
	Propagate,
}

struct ProcessGrantInputs<'a> {
	resource: &'a tg::Id,
	entries: &'a [crate::lmdb::grant::GrantEntry],
	child_entries: &'a [Vec<crate::lmdb::grant::GrantEntry>],
	command_object_entries: Option<&'a [crate::lmdb::grant::GrantEntry]>,
	error_object_entries: &'a [Vec<crate::lmdb::grant::GrantEntry>],
	log_object_entries: Option<&'a [crate::lmdb::grant::GrantEntry]>,
	output_object_entries: &'a [Vec<crate::lmdb::grant::GrantEntry>],
	set: ProcessGrantSet,
}

#[derive(Clone, Copy)]
struct ProcessGrantSet {
	error: bool,
	output: bool,
}

struct ProcessOutput {
	changed: bool,
	depth_exceeded: bool,
}

#[derive(Clone, Copy)]
struct GrantCover {
	expires_at: Option<i64>,
}

impl GrantUpdate {
	pub fn new(source: Source) -> Self {
		Self { source }
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the grant update"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the grant update"))
	}
}

impl NodeUpdate {
	pub fn new(source: Source) -> Self {
		Self { source }
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the node update"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the node update"))
	}
}

impl StorageUpdate {
	pub fn new() -> Self {
		Self {}
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the storage update"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the storage update"))
	}
}

impl Index {
	pub async fn try_get_oldest_update_transaction_id(
		&self,
		kind: crate::update::Kind,
	) -> tg::Result<Option<u64>> {
		let response = self
			.send_read_request(crate::read::Request::TryGetOldestUpdateTransactionId { kind })
			.await?;
		let crate::read::Response::TryGetOldestUpdateTransactionId(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn try_get_oldest_update_transaction_id_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		kind: crate::update::Kind,
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
		let crate::lmdb::Key::Update(crate::lmdb::update::Key::UpdateVersion { version, .. }) = key
		else {
			return Err(tg::error!("unexpected key type"));
		};

		Ok(Some(version))
	}

	pub async fn update_batch(
		&self,
		kind: crate::update::Kind,
		batch_size: usize,
	) -> tg::Result<crate::update::Output> {
		let request = Request::Update(crate::lmdb::Update { batch_size, kind });
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
		kind: crate::update::Kind,
		max_process_depth: Option<u64>,
		usage_partition_total: u64,
	) -> tg::Result<crate::update::Output> {
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
				let crate::lmdb::Key::Update(crate::lmdb::update::Key::UpdateVersion {
					version,
					id,
					kind,
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((version, id, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		let mut output = crate::update::Output::default();
		for (version, id, kind) in entries {
			let key = crate::lmdb::Key::Update(crate::lmdb::update::Key::Update {
				id: id.clone(),
				kind: kind.clone(),
			});
			let key = Self::pack(subspace, &key);
			let value = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get update key"))?
				.ok_or_else(|| tg::error!("expected an update key for the update version key"))?;

			let source = match &kind {
				Kind::Grant(_) | Kind::Node => Some(deserialize_source_update(&kind, value)?),
				Kind::Storage(_) => {
					StorageUpdate::deserialize(value)?;
					None
				},
			};

			let changed = match &kind {
				Kind::Grant(subject) => match &id {
					tg::Either::Left(id) => Self::update_object_grants_for_subject(
						db,
						subspace,
						transaction,
						id,
						subject,
					)?,
					tg::Either::Right(id) => Self::update_process_grants_for_subject(
						db,
						subspace,
						transaction,
						id,
						subject,
					)?,
				},
				Kind::Node => match &id {
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
				Kind::Storage(StorageKind::Add {
					account,
					touched_at,
				}) => match &id {
					tg::Either::Left(object) => {
						let entry = crate::usage::storage::put::ObjectArg {
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
						let entry = crate::usage::storage::put::ProcessArg {
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
				Kind::Storage(
					StorageKind::Clean(_) | StorageKind::CleanAll | StorageKind::Propagate { .. },
				) => return Err(tg::error!("unsupported LMDB storage update kind")),
			};

			if !matches!(kind, Kind::Storage(_))
				&& match source.unwrap() {
					Source::Put => true,
					Source::Propagate => changed,
				} {
				Self::enqueue_parents(db, subspace, transaction, &id, &kind, version)?;
			}

			let key = crate::lmdb::Key::Update(crate::lmdb::update::Key::Update {
				id: id.clone(),
				kind: kind.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete update key"))?;
			let key = crate::lmdb::Key::Update(crate::lmdb::update::Key::UpdateVersion {
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
		let key = crate::lmdb::Key::Object(crate::lmdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?;
		let Some(bytes) = bytes else {
			return Ok(false);
		};
		let mut object = crate::object::Object::deserialize(bytes)?;

		let children = Self::get_object_children_with_transaction(db, subspace, transaction, id)?;

		let child_objects: Vec<Option<crate::object::Object>> = children
			.iter()
			.map(|child| Self::try_get_object_with_transaction(db, subspace, transaction, child))
			.collect::<tg::Result<_>>()?;

		let mut changed = false;

		if !object.storage.subtree {
			let value = child_objects
				.iter()
				.all(|child| child.as_ref().is_some_and(|object| object.storage.subtree));
			if value {
				object.storage.subtree = true;
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

	fn update_object_grants_for_subject(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
		subject: &tg::authorization::Subject,
	) -> tg::Result<bool> {
		let resource = tg::Id::from(id.clone());
		let children = Self::get_object_children_with_transaction(db, subspace, transaction, id)?;
		let entries = Self::get_resource_grant_entries_for_subject_with_transaction(
			db,
			subspace,
			transaction,
			&resource,
			subject,
		)?;
		let child_entries = children
			.iter()
			.map(|child| {
				let resource = tg::Id::from(child.clone());
				Self::get_resource_grant_entries_for_subject_with_transaction(
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
		for entry in entries.iter().filter(|entry| entry.permission == node) {
			let Some(entry_expires_at) = entry.effective_expires_at() else {
				continue;
			};
			let expires_at = child_entries
				.iter()
				.try_fold(entry_expires_at, |output, entries| {
					Self::grant_entries_cover_expires_at(entries, &entry.subject, subtree)
						.map(|cover| Self::min_expires_at(output, cover.expires_at))
				});
			if let Some(expires_at) = expires_at {
				if Self::has_non_materialized_cover(&entries, &entry.subject, subtree, expires_at) {
					continue;
				}
				expected.insert((entry.subject.clone(), subtree, expires_at));
			}
		}
		let managed = BTreeSet::from([subtree]);
		let materialized_changed = Self::reconcile_materialized_grants(
			db,
			subspace,
			transaction,
			&resource,
			&entries,
			&expected,
			&managed,
		)?;
		let entries = Self::get_resource_grant_entries_for_subject_with_transaction(
			db,
			subspace,
			transaction,
			&resource,
			subject,
		)?;
		let implicit_changed = Self::promote_process_implicit_grants_for_subject(
			db,
			subspace,
			transaction,
			id,
			subject,
			&entries,
		)?;

		Ok(implicit_changed || materialized_changed)
	}

	fn promote_process_implicit_grants_for_subject(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
		subject: &tg::authorization::Subject,
		entries: &[crate::lmdb::grant::GrantEntry],
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
				let entries = Self::get_resource_grant_entries_for_subject_with_transaction(
					db,
					subspace,
					transaction,
					&resource,
					subject,
				)?;
				if entries.iter().any(|entry| {
					entry.is_non_expiring_process_implicit() && entry.permission.implies(node)
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
			.filter(|entry| {
				entry.explicit || entry.implicit.is_some() || entry.materialized.is_some()
			})
			.map(|entry| entry.permission)
			.collect::<BTreeSet<_>>();
		let creator = tg::Principal::Process(process.clone());
		let resource = tg::Id::from(id.clone());
		let mut changed = false;
		for permission in permissions {
			let entry = crate::lmdb::grant::GrantIndexEntry {
				creator: Some(&creator),
				expires_at: None,
				permission,
				resource: &resource,
				subject,
			};
			if Self::put_grant_index_entry(
				db,
				subspace,
				transaction,
				&entry,
				crate::lmdb::grant::GrantSource::Implicit,
				None,
			)? {
				changed = true;
			}
		}

		Ok(changed)
	}

	fn reconcile_materialized_grants(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		resource: &tg::Id,
		entries: &[crate::lmdb::grant::GrantEntry],
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
			let entry = crate::lmdb::grant::GrantIndexEntry {
				creator: None,
				expires_at: *expires_at,
				permission: *permission,
				subject,
				resource,
			};
			if Self::delete_grant_index_entry(
				db,
				subspace,
				transaction,
				&entry,
				crate::lmdb::grant::GrantSource::Materialized,
			)? {
				changed = true;
			}
		}
		for (subject, permission, expires_at) in expected.difference(&current) {
			let entry = crate::lmdb::grant::GrantIndexEntry {
				creator: None,
				expires_at: *expires_at,
				permission: *permission,
				subject,
				resource,
			};
			if Self::put_grant_index_entry(
				db,
				subspace,
				transaction,
				&entry,
				crate::lmdb::grant::GrantSource::Materialized,
				None,
			)? {
				changed = true;
			}
		}
		Ok(changed)
	}

	fn update_process_grants(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		input: &ProcessGrantInputs<'_>,
	) -> tg::Result<bool> {
		let object_subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let process_permission = |permission| tg::authorization::Permission::Process(permission);
		let node = process_permission(tg::authorization::permission::process::Permission::Node);
		let node_command =
			process_permission(tg::authorization::permission::process::Permission::NodeCommand);
		let node_error =
			process_permission(tg::authorization::permission::process::Permission::NodeError);
		let node_log =
			process_permission(tg::authorization::permission::process::Permission::NodeLog);
		let node_output =
			process_permission(tg::authorization::permission::process::Permission::NodeOutput);
		let subtree =
			process_permission(tg::authorization::permission::process::Permission::Subtree);
		let subtree_command =
			process_permission(tg::authorization::permission::process::Permission::SubtreeCommand);
		let subtree_error =
			process_permission(tg::authorization::permission::process::Permission::SubtreeError);
		let subtree_log =
			process_permission(tg::authorization::permission::process::Permission::SubtreeLog);
		let subtree_output =
			process_permission(tg::authorization::permission::process::Permission::SubtreeOutput);

		let mut expected = BTreeSet::new();

		if let Some(command_object_entries) = input.command_object_entries {
			Self::insert_object_aspect_grants(
				&mut expected,
				input.entries,
				command_object_entries,
				&[command_object_entries],
				object_subtree,
				node_command,
			);
		}
		if input.set.error {
			let error_object_entries = input
				.error_object_entries
				.iter()
				.map(Vec::as_slice)
				.collect::<Vec<_>>();
			Self::insert_object_aspect_grants(
				&mut expected,
				input.entries,
				error_object_entries.iter().flat_map(|entries| *entries),
				&error_object_entries,
				object_subtree,
				node_error,
			);
		}
		if let Some(log_object_entries) = input.log_object_entries {
			Self::insert_object_aspect_grants(
				&mut expected,
				input.entries,
				log_object_entries,
				&[log_object_entries],
				object_subtree,
				node_log,
			);
		}
		if input.set.output {
			let output_object_entries = input
				.output_object_entries
				.iter()
				.map(Vec::as_slice)
				.collect::<Vec<_>>();
			Self::insert_object_aspect_grants(
				&mut expected,
				input.entries,
				output_object_entries.iter().flat_map(|entries| *entries),
				&output_object_entries,
				object_subtree,
				node_output,
			);
		}

		for (source, target) in [
			(node, subtree),
			(node_command, subtree_command),
			(node_error, subtree_error),
			(node_log, subtree_log),
			(node_output, subtree_output),
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
							Self::grant_entries_cover_expires_at(entries, &entry.subject, target)
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
			node_command,
			node_error,
			node_log,
			node_output,
			subtree,
			subtree_command,
			subtree_error,
			subtree_log,
			subtree_output,
		]);
		Self::reconcile_materialized_grants(
			db,
			subspace,
			transaction,
			input.resource,
			input.entries,
			&expected,
			&managed,
		)
	}

	fn insert_object_aspect_grants<'a>(
		expected: &mut BTreeSet<(
			tg::authorization::Subject,
			tg::authorization::Permission,
			Option<i64>,
		)>,
		target_entries: &[crate::lmdb::grant::GrantEntry],
		sources: impl IntoIterator<Item = &'a crate::lmdb::grant::GrantEntry>,
		required: &[&[crate::lmdb::grant::GrantEntry]],
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
					Self::grant_entries_cover_expires_at(entries, &entry.subject, source_permission)
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
		entries: &[crate::lmdb::grant::GrantEntry],
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

	fn grant_entries_cover_expires_at(
		entries: &[crate::lmdb::grant::GrantEntry],
		subject: &tg::authorization::Subject,
		permission: tg::authorization::Permission,
	) -> Option<GrantCover> {
		entries
			.iter()
			.filter(|entry| entry.subject == *subject && entry.permission == permission)
			.filter_map(|entry| {
				entry
					.effective_expires_at()
					.map(|expires_at| GrantCover { expires_at })
			})
			.reduce(|left, right| GrantCover {
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

	fn update_process_grants_for_subject(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::process::Id,
		subject: &tg::authorization::Subject,
	) -> tg::Result<bool> {
		let key = crate::lmdb::Key::Process(crate::lmdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
		let Some(bytes) = bytes else {
			return Ok(false);
		};
		let process = crate::process::Process::deserialize(bytes)?;
		let resource = tg::Id::from(id.clone());
		let entries = Self::get_resource_grant_entries_for_subject_with_transaction(
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
				Self::get_resource_grant_entries_for_subject_with_transaction(
					db,
					subspace,
					transaction,
					&resource,
					subject,
				)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let objects = Self::get_process_objects_with_transaction(db, subspace, transaction, id)?;
		let mut command_object_entries: Option<Vec<crate::lmdb::grant::GrantEntry>> = None;
		let mut error_object_entries: Vec<Vec<crate::lmdb::grant::GrantEntry>> = Vec::new();
		let mut log_object_entries: Option<Vec<crate::lmdb::grant::GrantEntry>> = None;
		let mut output_object_entries: Vec<Vec<crate::lmdb::grant::GrantEntry>> = Vec::new();
		for (object, kind) in objects {
			let resource = tg::Id::from(object);
			let entries = Self::get_resource_grant_entries_for_subject_with_transaction(
				db,
				subspace,
				transaction,
				&resource,
				subject,
			)?;
			match kind {
				crate::process::object::Kind::Command => {
					command_object_entries = Some(entries);
				},
				crate::process::object::Kind::Error => {
					error_object_entries.push(entries);
				},
				crate::process::object::Kind::Log => {
					log_object_entries = Some(entries);
				},
				crate::process::object::Kind::Output => {
					output_object_entries.push(entries);
				},
			}
		}
		let entry = ProcessGrantInputs {
			resource: &resource,
			entries: &entries,
			child_entries: &child_entries,
			command_object_entries: command_object_entries.as_deref(),
			error_object_entries: &error_object_entries,
			log_object_entries: log_object_entries.as_deref(),
			output_object_entries: &output_object_entries,
			set: ProcessGrantSet {
				error: process.set.error,
				output: process.set.output,
			},
		};
		Self::update_process_grants(db, subspace, transaction, &entry)
	}

	fn update_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::process::Id,
		max_process_depth: Option<u64>,
	) -> tg::Result<ProcessOutput> {
		let process_key = crate::lmdb::Key::Process(crate::lmdb::process::Key::Process(id.clone()));
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
		let mut process = crate::process::Process::deserialize(bytes)?;

		let children = Self::get_process_children_with_transaction(db, subspace, transaction, id)?;
		let children = children
			.iter()
			.map(|child| Self::try_get_process_with_transaction(db, subspace, transaction, child))
			.collect::<tg::Result<Vec<_>>>()?;

		let objects = Self::get_process_objects_with_transaction(db, subspace, transaction, id)?;
		let mut command_object: Option<crate::object::Object> = None;
		let mut error_objects: Vec<Option<crate::object::Object>> = Vec::new();
		let mut log_object: Option<Option<crate::object::Object>> = None;
		let mut output_objects: Vec<Option<crate::object::Object>> = Vec::new();
		for (id, kind) in &objects {
			let object = Self::try_get_object_with_transaction(db, subspace, transaction, id)?;
			match kind {
				crate::process::object::Kind::Command => {
					command_object = object;
				},
				crate::process::object::Kind::Error => {
					error_objects.push(object);
				},
				crate::process::object::Kind::Log => {
					log_object = Some(object);
				},
				crate::process::object::Kind::Output => {
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

		if let Some(object) = &command_object {
			if process.metadata.node.command.count.is_none()
				&& let Some(value) = object.metadata.subtree.count
			{
				process.metadata.node.command.count = Some(value);
				changed = true;
			}
			if process.metadata.node.command.depth.is_none()
				&& let Some(value) = object.metadata.subtree.depth
			{
				process.metadata.node.command.depth = Some(value);
				changed = true;
			}
			if process.metadata.node.command.size.is_none()
				&& let Some(value) = object.metadata.subtree.size
			{
				process.metadata.node.command.size = Some(value);
				changed = true;
			}
			if process.metadata.node.command.solvable.is_none()
				&& let Some(value) = object.metadata.subtree.solvable
			{
				process.metadata.node.command.solvable = Some(value);
				changed = true;
			}
			if process.metadata.node.command.solved.is_none()
				&& let Some(value) = object.metadata.subtree.solved
			{
				process.metadata.node.command.solved = Some(value);
				changed = true;
			}
		}

		if process.set.error {
			if process.metadata.node.error.count.is_none() {
				let value = error_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.count)
					})
					.sum::<Option<u64>>();
				if let Some(value) = value {
					process.metadata.node.error.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.error.depth.is_none() {
				let value = error_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.depth)
					})
					.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
				if let Some(value) = value {
					process.metadata.node.error.depth = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.error.size.is_none() {
				let value = error_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.size)
					})
					.sum::<Option<u64>>();
				if let Some(value) = value {
					process.metadata.node.error.size = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.error.solvable.is_none() {
				let value = error_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.solvable)
					})
					.try_fold(false, |output, value| value.map(|value| output || value));
				if let Some(value) = value {
					process.metadata.node.error.solvable = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.error.solved.is_none() {
				let value = error_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.solved)
					})
					.try_fold(true, |output, value| value.map(|value| output && value));
				if let Some(value) = value {
					process.metadata.node.error.solved = Some(value);
					changed = true;
				}
			}
		}

		if process.set.log {
			if let Some(Some(object)) = &log_object {
				if process.metadata.node.log.count.is_none()
					&& let Some(value) = object.metadata.subtree.count
				{
					process.metadata.node.log.count = Some(value);
					changed = true;
				}
				if process.metadata.node.log.depth.is_none()
					&& let Some(value) = object.metadata.subtree.depth
				{
					process.metadata.node.log.depth = Some(value);
					changed = true;
				}
				if process.metadata.node.log.size.is_none()
					&& let Some(value) = object.metadata.subtree.size
				{
					process.metadata.node.log.size = Some(value);
					changed = true;
				}
				if process.metadata.node.log.solvable.is_none()
					&& let Some(value) = object.metadata.subtree.solvable
				{
					process.metadata.node.log.solvable = Some(value);
					changed = true;
				}
				if process.metadata.node.log.solved.is_none()
					&& let Some(value) = object.metadata.subtree.solved
				{
					process.metadata.node.log.solved = Some(value);
					changed = true;
				}
			} else if log_object.is_none() {
				if process.metadata.node.log.count.is_none() {
					process.metadata.node.log.count = Some(0);
					changed = true;
				}
				if process.metadata.node.log.depth.is_none() {
					process.metadata.node.log.depth = Some(0);
					changed = true;
				}
				if process.metadata.node.log.size.is_none() {
					process.metadata.node.log.size = Some(0);
					changed = true;
				}
				if process.metadata.node.log.solvable.is_none() {
					process.metadata.node.log.solvable = Some(false);
					changed = true;
				}
				if process.metadata.node.log.solved.is_none() {
					process.metadata.node.log.solved = Some(true);
					changed = true;
				}
			}
		}

		if process.set.output {
			if process.metadata.node.output.count.is_none() {
				let value = output_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.count)
					})
					.sum::<Option<u64>>();
				if let Some(value) = value {
					process.metadata.node.output.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.output.depth.is_none() {
				let value = output_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.depth)
					})
					.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
				if let Some(value) = value {
					process.metadata.node.output.depth = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.output.size.is_none() {
				let value = output_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.size)
					})
					.sum::<Option<u64>>();
				if let Some(value) = value {
					process.metadata.node.output.size = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.output.solvable.is_none() {
				let value = output_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.solvable)
					})
					.try_fold(false, |output, value| value.map(|value| output || value));
				if let Some(value) = value {
					process.metadata.node.output.solvable = Some(value);
					changed = true;
				}
			}

			if process.metadata.node.output.solved.is_none() {
				let value = output_objects
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|object| object.metadata.subtree.solved)
					})
					.try_fold(true, |output, value| value.map(|value| output && value));
				if let Some(value) = value {
					process.metadata.node.output.solved = Some(value);
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

			if process.metadata.subtree.command.count.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.command.count)
					})
					.fold(process.metadata.node.command.count, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					process.metadata.subtree.command.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.command.depth.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.command.depth)
					})
					.fold(process.metadata.node.command.depth, |output, value| {
						output.and_then(|output| value.map(|value| output.max(value)))
					});
				if let Some(value) = value {
					process.metadata.subtree.command.depth = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.command.size.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.command.size)
					})
					.fold(process.metadata.node.command.size, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					process.metadata.subtree.command.size = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.command.solvable.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.command.solvable)
					})
					.fold(process.metadata.node.command.solvable, |output, value| {
						output.and_then(|output| value.map(|value| output || value))
					});
				if let Some(value) = value {
					process.metadata.subtree.command.solvable = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.command.solved.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.command.solved)
					})
					.fold(process.metadata.node.command.solved, |output, value| {
						output.and_then(|output| value.map(|value| output && value))
					});
				if let Some(value) = value {
					process.metadata.subtree.command.solved = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.error.count.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.error.count)
					})
					.fold(process.metadata.node.error.count, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					process.metadata.subtree.error.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.error.depth.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.error.depth)
					})
					.fold(process.metadata.node.error.depth, |output, value| {
						output.and_then(|output| value.map(|value| output.max(value)))
					});
				if let Some(value) = value {
					process.metadata.subtree.error.depth = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.error.size.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.error.size)
					})
					.fold(process.metadata.node.error.size, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					process.metadata.subtree.error.size = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.error.solvable.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.error.solvable)
					})
					.fold(process.metadata.node.error.solvable, |output, value| {
						output.and_then(|output| value.map(|value| output || value))
					});
				if let Some(value) = value {
					process.metadata.subtree.error.solvable = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.error.solved.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.error.solved)
					})
					.fold(process.metadata.node.error.solved, |output, value| {
						output.and_then(|output| value.map(|value| output && value))
					});
				if let Some(value) = value {
					process.metadata.subtree.error.solved = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.log.count.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.log.count)
					})
					.fold(process.metadata.node.log.count, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					process.metadata.subtree.log.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.log.depth.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.log.depth)
					})
					.fold(process.metadata.node.log.depth, |output, value| {
						output.and_then(|output| value.map(|value| output.max(value)))
					});
				if let Some(value) = value {
					process.metadata.subtree.log.depth = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.log.size.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.log.size)
					})
					.fold(process.metadata.node.log.size, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					process.metadata.subtree.log.size = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.log.solvable.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.log.solvable)
					})
					.fold(process.metadata.node.log.solvable, |output, value| {
						output.and_then(|output| value.map(|value| output || value))
					});
				if let Some(value) = value {
					process.metadata.subtree.log.solvable = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.log.solved.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.log.solved)
					})
					.fold(process.metadata.node.log.solved, |output, value| {
						output.and_then(|output| value.map(|value| output && value))
					});
				if let Some(value) = value {
					process.metadata.subtree.log.solved = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.output.count.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.output.count)
					})
					.fold(process.metadata.node.output.count, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					process.metadata.subtree.output.count = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.output.depth.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.output.depth)
					})
					.fold(process.metadata.node.output.depth, |output, value| {
						output.and_then(|output| value.map(|value| output.max(value)))
					});
				if let Some(value) = value {
					process.metadata.subtree.output.depth = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.output.size.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.output.size)
					})
					.fold(process.metadata.node.output.size, |output, value| {
						output.and_then(|output| value.map(|value| output + value))
					});
				if let Some(value) = value {
					process.metadata.subtree.output.size = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.output.solvable.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.output.solvable)
					})
					.fold(process.metadata.node.output.solvable, |output, value| {
						output.and_then(|output| value.map(|value| output || value))
					});
				if let Some(value) = value {
					process.metadata.subtree.output.solvable = Some(value);
					changed = true;
				}
			}

			if process.metadata.subtree.output.solved.is_none() {
				let value = children
					.iter()
					.map(|option| {
						option
							.as_ref()
							.and_then(|child| child.metadata.subtree.output.solved)
					})
					.fold(process.metadata.node.output.solved, |output, value| {
						output.and_then(|output| value.map(|value| output && value))
					});
				if let Some(value) = value {
					process.metadata.subtree.output.solved = Some(value);
					changed = true;
				}
			}
		}

		if let Some(object) = &command_object
			&& !process.storage.node_command
			&& object.storage.subtree
		{
			process.storage.node_command = true;
			changed = true;
		}

		if process.set.error && !process.storage.node_error {
			let value = error_objects
				.iter()
				.all(|option| option.as_ref().is_some_and(|object| object.storage.subtree));
			if value {
				process.storage.node_error = true;
				changed = true;
			}
		}

		if process.set.log {
			if let Some(Some(object)) = &log_object {
				if !process.storage.node_log && object.storage.subtree {
					process.storage.node_log = true;
					changed = true;
				}
			} else if log_object.is_none() && !process.storage.node_log {
				process.storage.node_log = true;
				changed = true;
			}
		}

		if process.set.output && !process.storage.node_output {
			let value = output_objects
				.iter()
				.all(|option| option.as_ref().is_some_and(|object| object.storage.subtree));
			if value {
				process.storage.node_output = true;
				changed = true;
			}
		}

		if process.set.children && !process.storage.subtree {
			let value = children
				.iter()
				.all(|child| child.as_ref().is_some_and(|child| child.storage.subtree));
			if value {
				process.storage.subtree = true;
				changed = true;
			}
		}

		if process.set.children {
			if !process.storage.subtree_command && process.storage.node_command {
				let value = children.iter().all(|child| {
					child
						.as_ref()
						.is_some_and(|child| child.storage.subtree_command)
				});
				if value {
					process.storage.subtree_command = true;
					changed = true;
				}
			}

			if !process.storage.subtree_error && process.storage.node_error {
				let value = children.iter().all(|child| {
					child
						.as_ref()
						.is_some_and(|child| child.storage.subtree_error)
				});
				if value {
					process.storage.subtree_error = true;
					changed = true;
				}
			}

			if !process.storage.subtree_log && process.storage.node_log {
				let value = children.iter().all(|child| {
					child
						.as_ref()
						.is_some_and(|child| child.storage.subtree_log)
				});
				if value {
					process.storage.subtree_log = true;
					changed = true;
				}
			}

			if !process.storage.subtree_output && process.storage.node_output {
				let value = children.iter().all(|child| {
					child
						.as_ref()
						.is_some_and(|child| child.storage.subtree_output)
				});
				if value {
					process.storage.subtree_output = true;
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
		Self::enqueue_update_with_kind(db, subspace, transaction, id, Kind::Node, source, version)
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
		let key = crate::lmdb::Key::Update(crate::lmdb::update::Key::Update {
			id: id.clone(),
			kind: kind.clone(),
		});
		let key = Self::pack(subspace, &key);
		if let Some(existing) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get update key"))?
		{
			if matches!(kind, Kind::Storage(_)) {
				return Ok(());
			}
			let existing = deserialize_source_update(&kind, existing)?;
			if existing == Source::Propagate && source == Source::Put {
				let value = serialize_update(&kind, source)?;
				db.put(transaction, &key, &value)
					.map_err(|error| tg::error!(!error, "failed to put update key"))?;
			}
			return Ok(());
		}

		let value = serialize_update(&kind, source)?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to put update key"))?;

		let version = version.unwrap_or_else(|| transaction.id() as u64);
		let key =
			crate::lmdb::Key::Update(crate::lmdb::update::Key::UpdateVersion { id, kind, version });
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put update version key"))?;

		Ok(())
	}
}

fn update_version_key_kind(kind: crate::update::Kind) -> KeyKind {
	match kind {
		crate::update::Kind::Grant => KeyKind::GrantUpdateVersion,
		crate::update::Kind::Node => KeyKind::NodeUpdateVersion,
		crate::update::Kind::Storage => KeyKind::StorageUpdateVersion,
	}
}

fn deserialize_source_update(kind: &Kind, bytes: &[u8]) -> tg::Result<Source> {
	let source = match kind {
		Kind::Grant(_) => GrantUpdate::deserialize(bytes)?.source,
		Kind::Node => NodeUpdate::deserialize(bytes)?.source,
		Kind::Storage(_) => return Err(tg::error!("expected a source update")),
	};

	Ok(source)
}

fn serialize_update(kind: &Kind, source: Source) -> tg::Result<Vec<u8>> {
	let value = match kind {
		Kind::Grant(_) => GrantUpdate::new(source).serialize()?,
		Kind::Node => NodeUpdate::new(source).serialize()?,
		Kind::Storage(_) => StorageUpdate::new().serialize()?,
	};

	Ok(value)
}
