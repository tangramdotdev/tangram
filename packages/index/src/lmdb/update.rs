mod key;

pub(super) use key::{Key, Kind};

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
pub(super) struct Update {
	#[tangram_serialize(id = 0)]
	pub source: Source,
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

impl Update {
	pub fn new(source: Source) -> Self {
		Self { source }
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the update"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the update"))
	}
}

impl Index {
	pub async fn try_get_oldest_update_transaction_id(&self) -> tg::Result<Option<u64>> {
		let response = self
			.send_read_request(crate::read::Request::TryGetOldestUpdateTransactionId)
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
	) -> tg::Result<Option<u64>> {
		let prefix = &(KeyKind::UpdateVersion.to_i32().unwrap(),);
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
		batch_size: usize,
		_partition_start: u64,
		_partition_end: u64,
	) -> tg::Result<crate::update::Output> {
		let request = Request::Update(crate::lmdb::Update { batch_size });
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
		max_process_depth: Option<u64>,
	) -> tg::Result<crate::update::Output> {
		let prefix = &(KeyKind::UpdateVersion.to_i32().unwrap(),);
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

			let update = Update::deserialize(value)?;

			let changed = match &kind {
				Kind::Item => match &id {
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
				Kind::Grants(principal) => match &id {
					tg::Either::Left(id) => Self::update_object_grants_for_principal(
						db,
						subspace,
						transaction,
						id,
						principal,
					)?,
					tg::Either::Right(id) => Self::update_process_grants_for_principal(
						db,
						subspace,
						transaction,
						id,
						principal,
					)?,
				},
			};

			if match update.source {
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
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
			.ok_or_else(|| tg::error!(%id, "object not found"))?;
		let mut object = crate::object::Object::deserialize(bytes)?;

		let children = Self::get_object_children_with_transaction(db, subspace, transaction, id)?;

		let child_objects: Vec<Option<crate::object::Object>> = children
			.iter()
			.map(|child| Self::try_get_object_with_transaction(db, subspace, transaction, child))
			.collect::<tg::Result<_>>()?;

		let mut changed = false;

		if !object.stored.subtree {
			let value = child_objects
				.iter()
				.all(|child| child.as_ref().is_some_and(|object| object.stored.subtree));
			if value {
				object.stored.subtree = true;
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

	fn update_object_grants_for_principal(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
		principal: &tg::grant::Principal,
	) -> tg::Result<bool> {
		let resource = tg::Id::from(id.clone());
		let children = Self::get_object_children_with_transaction(db, subspace, transaction, id)?;
		let entries = Self::get_resource_grant_entries_for_principal_with_transaction(
			db,
			subspace,
			transaction,
			&resource,
			principal,
		)?;
		let child_entries = children
			.iter()
			.map(|child| {
				let resource = tg::Id::from(child.clone());
				Self::get_resource_grant_entries_for_principal_with_transaction(
					db,
					subspace,
					transaction,
					&resource,
					principal,
				)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let node = tg::grant::Permission::Object(tg::grant::permission::object::Permission::Node);
		let subtree =
			tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree);
		let mut expected = BTreeSet::new();
		for entry in entries.iter().filter(|entry| entry.permission == node) {
			let Some(entry_expires_at) = entry.effective_expires_at() else {
				continue;
			};
			let expires_at = child_entries
				.iter()
				.try_fold(entry_expires_at, |output, entries| {
					Self::grant_entries_cover_expires_at(entries, &entry.principal, subtree)
						.map(|cover| Self::min_expires_at(output, cover.expires_at))
				});
			if let Some(expires_at) = expires_at {
				if Self::has_non_materialized_cover(&entries, &entry.principal, subtree, expires_at)
				{
					continue;
				}
				expected.insert((entry.principal.clone(), subtree, expires_at));
			}
		}
		let managed = BTreeSet::from([subtree]);
		Self::reconcile_materialized_grants(
			db,
			subspace,
			transaction,
			&resource,
			&entries,
			&expected,
			&managed,
		)
	}

	fn reconcile_materialized_grants(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		resource: &tg::Id,
		entries: &[crate::lmdb::grant::GrantEntry],
		expected: &BTreeSet<(tg::grant::Principal, tg::grant::Permission, Option<i64>)>,
		managed: &BTreeSet<tg::grant::Permission>,
	) -> tg::Result<bool> {
		let mut changed = false;
		let current = entries
			.iter()
			.filter(|entry| managed.contains(&entry.permission))
			.filter_map(|entry| {
				entry
					.materialized
					.map(|expires_at| (entry.principal.clone(), entry.permission, expires_at))
			})
			.collect::<BTreeSet<_>>();
		for (principal, permission, expires_at) in current.difference(expected) {
			if Self::delete_grant_index_entry(
				db,
				subspace,
				transaction,
				&crate::lmdb::grant::GrantIndexEntry {
					creator: None,
					expires_at: *expires_at,
					permission: *permission,
					principal,
					resource,
				},
				crate::lmdb::grant::GrantSource::Materialized,
			)? {
				changed = true;
			}
		}
		for (principal, permission, expires_at) in expected.difference(&current) {
			if Self::put_grant_index_entry(
				db,
				subspace,
				transaction,
				&crate::lmdb::grant::GrantIndexEntry {
					creator: None,
					expires_at: *expires_at,
					permission: *permission,
					principal,
					resource,
				},
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
		let object_subtree =
			tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree);
		let process_permission = |permission| tg::grant::Permission::Process(permission);
		let node = process_permission(tg::grant::permission::process::Permission::Node);
		let node_command =
			process_permission(tg::grant::permission::process::Permission::NodeCommand);
		let node_error = process_permission(tg::grant::permission::process::Permission::NodeError);
		let node_log = process_permission(tg::grant::permission::process::Permission::NodeLog);
		let node_output =
			process_permission(tg::grant::permission::process::Permission::NodeOutput);
		let subtree = process_permission(tg::grant::permission::process::Permission::Subtree);
		let subtree_command =
			process_permission(tg::grant::permission::process::Permission::SubtreeCommand);
		let subtree_error =
			process_permission(tg::grant::permission::process::Permission::SubtreeError);
		let subtree_log =
			process_permission(tg::grant::permission::process::Permission::SubtreeLog);
		let subtree_output =
			process_permission(tg::grant::permission::process::Permission::SubtreeOutput);

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
							Self::grant_entries_cover_expires_at(entries, &entry.principal, target)
								.map(|cover| Self::min_expires_at(output, cover.expires_at))
						});
				if let Some(expires_at) = expires_at {
					if Self::has_non_materialized_cover(
						input.entries,
						&entry.principal,
						target,
						expires_at,
					) {
						continue;
					}
					expected.insert((entry.principal.clone(), target, expires_at));
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
		expected: &mut BTreeSet<(tg::grant::Principal, tg::grant::Permission, Option<i64>)>,
		target_entries: &[crate::lmdb::grant::GrantEntry],
		sources: impl IntoIterator<Item = &'a crate::lmdb::grant::GrantEntry>,
		required: &[&[crate::lmdb::grant::GrantEntry]],
		source_permission: tg::grant::Permission,
		target_permission: tg::grant::Permission,
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
					Self::grant_entries_cover_expires_at(
						entries,
						&entry.principal,
						source_permission,
					)
					.map(|cover| Self::min_expires_at(output, cover.expires_at))
				});
			if let Some(expires_at) = expires_at {
				if Self::has_non_materialized_cover(
					target_entries,
					&entry.principal,
					target_permission,
					expires_at,
				) {
					continue;
				}
				expected.insert((entry.principal.clone(), target_permission, expires_at));
			}
		}
	}

	fn has_non_materialized_cover(
		entries: &[crate::lmdb::grant::GrantEntry],
		principal: &tg::grant::Principal,
		permission: tg::grant::Permission,
		expires_at: Option<i64>,
	) -> bool {
		entries.iter().any(|entry| {
			entry.principal == *principal
				&& entry.permission == permission
				&& entry.has_non_materialized_cover(expires_at)
		})
	}

	fn grant_entries_cover_expires_at(
		entries: &[crate::lmdb::grant::GrantEntry],
		principal: &tg::grant::Principal,
		permission: tg::grant::Permission,
	) -> Option<GrantCover> {
		entries
			.iter()
			.filter(|entry| entry.principal == *principal && entry.permission == permission)
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

	fn update_process_grants_for_principal(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::process::Id,
		principal: &tg::grant::Principal,
	) -> tg::Result<bool> {
		let key = crate::lmdb::Key::Process(crate::lmdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?
			.ok_or_else(|| tg::error!(%id, "process not found"))?;
		let process = crate::process::Process::deserialize(bytes)?;
		let resource = tg::Id::from(id.clone());
		let entries = Self::get_resource_grant_entries_for_principal_with_transaction(
			db,
			subspace,
			transaction,
			&resource,
			principal,
		)?;
		let children = Self::get_process_children_with_transaction(db, subspace, transaction, id)?;
		let child_entries = children
			.iter()
			.map(|child| {
				let resource = tg::Id::from(child.clone());
				Self::get_resource_grant_entries_for_principal_with_transaction(
					db,
					subspace,
					transaction,
					&resource,
					principal,
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
			let entries = Self::get_resource_grant_entries_for_principal_with_transaction(
				db,
				subspace,
				transaction,
				&resource,
				principal,
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
		Self::update_process_grants(
			db,
			subspace,
			transaction,
			&ProcessGrantInputs {
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
			},
		)
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
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?
			.ok_or_else(|| tg::error!(%id, "process not found"))?;
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
			&& !process.stored.node_command
			&& object.stored.subtree
		{
			process.stored.node_command = true;
			changed = true;
		}

		if process.set.error && !process.stored.node_error {
			let value = error_objects
				.iter()
				.all(|option| option.as_ref().is_some_and(|object| object.stored.subtree));
			if value {
				process.stored.node_error = true;
				changed = true;
			}
		}

		if process.set.log {
			if let Some(Some(object)) = &log_object {
				if !process.stored.node_log && object.stored.subtree {
					process.stored.node_log = true;
					changed = true;
				}
			} else if log_object.is_none() && !process.stored.node_log {
				process.stored.node_log = true;
				changed = true;
			}
		}

		if process.set.output && !process.stored.node_output {
			let value = output_objects
				.iter()
				.all(|option| option.as_ref().is_some_and(|object| object.stored.subtree));
			if value {
				process.stored.node_output = true;
				changed = true;
			}
		}

		if process.set.children && !process.stored.subtree {
			let value = children
				.iter()
				.all(|child| child.as_ref().is_some_and(|child| child.stored.subtree));
			if value {
				process.stored.subtree = true;
				changed = true;
			}
		}

		if process.set.children {
			if !process.stored.subtree_command && process.stored.node_command {
				let value = children.iter().all(|child| {
					child
						.as_ref()
						.is_some_and(|child| child.stored.subtree_command)
				});
				if value {
					process.stored.subtree_command = true;
					changed = true;
				}
			}

			if !process.stored.subtree_error && process.stored.node_error {
				let value = children.iter().all(|child| {
					child
						.as_ref()
						.is_some_and(|child| child.stored.subtree_error)
				});
				if value {
					process.stored.subtree_error = true;
					changed = true;
				}
			}

			if !process.stored.subtree_log && process.stored.node_log {
				let value = children
					.iter()
					.all(|child| child.as_ref().is_some_and(|child| child.stored.subtree_log));
				if value {
					process.stored.subtree_log = true;
					changed = true;
				}
			}

			if !process.stored.subtree_output && process.stored.node_output {
				let value = children.iter().all(|child| {
					child
						.as_ref()
						.is_some_and(|child| child.stored.subtree_output)
				});
				if value {
					process.stored.subtree_output = true;
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
		Self::enqueue_update_with_kind(db, subspace, transaction, id, Kind::Item, source, version)
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
			let existing = Update::deserialize(existing)?;
			if existing.source == Source::Propagate && source == Source::Put {
				let value = Update::new(source).serialize()?;
				db.put(transaction, &key, &value)
					.map_err(|error| tg::error!(!error, "failed to put update key"))?;
			}
			return Ok(());
		}

		let value = Update::new(source).serialize()?;
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
