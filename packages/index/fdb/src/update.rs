mod key;
#[cfg(test)]
mod tests;

pub(super) use key::{Key, Kind, UsageKind};

use {
	super::{Index, Kind as KeyKind, Request, Response},
	foundationdb as fdb,
	foundationdb_tuple::{self as fdbt, Subspace},
	futures::{TryStreamExt as _, future},
	num_traits::ToPrimitive as _,
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::ControlFlow,
	},
	tangram_client::prelude::*,
};

const STORAGE_RELATION_BATCH_SIZE: usize = 8;

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) struct PermissionUpdate {
	#[tangram_serialize(id = 0)]
	pub source: Source,
}

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) struct StorageAndMetadataUpdate {
	#[tangram_serialize(id = 0)]
	pub source: Source,
}

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) struct UsageUpdate {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub cursor: Option<UsageCursor>,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub version: Option<[u8; 12]>,
}

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) enum UsageCursor {
	#[tangram_serialize(id = 0)]
	Object(tg::object::Id),

	#[tangram_serialize(id = 3)]
	ObjectAccount(tangram_index::usage::Account),

	#[tangram_serialize(id = 1)]
	ProcessChild(i64),

	#[tangram_serialize(id = 2)]
	ProcessObject(Option<ProcessObjectCursor>),

	#[tangram_serialize(id = 4)]
	ProcessAccount(tangram_index::usage::Account),
}

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) struct ProcessObjectCursor {
	#[tangram_serialize(id = 0)]
	pub kind: tangram_index::process::object::Kind,

	#[tangram_serialize(id = 1)]
	pub object: tg::object::Id,
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

enum UsageRelationship {
	Object(tg::object::Id, Option<tangram_index::process::object::Kind>),
	Process(tg::process::Id),
}

#[derive(Clone, Copy)]
struct PermissionCover {
	expires_at: Option<i64>,
}

impl PermissionUpdate {
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

impl StorageAndMetadataUpdate {
	pub fn new(source: Source) -> Self {
		Self { source }
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
	pub fn new() -> Self {
		Self {
			cursor: None,
			version: None,
		}
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
	pub(super) fn enqueue_update(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		partition_total: u64,
	) {
		Self::enqueue_update_with_kind(
			txn,
			subspace,
			id,
			&Kind::StorageAndMetadata,
			Source::Put,
			partition_total,
		);
	}

	pub(super) fn enqueue_update_with_kind(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		kind: &Kind,
		source: Source,
		partition_total: u64,
	) {
		Self::enqueue_update_with_kind_at_version(
			txn,
			subspace,
			id,
			kind,
			source,
			partition_total,
			None,
		);
	}

	pub(super) fn enqueue_update_with_kind_at_version(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		kind: &Kind,
		source: Source,
		partition_total: u64,
		version: Option<&fdbt::Versionstamp>,
	) {
		let value = serialize_update(kind, source).unwrap();
		Self::enqueue_update_value(txn, subspace, id, kind, &value, partition_total, version);
	}

	fn enqueue_update_value(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		kind: &Kind,
		value: &[u8],
		partition_total: u64,
		version: Option<&fdbt::Versionstamp>,
	) {
		let key = Self::pack(
			subspace,
			&crate::Key::Update(crate::update::Key::Update {
				id: id.clone(),
				kind: kind.clone(),
			}),
		);
		txn.set(&key, value);

		let partition = rand::random_range(0..partition_total);
		if let Some(version) = version {
			let key = Self::pack(
				subspace,
				&crate::Key::Update(crate::update::Key::UpdateVersion {
					id: id.clone(),
					kind: kind.clone(),
					partition,
					version: version.clone(),
				}),
			);
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			txn.set(&key, &[]);
		} else {
			let version = fdbt::Versionstamp::incomplete(0);
			let key = Self::pack_with_versionstamp(
				subspace,
				&crate::Key::Update(crate::update::Key::UpdateVersion {
					id: id.clone(),
					kind: kind.clone(),
					partition,
					version,
				}),
			);
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			txn.atomic_op(&key, &[], fdb::options::MutationType::SetVersionstampedKey);
		}
	}

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

	pub(crate) async fn try_get_oldest_update_transaction_id_with_transaction(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		kind: tangram_index::update::Kind,
		partition_total: u64,
	) -> tg::Result<ControlFlow<Option<u64>, fdb::FdbError>> {
		let key_kind = update_version_key_kind(kind).to_i32().unwrap();
		let futures = (0..partition_total).map(|partition| {
			let begin = Self::pack(subspace, &(key_kind, partition));
			let end = Self::pack(subspace, &(key_kind, partition.saturating_add(1)));
			let range = fdb::RangeOption {
				begin: fdb::KeySelector::first_greater_or_equal(begin),
				end: fdb::KeySelector::first_greater_or_equal(end),
				limit: Some(1),
				mode: fdb::options::StreamingMode::WantAll,
				..Default::default()
			};
			async move {
				// A snapshot is sufficient because processing an update atomically preserves its version in downstream work.
				let result = txn.get_range(&range, 1, true).await;
				let entries = crate::retry!(result);
				let Some(entry) = entries.first() else {
					return Ok(ControlFlow::Break(None));
				};
				let key = Self::unpack(subspace, entry.key())?;
				let crate::Key::Update(crate::update::Key::UpdateVersion { version, .. }) = key
				else {
					return Err(tg::error!("unexpected update key"));
				};
				let transaction_id =
					u64::from_be_bytes(version.as_bytes()[..8].try_into().unwrap());
				Ok(ControlFlow::Break(Some(transaction_id)))
			}
		});
		let transaction_id = {
			let result = future::try_join_all(futures).await;
			let results = result?;
			let mut values = Vec::with_capacity(results.len());
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		}
		.into_iter()
		.flatten()
		.min();

		Ok(ControlFlow::Break(transaction_id))
	}

	pub async fn update_batch(
		&self,
		kind: tangram_index::update::Kind,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<tangram_index::update::Output> {
		let request = Request::Update(crate::Update {
			batch_size,
			kind,
			partition_end,
			partition_start,
		});
		let response = self.send_write_request(request).await?;
		let Response::UpdateOutput(output) = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(output)
	}

	#[allow(clippy::too_many_arguments)]
	pub(super) async fn update_with_transaction(
		txn: &crate::Transaction,
		subspace: &Subspace,
		batch_size: usize,
		kind: tangram_index::update::Kind,
		partition_start: u64,
		partition_end: u64,
		max_process_depth: Option<u64>,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<tangram_index::update::Output, fdb::FdbError>> {
		let cleaning_partition_total = partition_totals.cleaning;
		let partition_total = partition_totals.update(kind);
		let mut entries = Vec::new();

		let key_kind = update_version_key_kind(kind).to_i32().unwrap();
		for partition in partition_start..partition_end {
			let remaining = batch_size.saturating_sub(entries.len());
			if remaining == 0 {
				break;
			}
			let begin = Self::pack(subspace, &(key_kind, partition));
			let end = Self::pack(subspace, &(key_kind, partition + 1));
			let range = fdb::RangeOption {
				begin: fdb::KeySelector::first_greater_or_equal(begin),
				end: fdb::KeySelector::first_greater_or_equal(end),
				limit: Some(remaining),
				mode: fdb::options::StreamingMode::WantAll,
				..Default::default()
			};
			let result = txn.get_range(&range, 1, false).await;
			let partition_entries = crate::retry!(result);
			for entry in partition_entries {
				let key = Self::unpack(subspace, entry.key())?;
				let crate::Key::Update(crate::update::Key::UpdateVersion {
					partition,
					version,
					id,
					kind,
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				entries.push((partition, version, id, kind));
			}
		}

		let mut output = tangram_index::update::Output::default();
		for (partition, queued_version, id, kind) in entries {
			let mut version = queued_version.clone();
			let key = Self::pack(
				subspace,
				&crate::Key::Update(crate::update::Key::Update {
					id: id.clone(),
					kind: kind.clone(),
				}),
			);
			let result = txn.get(&key, false).await;
			let value = crate::retry!(result);

			// A consumed payload does not imply that every queued version has propagated.
			let value = if let Some(value) = value {
				value.to_vec()
			} else {
				let key = match &kind {
					Kind::Permission(_) | Kind::StorageAndMetadata => {
						Some(Key::PropagatedVersion {
							id: id.clone(),
							kind: kind.clone(),
						})
					},
					Kind::Usage(UsageKind::Clean(_) | UsageKind::CleanAll) => None,
					Kind::Usage(UsageKind::Propagate { account, .. }) => {
						Some(Key::UsageUpdatePropagatedVersion {
							account: account.clone(),
							id: id.clone(),
						})
					},
					Kind::Usage(UsageKind::Put { account, .. }) => {
						Some(Key::UsageUpdatePutVersion {
							account: account.clone(),
							id: id.clone(),
						})
					},
				};
				let propagated_version = if let Some(key) = key {
					crate::propagate!(Self::try_get_propagation_version(txn, subspace, &key).await)
				} else {
					None
				};
				if propagated_version.is_none_or(|propagated_version| version >= propagated_version)
				{
					Self::clear_update_version(
						txn,
						subspace,
						&id,
						&kind,
						partition,
						&queued_version,
					);
					output.count += 1;
					continue;
				}
				serialize_update(&kind, Source::Propagate)?
			};

			let (cursor, source) = match &kind {
				Kind::Permission(_) | Kind::StorageAndMetadata => {
					(None, Some(deserialize_source_update(&kind, &value)?))
				},
				Kind::Usage(_) => {
					let update = UsageUpdate::deserialize(&value)?;
					let previous = update.version.map(fdbt::Versionstamp::from);
					// Revisit earlier pages when an older obligation joins the traversal.
					let cursor = if previous
						.as_ref()
						.is_some_and(|previous| version < *previous)
					{
						None
					} else {
						if let Some(previous) = previous {
							version = version.min(previous);
						}
						update.cursor
					};
					(cursor, None)
				},
			};
			let mut next_cursor = None;

			let changed = match &kind {
				Kind::Permission(subject) => match &id {
					tg::Either::Left(id) => {
						crate::propagate!(
							Self::update_object_permissions_for_subject(
								txn,
								subspace,
								id,
								subject,
								cleaning_partition_total,
							)
							.await
						)
					},
					tg::Either::Right(id) => {
						crate::propagate!(
							Self::update_process_permissions_for_subject(
								txn,
								subspace,
								id,
								subject,
								cleaning_partition_total,
							)
							.await
						)
					},
				},
				Kind::StorageAndMetadata => match &id {
					tg::Either::Left(id) => {
						crate::propagate!(Self::update_object(txn, subspace, id).await)
					},
					tg::Either::Right(id) => {
						let process_output = crate::propagate!(
							Self::update_process(txn, subspace, id, max_process_depth).await
						);
						if process_output.depth_exceeded {
							output.processes_with_depth_exceeded.push(id.clone());
						}
						process_output.changed
					},
				},
				Kind::Usage(UsageKind::Clean(account)) => {
					next_cursor = crate::propagate!(
						Self::propagate_storage_clean(
							txn,
							subspace,
							&id,
							account,
							cursor.as_ref(),
							cleaning_partition_total,
						)
						.await
					);
					false
				},
				Kind::Usage(UsageKind::CleanAll) => {
					next_cursor = crate::propagate!(
						Self::propagate_storage_accounts_clean(
							txn,
							subspace,
							&id,
							cursor.as_ref(),
							cleaning_partition_total,
						)
						.await
					);
					false
				},
				Kind::Usage(UsageKind::Propagate {
					account,
					touched_at,
				}) => {
					next_cursor = crate::propagate!(
						Self::propagate_storage_relationships(
							txn,
							subspace,
							&id,
							account,
							cursor.as_ref(),
							partition_total,
							*touched_at,
							&version,
						)
						.await
					);
					false
				},
				Kind::Usage(UsageKind::Put {
					account,
					permissions,
					touched_at,
				}) => match &id {
					tg::Either::Left(object) => {
						crate::propagate!(
							Self::put_account_object(
								txn,
								subspace,
								&tangram_index::usage::storage::put::ObjectArg {
									account: account.clone(),
									object: object.clone(),
									touched_at: *touched_at,
								},
								partition_totals,
								Some(*permissions),
								Some(&version),
							)
							.await
						)
					},
					tg::Either::Right(process) => {
						crate::propagate!(
							Self::put_account_process(
								txn,
								subspace,
								&tangram_index::usage::storage::put::ProcessArg {
									account: account.clone(),
									process: process.clone(),
									touched_at: *touched_at,
								},
								partition_totals,
								Some(*permissions),
								Some(&version),
							)
							.await
						)
					},
				},
			};

			if let Some(source) = source {
				let propagated_version = crate::propagate!(
					Self::try_get_update_propagated_version(txn, subspace, &id, &kind).await
				);
				let propagate = source == Source::Put
					|| changed || propagated_version
					.as_ref()
					.is_some_and(|propagated_version| version < *propagated_version);
				if propagate {
					crate::propagate!(
						Self::enqueue_parents(txn, subspace, &id, &kind, &version, partition_total)
							.await
					);
					// Remember the version used to enqueue the parents, including an older version for an unchanged item.
					let key = crate::Key::Update(Key::PropagatedVersion {
						id: id.clone(),
						kind: kind.clone(),
					});
					txn.set(&Self::pack(subspace, &key), version.as_bytes());
					let id_bytes = match &id {
						tg::Either::Left(id) => id.to_bytes(),
						tg::Either::Right(id) => id.to_bytes(),
					};
					let partition =
						Self::partition_for_id(id_bytes.as_ref(), cleaning_partition_total);
					// Keep one cleanup entry for the retained propagated version.
					if let Some(previous) =
						propagated_version.filter(|previous| *previous != version)
					{
						let key = crate::Key::Update(Key::Clean {
							id: id.clone(),
							kind: kind.clone(),
							partition,
							version: previous,
						});
						txn.clear(&Self::pack(subspace, &key));
					}
					let key = crate::Key::Update(Key::Clean {
						id: id.clone(),
						kind: kind.clone(),
						partition,
						version: version.clone(),
					});
					txn.set(&Self::pack(subspace, &key), &[]);
				}
			}

			let continued = if let Some(cursor) = next_cursor {
				let update = UsageUpdate {
					cursor: Some(cursor),
					version: Some(*version.as_bytes()),
				};
				let key = Self::pack(
					subspace,
					&crate::Key::Update(crate::update::Key::Update {
						id: id.clone(),
						kind: kind.clone(),
					}),
				);
				txn.set(&key, &update.serialize()?);
				true
			} else {
				crate::propagate!(
					Self::schedule_update_item_clean(txn, subspace, &id, cleaning_partition_total,)
						.await
				);
				let key = Self::pack(
					subspace,
					&crate::Key::Update(crate::update::Key::Update {
						id: id.clone(),
						kind: kind.clone(),
					}),
				);
				txn.clear(&key);
				false
			};
			if !continued {
				Self::clear_update_version(txn, subspace, &id, &kind, partition, &queued_version);
			}

			output.count += 1;
		}

		Ok(ControlFlow::Break(output))
	}

	async fn try_get_update_propagated_version(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		kind: &Kind,
	) -> tg::Result<ControlFlow<Option<fdbt::Versionstamp>, fdb::FdbError>> {
		let key = Key::PropagatedVersion {
			id: id.clone(),
			kind: kind.clone(),
		};
		Self::try_get_propagation_version(txn, subspace, &key).await
	}

	pub(super) async fn try_get_propagation_version(
		txn: &crate::Transaction,
		subspace: &Subspace,
		key: &Key,
	) -> tg::Result<ControlFlow<Option<fdbt::Versionstamp>, fdb::FdbError>> {
		let key = Self::pack(subspace, &crate::Key::Update(key.clone()));
		let result = txn.get(&key, false).await;
		let value = crate::retry!(result);
		let version = value
			.map(|value| {
				let bytes: [u8; 12] = value.as_ref().try_into().map_err(|error| {
					tg::error!(
						!error,
						"failed to deserialize the propagated update version"
					)
				})?;
				Ok::<_, tg::Error>(fdbt::Versionstamp::from(bytes))
			})
			.transpose()?;

		Ok(ControlFlow::Break(version))
	}

	pub(super) fn clear_update_propagated_versions(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &[u8],
	) {
		for kind in [
			KeyKind::PermissionUpdatePropagatedVersion,
			KeyKind::StorageAndMetadataUpdatePropagatedVersion,
			KeyKind::UsageUpdatePropagatedVersion,
			KeyKind::UsageUpdatePutVersion,
		] {
			let prefix = Self::pack(subspace, &(kind.to_i32().unwrap(), id));
			let (_, end) = Subspace::from_bytes(prefix.clone()).range();
			txn.clear_range(&prefix, &end);
		}
	}

	pub(super) async fn lower_usage_update_put_version(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		account: &tangram_index::usage::Account,
		version: &fdbt::Versionstamp,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let key = Key::UsageUpdatePutVersion {
			account: account.clone(),
			id: id.clone(),
		};
		let previous =
			crate::propagate!(Self::try_get_propagation_version(txn, subspace, &key).await);
		if previous.is_some_and(|previous| *version >= previous) {
			return Ok(ControlFlow::Break(false));
		}
		txn.set(
			&Self::pack(subspace, &crate::Key::Update(key)),
			version.as_bytes(),
		);
		Ok(ControlFlow::Break(true))
	}

	pub(super) fn clear_usage_update_versions(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		account: &tangram_index::usage::Account,
	) {
		for key in [
			Key::UsageUpdatePropagatedVersion {
				account: account.clone(),
				id: id.clone(),
			},
			Key::UsageUpdatePutVersion {
				account: account.clone(),
				id: id.clone(),
			},
		] {
			txn.clear(&Self::pack(subspace, &crate::Key::Update(key)));
		}
	}

	#[allow(clippy::too_many_arguments)]
	async fn propagate_storage_relationships(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		account: &tangram_index::usage::Account,
		cursor: Option<&UsageCursor>,
		usage_update_partition_total: u64,
		touched_at: i64,
		version: &fdbt::Versionstamp,
	) -> tg::Result<ControlFlow<Option<UsageCursor>, fdb::FdbError>> {
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
		let result = txn
			.get(&Self::pack(subspace, &crate::Key::Usage(key)), false)
			.await;
		let Some(value) = crate::retry!(result) else {
			return Ok(ControlFlow::Break(None));
		};
		let entry = tangram_index::usage::storage::Entry::deserialize(&value)?;

		// Resolve the put version once a versionstamped insertion starts propagating.
		if cursor.is_none() {
			crate::propagate!(
				Self::lower_usage_update_put_version(txn, subspace, id, account, version).await
			);
		}
		let (relationships, cursor) = crate::propagate!(
			Self::get_storage_relationships_page(txn, subspace, id, cursor).await
		);
		for relationship in relationships {
			let (id, permissions) = match relationship {
				UsageRelationship::Object(object, kind) => {
					let permissions = match kind {
						Some(kind) => tangram_index::usage::storage::object_permissions(
							entry.permissions,
							kind,
						),
						None => tangram_index::usage::storage::child_permissions(entry.permissions),
					};
					(tg::Either::Left(object), permissions)
				},
				UsageRelationship::Process(process) => (
					tg::Either::Right(process),
					tangram_index::usage::storage::child_permissions(entry.permissions),
				),
			};
			if permissions.is_empty() {
				continue;
			}
			let kind = Kind::Usage(UsageKind::Put {
				account: account.clone(),
				permissions,
				touched_at,
			});
			Self::enqueue_update_with_kind_at_version(
				txn,
				subspace,
				&id,
				&kind,
				Source::Put,
				usage_update_partition_total,
				Some(version),
			);
		}

		if cursor.is_none() {
			let key = Key::UsageUpdatePropagatedVersion {
				account: account.clone(),
				id: id.clone(),
			};
			txn.set(
				&Self::pack(subspace, &crate::Key::Update(key)),
				version.as_bytes(),
			);
		}

		Ok(ControlFlow::Break(cursor))
	}

	async fn propagate_storage_clean(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		account: &tangram_index::usage::Account,
		cursor: Option<&UsageCursor>,
		cleaning_partition_total: u64,
	) -> tg::Result<ControlFlow<Option<UsageCursor>, fdb::FdbError>> {
		let (relationships, cursor) = crate::propagate!(
			Self::get_storage_relationships_page(txn, subspace, id, cursor).await
		);
		{
			let result =
				future::try_join_all(relationships.iter().map(|relationship| async move {
					match relationship {
						UsageRelationship::Object(object, _) => {
							Self::schedule_account_object_for_cleaning(
								txn,
								subspace,
								account,
								object,
								cleaning_partition_total,
							)
							.await
						},
						UsageRelationship::Process(process) => {
							Self::schedule_account_process_for_cleaning(
								txn,
								subspace,
								account,
								process,
								cleaning_partition_total,
							)
							.await
						},
					}
				}))
				.await;
			let results = result?;
			for result in results {
				match result {
					ControlFlow::Break(()) => {},
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				}
			}
		};

		Ok(ControlFlow::Break(cursor))
	}

	async fn propagate_storage_accounts_clean(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		cursor: Option<&UsageCursor>,
		cleaning_partition_total: u64,
	) -> tg::Result<ControlFlow<Option<UsageCursor>, fdb::FdbError>> {
		let (accounts, cursor) =
			crate::propagate!(Self::get_storage_accounts_page(txn, subspace, id, cursor).await);
		{
			let result = future::try_join_all(accounts.iter().map(|account| async move {
				match id {
					tg::Either::Left(object) => {
						Self::schedule_account_object_for_cleaning(
							txn,
							subspace,
							account,
							object,
							cleaning_partition_total,
						)
						.await
					},
					tg::Either::Right(process) => {
						Self::schedule_account_process_for_cleaning(
							txn,
							subspace,
							account,
							process,
							cleaning_partition_total,
						)
						.await
					},
				}
			}))
			.await;
			let results = result?;
			for result in results {
				match result {
					ControlFlow::Break(()) => {},
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				}
			}
		};

		Ok(ControlFlow::Break(cursor))
	}

	async fn get_storage_relationships_page(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		cursor: Option<&UsageCursor>,
	) -> tg::Result<ControlFlow<(Vec<UsageRelationship>, Option<UsageCursor>), fdb::FdbError>> {
		match id {
			tg::Either::Left(object) => {
				let after = match cursor {
					None => None,
					Some(UsageCursor::Object(object)) => Some(object),
					Some(
						UsageCursor::ObjectAccount(_)
						| UsageCursor::ProcessChild(_)
						| UsageCursor::ProcessObject(_)
						| UsageCursor::ProcessAccount(_),
					) => {
						return Err(tg::error!(%object, "an object update has an invalid cursor"));
					},
				};
				Self::get_storage_object_relationships_page(txn, subspace, object, after).await
			},
			tg::Either::Right(process) => {
				if matches!(
					cursor,
					Some(
						UsageCursor::Object(_)
							| UsageCursor::ObjectAccount(_)
							| UsageCursor::ProcessAccount(_)
					)
				) {
					return Err(tg::error!(%process, "a process update has an invalid cursor"));
				}
				Self::get_storage_process_relationships_page(txn, subspace, process, cursor).await
			},
		}
	}

	async fn get_storage_accounts_page(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		cursor: Option<&UsageCursor>,
	) -> tg::Result<
		ControlFlow<(Vec<tangram_index::usage::Account>, Option<UsageCursor>), fdb::FdbError>,
	> {
		let (kind, item, after) = match (id, cursor) {
			(tg::Either::Left(object), None) => (KeyKind::ObjectAccount, object.to_bytes(), None),
			(tg::Either::Left(object), Some(UsageCursor::ObjectAccount(account))) => (
				KeyKind::ObjectAccount,
				object.to_bytes(),
				Some(crate::Key::Usage(crate::usage::Key::ObjectAccount {
					account: account.clone(),
					object: object.clone(),
				})),
			),
			(tg::Either::Right(process), None) => {
				(KeyKind::ProcessAccount, process.to_bytes(), None)
			},
			(tg::Either::Right(process), Some(UsageCursor::ProcessAccount(account))) => (
				KeyKind::ProcessAccount,
				process.to_bytes(),
				Some(crate::Key::Usage(crate::usage::Key::ProcessAccount {
					account: account.clone(),
					process: process.clone(),
				})),
			),
			(tg::Either::Left(object), Some(_)) => {
				return Err(tg::error!(%object, "an object account update has an invalid cursor"));
			},
			(tg::Either::Right(process), Some(_)) => {
				return Err(tg::error!(%process, "a process account update has an invalid cursor"));
			},
		};
		let prefix = Self::pack(subspace, &(kind.to_i32().unwrap(), item.as_ref()));
		let mut range = fdb::RangeOption {
			limit: Some(STORAGE_RELATION_BATCH_SIZE + 1),
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		if let Some(after) = after {
			let mut begin = Self::pack(subspace, &after);
			begin.push(0);
			range.begin = fdb::KeySelector::first_greater_or_equal(begin);
		}
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::retry!(result);
		let mut accounts = entries
			.iter()
			.map(|entry| match Self::unpack(subspace, entry.key())? {
				crate::Key::Usage(
					crate::usage::Key::ObjectAccount { account, .. }
					| crate::usage::Key::ProcessAccount { account, .. },
				) => Ok(account),
				_ => Err(tg::error!("unexpected key type")),
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let cursor = if accounts.len() > STORAGE_RELATION_BATCH_SIZE {
			accounts.truncate(STORAGE_RELATION_BATCH_SIZE);
			let account = accounts.last().unwrap().clone();
			Some(match id {
				tg::Either::Left(_) => UsageCursor::ObjectAccount(account),
				tg::Either::Right(_) => UsageCursor::ProcessAccount(account),
			})
		} else {
			None
		};

		Ok(ControlFlow::Break((accounts, cursor)))
	}

	async fn get_storage_object_relationships_page(
		txn: &crate::Transaction,
		subspace: &Subspace,
		object: &tg::object::Id,
		after: Option<&tg::object::Id>,
	) -> tg::Result<ControlFlow<(Vec<UsageRelationship>, Option<UsageCursor>), fdb::FdbError>> {
		let object_bytes = object.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				KeyKind::ObjectChild.to_i32().unwrap(),
				object_bytes.as_ref(),
			),
		);
		let mut range = fdb::RangeOption {
			limit: Some(STORAGE_RELATION_BATCH_SIZE + 1),
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		if let Some(after) = after {
			let key = crate::Key::Object(crate::object::Key::ObjectChild {
				child: after.clone(),
				object: object.clone(),
			});
			let mut begin = Self::pack(subspace, &key);
			begin.push(0);
			range.begin = fdb::KeySelector::first_greater_or_equal(begin);
		}
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::retry!(result);
		let mut children = entries
			.iter()
			.map(|entry| {
				let crate::Key::Object(crate::object::Key::ObjectChild { child, .. }) =
					Self::unpack(subspace, entry.key())?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let cursor = if children.len() > STORAGE_RELATION_BATCH_SIZE {
			children.truncate(STORAGE_RELATION_BATCH_SIZE);
			Some(UsageCursor::Object(children.last().unwrap().clone()))
		} else {
			None
		};
		let relationships = children
			.into_iter()
			.map(|object| UsageRelationship::Object(object, None))
			.collect();

		Ok(ControlFlow::Break((relationships, cursor)))
	}

	async fn get_storage_process_relationships_page(
		txn: &crate::Transaction,
		subspace: &Subspace,
		process: &tg::process::Id,
		cursor: Option<&UsageCursor>,
	) -> tg::Result<ControlFlow<(Vec<UsageRelationship>, Option<UsageCursor>), fdb::FdbError>> {
		let mut relationships = Vec::new();
		let process_object_cursor = match cursor {
			None | Some(UsageCursor::ProcessChild(_)) => {
				let after = match cursor {
					Some(UsageCursor::ProcessChild(position)) => Some(*position),
					_ => None,
				};
				let (children, child_cursor, more) = crate::propagate!(
					Self::get_storage_process_children_page(txn, subspace, process, after).await
				);
				relationships.extend(children.into_iter().map(UsageRelationship::Process));
				if more {
					return Ok(ControlFlow::Break((
						relationships,
						Some(UsageCursor::ProcessChild(child_cursor.unwrap())),
					)));
				}
				if relationships.len() == STORAGE_RELATION_BATCH_SIZE {
					return Ok(ControlFlow::Break((
						relationships,
						Some(UsageCursor::ProcessObject(None)),
					)));
				}
				None
			},
			Some(UsageCursor::ProcessObject(cursor)) => cursor.as_ref(),
			Some(
				UsageCursor::Object(_)
				| UsageCursor::ObjectAccount(_)
				| UsageCursor::ProcessAccount(_),
			) => unreachable!(),
		};
		let limit = STORAGE_RELATION_BATCH_SIZE - relationships.len();
		let (objects, more) = crate::propagate!(
			Self::get_storage_process_objects_page(
				txn,
				subspace,
				process,
				process_object_cursor,
				limit,
			)
			.await
		);
		let cursor = objects.last().map(|(object, kind)| ProcessObjectCursor {
			kind: *kind,
			object: object.clone(),
		});
		relationships.extend(
			objects
				.into_iter()
				.map(|(object, kind)| UsageRelationship::Object(object, Some(kind))),
		);
		let cursor = more.then_some(UsageCursor::ProcessObject(cursor));

		Ok(ControlFlow::Break((relationships, cursor)))
	}

	async fn get_storage_process_children_page(
		txn: &crate::Transaction,
		subspace: &Subspace,
		process: &tg::process::Id,
		after: Option<i64>,
	) -> tg::Result<ControlFlow<(Vec<tg::process::Id>, Option<i64>, bool), fdb::FdbError>> {
		let process_bytes = process.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				KeyKind::ProcessChild.to_i32().unwrap(),
				process_bytes.as_ref(),
			),
		);
		let mut range = fdb::RangeOption {
			limit: Some(STORAGE_RELATION_BATCH_SIZE + 1),
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		if let Some(after) = after {
			let position = after
				.checked_add(1)
				.ok_or_else(|| tg::error!("the process has too many children"))?;
			let begin = Self::pack(
				subspace,
				&(
					KeyKind::ProcessChild.to_i32().unwrap(),
					process_bytes.as_ref(),
					position,
				),
			);
			range.begin = fdb::KeySelector::first_greater_or_equal(begin);
		}
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		let mut children = entries
			.iter()
			.map(|entry| {
				let crate::Key::Process(crate::process::Key::ProcessChild {
					child, position, ..
				}) = Self::unpack(subspace, entry.key())?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((child, position))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let more = children.len() > STORAGE_RELATION_BATCH_SIZE;
		children.truncate(STORAGE_RELATION_BATCH_SIZE);
		let cursor = children.last().map(|(_, position)| *position);
		let children = children.into_iter().map(|(child, _)| child).collect();

		Ok(ControlFlow::Break((children, cursor, more)))
	}

	async fn get_storage_process_objects_page(
		txn: &crate::Transaction,
		subspace: &Subspace,
		process: &tg::process::Id,
		after: Option<&ProcessObjectCursor>,
		limit: usize,
	) -> tg::Result<
		ControlFlow<
			(
				Vec<(tg::object::Id, tangram_index::process::object::Kind)>,
				bool,
			),
			fdb::FdbError,
		>,
	> {
		let process_bytes = process.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(
				KeyKind::ProcessObject.to_i32().unwrap(),
				process_bytes.as_ref(),
			),
		);
		let mut range = fdb::RangeOption {
			limit: Some(limit + 1),
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		if let Some(after) = after {
			let key = crate::Key::Process(crate::process::Key::ProcessObject {
				kind: after.kind,
				object: after.object.clone(),
				process: process.clone(),
			});
			let mut begin = Self::pack(subspace, &key);
			begin.push(0);
			range.begin = fdb::KeySelector::first_greater_or_equal(begin);
		}
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::retry!(result);
		let mut objects = entries
			.iter()
			.map(|entry| {
				let crate::Key::Process(crate::process::Key::ProcessObject {
					kind, object, ..
				}) = Self::unpack(subspace, entry.key())?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((object, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let more = objects.len() > limit;
		objects.truncate(limit);

		Ok(ControlFlow::Break((objects, more)))
	}

	async fn schedule_update_item_clean(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = match id {
			tg::Either::Left(id) => {
				let Some(object) = crate::propagate!(
					Self::try_get_object_with_transaction(txn, subspace, id).await
				) else {
					Self::clear_update_propagated_versions(txn, subspace, id.to_bytes().as_ref());
					return Ok(ControlFlow::Break(()));
				};
				let partition = Self::partition_for_id(id.to_bytes().as_ref(), partition_total);
				crate::clean::Key::Object {
					id: id.clone(),
					partition,
					touched_at: object.touched_at,
				}
			},
			tg::Either::Right(id) => {
				let Some(process) = crate::propagate!(
					Self::try_get_process_with_transaction(txn, subspace, id).await
				) else {
					Self::clear_update_propagated_versions(txn, subspace, id.to_bytes().as_ref());
					return Ok(ControlFlow::Break(()));
				};
				let partition = Self::partition_for_id(id.to_bytes().as_ref(), partition_total);
				crate::clean::Key::Process {
					id: id.clone(),
					partition,
					touched_at: process.touched_at,
				}
			},
		};
		let key = crate::Key::Clean(key);
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(ControlFlow::Break(()))
	}

	async fn update_object(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let key = crate::Key::Object(crate::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::retry!(result);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(false));
		};
		let mut object = tangram_index::object::Object::deserialize(&bytes)?;

		let children =
			crate::propagate!(Self::get_object_children_with_transaction(txn, subspace, id).await);

		let child_objects = {
			let result = future::try_join_all(
				children
					.iter()
					.map(|child| Self::try_get_object_with_transaction(txn, subspace, child)),
			)
			.await;
			let results = result?;
			let mut values = Vec::with_capacity(results.len());
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		};
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
			let value = object
				.serialize()
				.map_err(|error| tg::error!(!error, "failed to serialize the object"))?;
			txn.set(&key, &value);
		}

		Ok(ControlFlow::Break(changed))
	}

	async fn update_object_permissions_for_subject(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		subject: &tg::authorization::Subject,
		partition_total: u64,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let resource = tg::Id::from(id.clone());
		let (children, entries) = futures::try_join!(
			Self::try_get_object_children_with_transaction(txn, subspace, id),
			Self::get_resource_permission_entries_for_subject_with_transaction(
				txn, subspace, &resource, subject,
			),
		)?;
		let children = match children {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let entries = match entries {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let child_entries = {
			let result = future::try_join_all(children.iter().flatten().map(|child| {
				let resource = tg::Id::from(child.clone());
				async move {
					Self::get_resource_permission_entries_for_subject_with_transaction(
						txn, subspace, &resource, subject,
					)
					.await
				}
			}))
			.await;
			let results = result?;
			let mut values = Vec::with_capacity(results.len());
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		};
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
		let delegations = crate::propagate!(
			Self::get_delegations_with_prefix(txn, subspace, &prefix, usize::MAX).await
		);
		for delegation in delegations {
			let source_entries = crate::propagate!(
				Self::get_resource_permission_entries_for_subject_with_transaction(
					txn,
					subspace,
					&resource,
					&delegation.source
				)
				.await
			);
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

		let materialized_changed = crate::propagate!(
			Self::reconcile_materialized_permissions(
				txn,
				subspace,
				&resource,
				&entries,
				&expected,
				&managed,
				partition_total,
			)
			.await
		);
		let entries = crate::propagate!(
			Self::get_resource_permission_entries_for_subject_with_transaction(
				txn, subspace, &resource, subject,
			)
			.await
		);
		let direct_changed = crate::propagate!(
			Self::promote_process_direct_permissions_for_subject(
				txn,
				subspace,
				id,
				subject,
				&entries,
				partition_total,
			)
			.await
		);

		Ok(ControlFlow::Break(direct_changed || materialized_changed))
	}

	async fn promote_process_direct_permissions_for_subject(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		subject: &tg::authorization::Subject,
		entries: &[crate::permission::PermissionEntry],
		partition_total: u64,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let tg::authorization::Subject::Process(process) = subject else {
			return Ok(ControlFlow::Break(false));
		};
		let processes =
			crate::propagate!(Self::get_object_processes_with_transaction(txn, subspace, id).await);
		let direct = processes
			.into_iter()
			.any(|(candidate, _)| candidate == *process);
		let node = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let mut anchored = direct;
		if !anchored {
			let parents = crate::propagate!(
				Self::get_object_parents_with_transaction(txn, subspace, id).await
			);
			for parent in parents {
				let resource = tg::Id::from(parent);
				let entries = crate::propagate!(
					Self::get_resource_permission_entries_for_subject_with_transaction(
						txn, subspace, &resource, subject,
					)
					.await
				);
				if entries.iter().any(|entry| {
					entry.is_non_expiring_process_direct() && entry.permission.implies(node)
				}) {
					anchored = true;
					break;
				}
			}
		}
		if !anchored {
			return Ok(ControlFlow::Break(false));
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
			if crate::propagate!(
				Self::put_permission_index_entry(
					txn,
					subspace,
					&crate::permission::PermissionIndexEntry {
						creator: Some(&creator),
						expires_at: None,
						permission,
						resource: &resource,
						subject,
					},
					crate::permission::PermissionSource::Direct,
					None,
					partition_total,
				)
				.await
			) {
				changed = true;
			}
		}

		Ok(ControlFlow::Break(changed))
	}

	async fn reconcile_materialized_permissions(
		txn: &crate::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		entries: &[crate::permission::PermissionEntry],
		expected: &BTreeSet<(
			tg::authorization::Subject,
			tg::authorization::Permission,
			Option<i64>,
		)>,
		managed: &BTreeSet<tg::authorization::Permission>,
		partition_total: u64,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
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
			if crate::propagate!(
				Self::delete_permission_index_entry(
					txn,
					subspace,
					&crate::permission::PermissionIndexEntry {
						creator: None,
						expires_at: *expires_at,
						permission: *permission,
						subject,
						resource,
					},
					crate::permission::PermissionSource::Materialized,
					partition_total,
				)
				.await
			) {
				changed = true;
			}
		}
		for (subject, permission, expires_at) in expected.difference(&current) {
			if crate::propagate!(
				Self::put_permission_index_entry(
					txn,
					subspace,
					&crate::permission::PermissionIndexEntry {
						creator: None,
						expires_at: *expires_at,
						permission: *permission,
						subject,
						resource,
					},
					crate::permission::PermissionSource::Materialized,
					None,
					partition_total,
				)
				.await
			) {
				changed = true;
			}
		}
		Ok(ControlFlow::Break(changed))
	}

	async fn update_process_permissions(
		txn: &crate::Transaction,
		subspace: &Subspace,
		input: &ProcessPermissionInputs<'_>,
		partition_total: u64,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let object_subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let node = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		let node_command_objects = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::NodeCommandObjects,
		);
		let node_error_objects = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::NodeErrorObjects,
		);
		let node_log_objects = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::NodeLogObjects,
		);
		let node_output_objects = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::NodeOutputObjects,
		);
		let subtree = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Subtree,
		);
		let subtree_command_objects = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::SubtreeCommandObjects,
		);
		let subtree_error_objects = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::SubtreeErrorObjects,
		);
		let subtree_log_objects = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::SubtreeLogObjects,
		);
		let subtree_output_objects = tg::authorization::Permission::Process(
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
			txn,
			subspace,
			input.resource,
			input.entries,
			&expected,
			&managed,
			partition_total,
		)
		.await
	}

	async fn update_process_permissions_for_subject(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		subject: &tg::authorization::Subject,
		partition_total: u64,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let key = crate::Key::Process(crate::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::retry!(result);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(false));
		};
		let process = tangram_index::process::Process::deserialize(&bytes)?;
		let resource = tg::Id::from(id.clone());
		let entries_future = Self::get_resource_permission_entries_for_subject_with_transaction(
			txn, subspace, &resource, subject,
		);
		let child_entries_future = async {
			let children = crate::propagate!(
				Self::get_process_children_with_transaction(txn, subspace, id).await
			);
			let child_entries = {
				let result = future::try_join_all(children.iter().map(|child| {
					let resource = tg::Id::from(child.clone());
					async move {
						Self::get_resource_permission_entries_for_subject_with_transaction(
							txn, subspace, &resource, subject,
						)
						.await
					}
				}))
				.await;
				let results = result?;
				let mut values = Vec::with_capacity(results.len());
				for result in results {
					let value = match result {
						ControlFlow::Break(value) => value,
						ControlFlow::Continue(error) => {
							return Ok(ControlFlow::Continue(error));
						},
					};
					values.push(value);
				}
				values
			};

			Ok::<_, tg::Error>(ControlFlow::Break(child_entries))
		};
		let object_entries_future = async {
			let objects = crate::propagate!(
				Self::get_process_objects_with_transaction(txn, subspace, id).await
			);
			let mut command_object_entries: Vec<Vec<crate::permission::PermissionEntry>> =
				Vec::new();
			let mut error_object_entries: Vec<Vec<crate::permission::PermissionEntry>> = Vec::new();
			let mut log_object_entries: Vec<Vec<crate::permission::PermissionEntry>> = Vec::new();
			let mut output_object_entries: Vec<Vec<crate::permission::PermissionEntry>> =
				Vec::new();
			for (object, kind) in objects {
				let resource = tg::Id::from(object);
				let entries = crate::propagate!(
					Self::get_resource_permission_entries_for_subject_with_transaction(
						txn, subspace, &resource, subject,
					)
					.await
				);
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

			Ok::<_, tg::Error>(ControlFlow::Break((
				command_object_entries,
				error_object_entries,
				log_object_entries,
				output_object_entries,
			)))
		};
		let (entries, child_entries, object_entries) =
			futures::try_join!(entries_future, child_entries_future, object_entries_future)?;
		let entries = match entries {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let child_entries = match child_entries {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let object_entries = match object_entries {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let (
			command_object_entries,
			error_object_entries,
			log_object_entries,
			output_object_entries,
		) = object_entries;
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
		Self::update_process_permissions(txn, subspace, &entry, partition_total).await
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

	async fn update_process(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		max_process_depth: Option<u64>,
	) -> tg::Result<ControlFlow<ProcessOutput, fdb::FdbError>> {
		let process_key = crate::Key::Process(crate::process::Key::Process(id.clone()));
		let process_key = Self::pack(subspace, &process_key);
		let result = txn.get(&process_key, false).await;
		let bytes = crate::retry!(result);
		let Some(bytes) = bytes else {
			let output = ProcessOutput {
				changed: false,
				depth_exceeded: false,
			};
			return Ok(ControlFlow::Break(output));
		};
		let mut process = tangram_index::process::Process::deserialize(&bytes)?;

		let children_future = async {
			let children = crate::propagate!(
				Self::get_process_children_with_transaction(txn, subspace, id).await
			);
			let children = {
				let result = future::try_join_all(
					children
						.iter()
						.map(|child| Self::try_get_process_with_transaction(txn, subspace, child)),
				)
				.await;
				let results = result?;
				let mut values = Vec::with_capacity(results.len());
				for result in results {
					let value = match result {
						ControlFlow::Break(value) => value,
						ControlFlow::Continue(error) => {
							return Ok(ControlFlow::Continue(error));
						},
					};
					values.push(value);
				}
				values
			};

			Ok::<_, tg::Error>(ControlFlow::Break(children))
		};

		let objects_future = async {
			let objects = crate::propagate!(
				Self::get_process_objects_with_transaction(txn, subspace, id).await
			);
			let mut command_objects: Vec<Option<tangram_index::object::Object>> = Vec::new();
			let mut error_objects: Vec<Option<tangram_index::object::Object>> = Vec::new();
			let mut log_objects: Vec<Option<tangram_index::object::Object>> = Vec::new();
			let mut output_objects: Vec<Option<tangram_index::object::Object>> = Vec::new();
			for (object_id, kind) in &objects {
				let object = crate::propagate!(
					Self::try_get_object_with_transaction(txn, subspace, object_id).await
				);
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

			Ok::<_, tg::Error>(ControlFlow::Break((
				command_objects,
				error_objects,
				log_objects,
				output_objects,
			)))
		};
		let (children, objects) = futures::try_join!(children_future, objects_future)?;
		let children = match children {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let objects = match objects {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let (command_objects, error_objects, log_objects, output_objects) = objects;

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
			let value = process
				.serialize()
				.map_err(|error| tg::error!(!error, "failed to serialize the process"))?;
			txn.set(&process_key, &value);
		}

		let output = ProcessOutput {
			changed,
			depth_exceeded,
		};

		Ok(ControlFlow::Break(output))
	}

	async fn enqueue_parents(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		kind: &Kind,
		version: &fdbt::Versionstamp,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
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
					let delegations = crate::propagate!(
						Self::get_delegations_with_prefix(txn, subspace, &prefix, usize::MAX).await
					);
					for delegation in delegations {
						crate::propagate!(
							Self::enqueue_update_propagate(
								txn,
								subspace,
								&tg::Either::Left(id.clone()),
								&Kind::Permission(delegation.subject),
								version,
								partition_total
							)
							.await
						);
					}
				}

				let parents = crate::propagate!(
					Self::get_object_parents_with_transaction(txn, subspace, id).await
				);
				for parent in parents {
					crate::propagate!(
						Self::enqueue_update_propagate(
							txn,
							subspace,
							&tg::Either::Left(parent),
							kind,
							version,
							partition_total,
						)
						.await
					);
				}
				let process_parents = crate::propagate!(
					Self::get_object_processes_with_transaction(txn, subspace, id).await
				);
				for (process, _kind) in process_parents {
					crate::propagate!(
						Self::enqueue_update_propagate(
							txn,
							subspace,
							&tg::Either::Right(process),
							kind,
							version,
							partition_total,
						)
						.await
					);
				}
			},
			tg::Either::Right(id) => {
				let parents = crate::propagate!(
					Self::get_process_parents_with_transaction(txn, subspace, id).await
				);
				for parent in parents {
					crate::propagate!(
						Self::enqueue_update_propagate(
							txn,
							subspace,
							&tg::Either::Right(parent),
							kind,
							version,
							partition_total,
						)
						.await
					);
				}
			},
		}
		Ok(ControlFlow::Break(()))
	}

	async fn enqueue_update_propagate(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		kind: &Kind,
		version: &fdbt::Versionstamp,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = Self::pack(
			subspace,
			&crate::Key::Update(crate::update::Key::Update {
				id: id.clone(),
				kind: kind.clone(),
			}),
		);
		let result = txn.get(&key, false).await;
		let source = crate::retry!(result)
			.map(|bytes| deserialize_source_update(kind, &bytes))
			.transpose()?;
		if !matches!(source, Some(Source::Put)) {
			let value = serialize_update(kind, Source::Propagate)?;
			txn.set(&key, &value);
		}

		let partition = rand::random_range(0..partition_total);
		let key = Self::pack(
			subspace,
			&crate::Key::Update(crate::update::Key::UpdateVersion {
				id: id.clone(),
				kind: kind.clone(),
				partition,
				version: version.clone(),
			}),
		);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.set(&key, &[]);

		Ok(ControlFlow::Break(()))
	}

	fn clear_update_version(
		txn: &crate::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		kind: &Kind,
		partition: u64,
		version: &fdbt::Versionstamp,
	) {
		let key = Self::pack(
			subspace,
			&crate::Key::Update(crate::update::Key::UpdateVersion {
				id: id.clone(),
				kind: kind.clone(),
				partition,
				version: version.clone(),
			}),
		);
		txn.clear(&key);
	}
}

fn update_version_key_kind(kind: tangram_index::update::Kind) -> KeyKind {
	match kind {
		tangram_index::update::Kind::Permission => KeyKind::PermissionUpdateVersion,
		tangram_index::update::Kind::StorageAndMetadata => KeyKind::StorageAndMetadataUpdateVersion,
		tangram_index::update::Kind::Usage => KeyKind::UsageUpdateVersion,
	}
}

fn deserialize_source_update(kind: &Kind, bytes: &[u8]) -> tg::Result<Source> {
	let source = match kind {
		Kind::Permission(_) => PermissionUpdate::deserialize(bytes)?.source,
		Kind::StorageAndMetadata => StorageAndMetadataUpdate::deserialize(bytes)?.source,
		Kind::Usage(_) => return Err(tg::error!("expected a source update")),
	};

	Ok(source)
}

fn serialize_update(kind: &Kind, source: Source) -> tg::Result<Vec<u8>> {
	let value = match kind {
		Kind::Permission(_) => PermissionUpdate::new(source).serialize()?,
		Kind::StorageAndMetadata => StorageAndMetadataUpdate::new(source).serialize()?,
		Kind::Usage(_) => UsageUpdate::new().serialize()?,
	};

	Ok(value)
}
