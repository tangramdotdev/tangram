mod key;

pub(super) use key::{Key, Kind, StorageKind};

use {
	super::{Index, Kind as KeyKind, Request, Response},
	foundationdb as fdb,
	foundationdb_tuple::{self as fdbt, Subspace},
	futures::{TryStreamExt as _, future},
	num_traits::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

const STORAGE_RELATION_BATCH_SIZE: usize = 8;

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
pub(super) struct StorageUpdate {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub cursor: Option<StorageCursor>,
}

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) enum StorageCursor {
	#[tangram_serialize(id = 0)]
	Object(tg::object::Id),

	#[tangram_serialize(id = 3)]
	ObjectAccount(crate::usage::Account),

	#[tangram_serialize(id = 1)]
	ProcessChild(i64),

	#[tangram_serialize(id = 2)]
	ProcessObject(Option<ProcessObjectCursor>),

	#[tangram_serialize(id = 4)]
	ProcessAccount(crate::usage::Account),
}

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(super) struct ProcessObjectCursor {
	#[tangram_serialize(id = 0)]
	pub kind: crate::process::object::Kind,

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

struct ProcessGrantInputs<'a> {
	resource: &'a tg::Id,
	entries: &'a [crate::fdb::grant::GrantEntry],
	child_entries: &'a [Vec<crate::fdb::grant::GrantEntry>],
	command_object_entries: Option<&'a [crate::fdb::grant::GrantEntry]>,
	error_object_entries: &'a [Vec<crate::fdb::grant::GrantEntry>],
	log_object_entries: Option<&'a [crate::fdb::grant::GrantEntry]>,
	output_object_entries: &'a [Vec<crate::fdb::grant::GrantEntry>],
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

enum StorageRelationship {
	Object(tg::object::Id),
	Process(tg::process::Id),
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
			.map_err(|error| tg::error!(!error, "failed to serialize the update"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the update"))
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
		Self { cursor: None }
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
	pub(super) fn enqueue_update(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		partition_total: u64,
	) {
		Self::enqueue_update_with_kind(
			txn,
			subspace,
			id,
			&Kind::Node,
			Source::Put,
			partition_total,
		);
	}

	pub(super) fn enqueue_update_with_kind(
		txn: &fdb::Transaction,
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
		txn: &fdb::Transaction,
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
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		kind: &Kind,
		value: &[u8],
		partition_total: u64,
		version: Option<&fdbt::Versionstamp>,
	) {
		let key = Self::pack(
			subspace,
			&crate::fdb::Key::Update(crate::fdb::update::Key::Update {
				id: id.clone(),
				kind: kind.clone(),
			}),
		);
		txn.set(&key, value);

		let partition = rand::random_range(0..partition_total);
		if let Some(version) = version {
			let key = Self::pack(
				subspace,
				&crate::fdb::Key::Update(crate::fdb::update::Key::UpdateVersion {
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
				&crate::fdb::Key::Update(crate::fdb::update::Key::UpdateVersion {
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

	pub(crate) async fn try_get_oldest_update_transaction_id_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		kind: crate::update::Kind,
		partition_total: u64,
	) -> tg::Result<Option<u64>> {
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
				let entries = txn.get_range(&range, 1, false).await.map_err(|error| {
					tg::error!(!error, "failed to get the update version range")
				})?;
				let Some(entry) = entries.first() else {
					return Ok(None);
				};
				let key = Self::unpack(subspace, entry.key())?;
				let crate::fdb::Key::Update(crate::fdb::update::Key::UpdateVersion {
					version, ..
				}) = key
				else {
					return Err(tg::error!("unexpected update key"));
				};
				let transaction_id =
					u64::from_be_bytes(version.as_bytes()[..8].try_into().unwrap());
				Ok(Some(transaction_id))
			}
		});
		let transaction_id = future::try_join_all(futures)
			.await?
			.into_iter()
			.flatten()
			.min();

		Ok(transaction_id)
	}

	pub async fn update_batch(
		&self,
		kind: crate::update::Kind,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<crate::update::Output> {
		let request = Request::Update(crate::fdb::Update {
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
		txn: &fdb::Transaction,
		subspace: &Subspace,
		batch_size: usize,
		kind: crate::update::Kind,
		partition_start: u64,
		partition_end: u64,
		max_process_depth: Option<u64>,
		partition_total: u64,
		usage_partition_total: u64,
	) -> tg::Result<crate::update::Output> {
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
			let partition_entries = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get update version range"))?;
			for entry in partition_entries {
				let key = Self::unpack(subspace, entry.key())?;
				let crate::fdb::Key::Update(crate::fdb::update::Key::UpdateVersion {
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

		let mut output = crate::update::Output::default();
		for (partition, version, id, kind) in entries {
			let key = Self::pack(
				subspace,
				&crate::fdb::Key::Update(crate::fdb::update::Key::Update {
					id: id.clone(),
					kind: kind.clone(),
				}),
			);
			let value = txn
				.get(&key, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get update key"))?;

			let Some(value) = value else {
				Self::clear_update_version(txn, subspace, &id, &kind, partition, &version);
				output.count += 1;
				continue;
			};

			let (cursor, source) = match &kind {
				Kind::Grant(_) | Kind::Node => {
					(None, Some(deserialize_source_update(&kind, &value)?))
				},
				Kind::Storage(_) => (StorageUpdate::deserialize(&value)?.cursor, None),
			};
			let mut next_cursor = None;

			let changed = match &kind {
				Kind::Grant(subject) => match &id {
					tg::Either::Left(id) => {
						Self::update_object_grants_for_subject(
							txn,
							subspace,
							id,
							subject,
							partition_total,
						)
						.await?
					},
					tg::Either::Right(id) => {
						Self::update_process_grants_for_subject(
							txn,
							subspace,
							id,
							subject,
							partition_total,
						)
						.await?
					},
				},
				Kind::Node => match &id {
					tg::Either::Left(id) => Self::update_object(txn, subspace, id).await?,
					tg::Either::Right(id) => {
						let process_output =
							Self::update_process(txn, subspace, id, max_process_depth).await?;
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
						Self::put_account_object(
							txn,
							subspace,
							&crate::usage::storage::put::ObjectArg {
								account: account.clone(),
								object: object.clone(),
								touched_at: *touched_at,
							},
							partition_total,
							usage_partition_total,
							false,
							Some(&version),
						)
						.await?
					},
					tg::Either::Right(process) => {
						Self::put_account_process(
							txn,
							subspace,
							&crate::usage::storage::put::ProcessArg {
								account: account.clone(),
								process: process.clone(),
								touched_at: *touched_at,
							},
							partition_total,
							usage_partition_total,
							false,
							Some(&version),
						)
						.await?
					},
				},
				Kind::Storage(StorageKind::Clean(account)) => {
					next_cursor = Self::propagate_storage_clean(
						txn,
						subspace,
						&id,
						account,
						cursor.as_ref(),
						partition_total,
					)
					.await?;
					false
				},
				Kind::Storage(StorageKind::CleanAll) => {
					next_cursor = Self::propagate_storage_accounts_clean(
						txn,
						subspace,
						&id,
						cursor.as_ref(),
						partition_total,
					)
					.await?;
					false
				},
				Kind::Storage(StorageKind::Propagate {
					account,
					touched_at,
				}) => {
					next_cursor = Self::propagate_storage_relationships(
						txn,
						subspace,
						&id,
						account,
						cursor.as_ref(),
						partition_total,
						*touched_at,
						&version,
					)
					.await?;
					false
				},
			};

			if !matches!(kind, Kind::Storage(_))
				&& match source.unwrap() {
					Source::Put => true,
					Source::Propagate => changed,
				} {
				Self::enqueue_parents(txn, subspace, &id, &kind, &version, partition_total).await?;
			}

			let continued = if let Some(cursor) = next_cursor {
				let update = StorageUpdate {
					cursor: Some(cursor),
				};
				let key = Self::pack(
					subspace,
					&crate::fdb::Key::Update(crate::fdb::update::Key::Update {
						id: id.clone(),
						kind: kind.clone(),
					}),
				);
				txn.set(&key, &update.serialize()?);
				true
			} else {
				Self::schedule_update_item_clean(txn, subspace, &id, partition_total).await?;
				let key = Self::pack(
					subspace,
					&crate::fdb::Key::Update(crate::fdb::update::Key::Update {
						id: id.clone(),
						kind: kind.clone(),
					}),
				);
				txn.clear(&key);
				false
			};
			if !continued {
				Self::clear_update_version(txn, subspace, &id, &kind, partition, &version);
			}

			output.count += 1;
		}

		Ok(output)
	}

	#[allow(clippy::too_many_arguments)]
	async fn propagate_storage_relationships(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		account: &crate::usage::Account,
		cursor: Option<&StorageCursor>,
		partition_total: u64,
		touched_at: i64,
		version: &fdbt::Versionstamp,
	) -> tg::Result<Option<StorageCursor>> {
		let (relationships, cursor) =
			Self::get_storage_relationships_page(txn, subspace, id, cursor).await?;
		let kind = Kind::Storage(StorageKind::Add {
			account: account.clone(),
			touched_at,
		});
		for relationship in relationships {
			let id = match relationship {
				StorageRelationship::Object(id) => tg::Either::Left(id),
				StorageRelationship::Process(id) => tg::Either::Right(id),
			};
			Self::enqueue_update_with_kind_at_version(
				txn,
				subspace,
				&id,
				&kind,
				Source::Put,
				partition_total,
				Some(version),
			);
		}

		Ok(cursor)
	}

	async fn propagate_storage_clean(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		account: &crate::usage::Account,
		cursor: Option<&StorageCursor>,
		partition_total: u64,
	) -> tg::Result<Option<StorageCursor>> {
		let (relationships, cursor) =
			Self::get_storage_relationships_page(txn, subspace, id, cursor).await?;
		future::try_join_all(relationships.iter().map(|relationship| async move {
			match relationship {
				StorageRelationship::Object(object) => {
					Self::schedule_account_object_for_cleaning(
						txn,
						subspace,
						account,
						object,
						partition_total,
					)
					.await
				},
				StorageRelationship::Process(process) => {
					Self::schedule_account_process_for_cleaning(
						txn,
						subspace,
						account,
						process,
						partition_total,
					)
					.await
				},
			}
		}))
		.await?;
		Ok(cursor)
	}

	async fn propagate_storage_accounts_clean(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		cursor: Option<&StorageCursor>,
		partition_total: u64,
	) -> tg::Result<Option<StorageCursor>> {
		let (accounts, cursor) = Self::get_storage_accounts_page(txn, subspace, id, cursor).await?;
		future::try_join_all(accounts.iter().map(|account| async move {
			match id {
				tg::Either::Left(object) => {
					Self::schedule_account_object_for_cleaning(
						txn,
						subspace,
						account,
						object,
						partition_total,
					)
					.await
				},
				tg::Either::Right(process) => {
					Self::schedule_account_process_for_cleaning(
						txn,
						subspace,
						account,
						process,
						partition_total,
					)
					.await
				},
			}
		}))
		.await?;

		Ok(cursor)
	}

	async fn get_storage_relationships_page(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		cursor: Option<&StorageCursor>,
	) -> tg::Result<(Vec<StorageRelationship>, Option<StorageCursor>)> {
		match id {
			tg::Either::Left(object) => {
				let after = match cursor {
					None => None,
					Some(StorageCursor::Object(object)) => Some(object),
					Some(
						StorageCursor::ObjectAccount(_)
						| StorageCursor::ProcessChild(_)
						| StorageCursor::ProcessObject(_)
						| StorageCursor::ProcessAccount(_),
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
						StorageCursor::Object(_)
							| StorageCursor::ObjectAccount(_)
							| StorageCursor::ProcessAccount(_)
					)
				) {
					return Err(tg::error!(%process, "a process update has an invalid cursor"));
				}
				Self::get_storage_process_relationships_page(txn, subspace, process, cursor).await
			},
		}
	}

	async fn get_storage_accounts_page(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		cursor: Option<&StorageCursor>,
	) -> tg::Result<(Vec<crate::usage::Account>, Option<StorageCursor>)> {
		let (kind, item, after) = match (id, cursor) {
			(tg::Either::Left(object), None) => (KeyKind::ObjectAccount, object.to_bytes(), None),
			(tg::Either::Left(object), Some(StorageCursor::ObjectAccount(account))) => (
				KeyKind::ObjectAccount,
				object.to_bytes(),
				Some(crate::fdb::Key::Usage(
					crate::fdb::usage::Key::ObjectAccount {
						account: account.clone(),
						object: object.clone(),
					},
				)),
			),
			(tg::Either::Right(process), None) => {
				(KeyKind::ProcessAccount, process.to_bytes(), None)
			},
			(tg::Either::Right(process), Some(StorageCursor::ProcessAccount(account))) => (
				KeyKind::ProcessAccount,
				process.to_bytes(),
				Some(crate::fdb::Key::Usage(
					crate::fdb::usage::Key::ProcessAccount {
						account: account.clone(),
						process: process.clone(),
					},
				)),
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
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get usage accounts"))?;
		let mut accounts = entries
			.iter()
			.map(|entry| match Self::unpack(subspace, entry.key())? {
				crate::fdb::Key::Usage(
					crate::fdb::usage::Key::ObjectAccount { account, .. }
					| crate::fdb::usage::Key::ProcessAccount { account, .. },
				) => Ok(account),
				_ => Err(tg::error!("unexpected key type")),
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let cursor = if accounts.len() > STORAGE_RELATION_BATCH_SIZE {
			accounts.truncate(STORAGE_RELATION_BATCH_SIZE);
			let account = accounts.last().unwrap().clone();
			Some(match id {
				tg::Either::Left(_) => StorageCursor::ObjectAccount(account),
				tg::Either::Right(_) => StorageCursor::ProcessAccount(account),
			})
		} else {
			None
		};

		Ok((accounts, cursor))
	}

	async fn get_storage_object_relationships_page(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		object: &tg::object::Id,
		after: Option<&tg::object::Id>,
	) -> tg::Result<(Vec<StorageRelationship>, Option<StorageCursor>)> {
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
			let key = crate::fdb::Key::Object(crate::fdb::object::Key::ObjectChild {
				child: after.clone(),
				object: object.clone(),
			});
			let mut begin = Self::pack(subspace, &key);
			begin.push(0);
			range.begin = fdb::KeySelector::first_greater_or_equal(begin);
		}
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, %object, "failed to get object relationships"))?;
		let mut children = entries
			.iter()
			.map(|entry| {
				let crate::fdb::Key::Object(crate::fdb::object::Key::ObjectChild { child, .. }) =
					Self::unpack(subspace, entry.key())?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let cursor = if children.len() > STORAGE_RELATION_BATCH_SIZE {
			children.truncate(STORAGE_RELATION_BATCH_SIZE);
			Some(StorageCursor::Object(children.last().unwrap().clone()))
		} else {
			None
		};
		let relationships = children
			.into_iter()
			.map(StorageRelationship::Object)
			.collect();

		Ok((relationships, cursor))
	}

	async fn get_storage_process_relationships_page(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		process: &tg::process::Id,
		cursor: Option<&StorageCursor>,
	) -> tg::Result<(Vec<StorageRelationship>, Option<StorageCursor>)> {
		let mut relationships = Vec::new();
		let process_object_cursor = match cursor {
			None | Some(StorageCursor::ProcessChild(_)) => {
				let after = match cursor {
					Some(StorageCursor::ProcessChild(position)) => Some(*position),
					_ => None,
				};
				let (children, child_cursor, more) =
					Self::get_storage_process_children_page(txn, subspace, process, after).await?;
				relationships.extend(children.into_iter().map(StorageRelationship::Process));
				if more {
					return Ok((
						relationships,
						Some(StorageCursor::ProcessChild(child_cursor.unwrap())),
					));
				}
				if relationships.len() == STORAGE_RELATION_BATCH_SIZE {
					return Ok((relationships, Some(StorageCursor::ProcessObject(None))));
				}
				None
			},
			Some(StorageCursor::ProcessObject(cursor)) => cursor.as_ref(),
			Some(
				StorageCursor::Object(_)
				| StorageCursor::ObjectAccount(_)
				| StorageCursor::ProcessAccount(_),
			) => unreachable!(),
		};
		let limit = STORAGE_RELATION_BATCH_SIZE - relationships.len();
		let (objects, more) = Self::get_storage_process_objects_page(
			txn,
			subspace,
			process,
			process_object_cursor,
			limit,
		)
		.await?;
		let cursor = objects.last().map(|(object, kind)| ProcessObjectCursor {
			kind: *kind,
			object: object.clone(),
		});
		relationships.extend(
			objects
				.into_iter()
				.map(|(object, _)| StorageRelationship::Object(object)),
		);
		let cursor = more.then_some(StorageCursor::ProcessObject(cursor));

		Ok((relationships, cursor))
	}

	async fn get_storage_process_children_page(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		process: &tg::process::Id,
		after: Option<i64>,
	) -> tg::Result<(Vec<tg::process::Id>, Option<i64>, bool)> {
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
		let entries = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await
			.map_err(|error| tg::error!(!error, %process, "failed to get process children"))?;
		let mut children = entries
			.iter()
			.map(|entry| {
				let crate::fdb::Key::Process(crate::fdb::process::Key::ProcessChild {
					child,
					position,
					..
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

		Ok((children, cursor, more))
	}

	async fn get_storage_process_objects_page(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		process: &tg::process::Id,
		after: Option<&ProcessObjectCursor>,
		limit: usize,
	) -> tg::Result<(Vec<(tg::object::Id, crate::process::object::Kind)>, bool)> {
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
			let key = crate::fdb::Key::Process(crate::fdb::process::Key::ProcessObject {
				kind: after.kind,
				object: after.object.clone(),
				process: process.clone(),
			});
			let mut begin = Self::pack(subspace, &key);
			begin.push(0);
			range.begin = fdb::KeySelector::first_greater_or_equal(begin);
		}
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, %process, "failed to get process objects"))?;
		let mut objects = entries
			.iter()
			.map(|entry| {
				let crate::fdb::Key::Process(crate::fdb::process::Key::ProcessObject {
					kind,
					object,
					..
				}) = Self::unpack(subspace, entry.key())?
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((object, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let more = objects.len() > limit;
		objects.truncate(limit);

		Ok((objects, more))
	}

	async fn schedule_update_item_clean(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = match id {
			tg::Either::Left(id) => {
				let Some(object) = Self::try_get_object_with_transaction(txn, subspace, id).await?
				else {
					return Ok(());
				};
				let partition = Self::partition_for_id(id.to_bytes().as_ref(), partition_total);
				crate::fdb::clean::Key::Object {
					id: id.clone(),
					partition,
					touched_at: object.touched_at,
				}
			},
			tg::Either::Right(id) => {
				let Some(process) =
					Self::try_get_process_with_transaction(txn, subspace, id).await?
				else {
					return Ok(());
				};
				let partition = Self::partition_for_id(id.to_bytes().as_ref(), partition_total);
				crate::fdb::clean::Key::Process {
					id: id.clone(),
					partition,
					touched_at: process.touched_at,
				}
			},
		};
		let key = crate::fdb::Key::Clean(key);
		txn.set(&Self::pack(subspace, &key), &[]);

		Ok(())
	}

	async fn update_object(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<bool> {
		let key = crate::fdb::Key::Object(crate::fdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?;
		let Some(bytes) = bytes else {
			return Ok(false);
		};
		let mut object = crate::object::Object::deserialize(&bytes)?;

		let children = Self::get_object_children_with_transaction(txn, subspace, id).await?;

		let child_objects: Vec<Option<crate::object::Object>> = future::try_join_all(
			children
				.iter()
				.map(|child| Self::try_get_object_with_transaction(txn, subspace, child)),
		)
		.await?;
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
			let value = object
				.serialize()
				.map_err(|error| tg::error!(!error, "failed to serialize the object"))?;
			txn.set(&key, &value);
		}

		Ok(changed)
	}

	async fn update_object_grants_for_subject(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		subject: &tg::authorization::Subject,
		partition_total: u64,
	) -> tg::Result<bool> {
		let resource = tg::Id::from(id.clone());
		let children = Self::get_object_children_with_transaction(txn, subspace, id).await?;
		let entries = Self::get_resource_grant_entries_for_subject_with_transaction(
			txn, subspace, &resource, subject,
		)
		.await?;
		let child_entries = future::try_join_all(children.iter().map(|child| {
			let resource = tg::Id::from(child.clone());
			async move {
				Self::get_resource_grant_entries_for_subject_with_transaction(
					txn, subspace, &resource, subject,
				)
				.await
			}
		}))
		.await?;
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
		Self::reconcile_materialized_grants(
			txn,
			subspace,
			&resource,
			&entries,
			&expected,
			&managed,
			partition_total,
		)
		.await
	}

	async fn reconcile_materialized_grants(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Id,
		entries: &[crate::fdb::grant::GrantEntry],
		expected: &BTreeSet<(
			tg::authorization::Subject,
			tg::authorization::Permission,
			Option<i64>,
		)>,
		managed: &BTreeSet<tg::authorization::Permission>,
		partition_total: u64,
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
			if Self::delete_grant_index_entry(
				txn,
				subspace,
				&crate::fdb::grant::GrantIndexEntry {
					creator: None,
					expires_at: *expires_at,
					permission: *permission,
					subject,
					resource,
				},
				crate::fdb::grant::GrantSource::Materialized,
				partition_total,
			)
			.await?
			{
				changed = true;
			}
		}
		for (subject, permission, expires_at) in expected.difference(&current) {
			if Self::put_grant_index_entry(
				txn,
				subspace,
				&crate::fdb::grant::GrantIndexEntry {
					creator: None,
					expires_at: *expires_at,
					permission: *permission,
					subject,
					resource,
				},
				crate::fdb::grant::GrantSource::Materialized,
				None,
				partition_total,
			)
			.await?
			{
				changed = true;
			}
		}
		Ok(changed)
	}

	async fn update_process_grants(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		input: &ProcessGrantInputs<'_>,
		partition_total: u64,
	) -> tg::Result<bool> {
		let object_subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let node = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		let node_command = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::NodeCommand,
		);
		let node_error = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::NodeError,
		);
		let node_log = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::NodeLog,
		);
		let node_output = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::NodeOutput,
		);
		let subtree = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Subtree,
		);
		let subtree_command = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::SubtreeCommand,
		);
		let subtree_error = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::SubtreeError,
		);
		let subtree_log = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::SubtreeLog,
		);
		let subtree_output = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::SubtreeOutput,
		);

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

	async fn update_process_grants_for_subject(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		subject: &tg::authorization::Subject,
		partition_total: u64,
	) -> tg::Result<bool> {
		let key = crate::fdb::Key::Process(crate::fdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
		let Some(bytes) = bytes else {
			return Ok(false);
		};
		let process = crate::process::Process::deserialize(&bytes)?;
		let resource = tg::Id::from(id.clone());
		let entries = Self::get_resource_grant_entries_for_subject_with_transaction(
			txn, subspace, &resource, subject,
		)
		.await?;
		let children = Self::get_process_children_with_transaction(txn, subspace, id).await?;
		let child_entries = future::try_join_all(children.iter().map(|child| {
			let resource = tg::Id::from(child.clone());
			async move {
				Self::get_resource_grant_entries_for_subject_with_transaction(
					txn, subspace, &resource, subject,
				)
				.await
			}
		}))
		.await?;
		let objects = Self::get_process_objects_with_transaction(txn, subspace, id).await?;
		let mut command_object_entries: Option<Vec<crate::fdb::grant::GrantEntry>> = None;
		let mut error_object_entries: Vec<Vec<crate::fdb::grant::GrantEntry>> = Vec::new();
		let mut log_object_entries: Option<Vec<crate::fdb::grant::GrantEntry>> = None;
		let mut output_object_entries: Vec<Vec<crate::fdb::grant::GrantEntry>> = Vec::new();
		for (object, kind) in objects {
			let resource = tg::Id::from(object);
			let entries = Self::get_resource_grant_entries_for_subject_with_transaction(
				txn, subspace, &resource, subject,
			)
			.await?;
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
			txn,
			subspace,
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
			partition_total,
		)
		.await
	}

	fn insert_object_aspect_grants<'a>(
		expected: &mut BTreeSet<(
			tg::authorization::Subject,
			tg::authorization::Permission,
			Option<i64>,
		)>,
		target_entries: &[crate::fdb::grant::GrantEntry],
		sources: impl IntoIterator<Item = &'a crate::fdb::grant::GrantEntry>,
		required: &[&[crate::fdb::grant::GrantEntry]],
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
		entries: &[crate::fdb::grant::GrantEntry],
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
		entries: &[crate::fdb::grant::GrantEntry],
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

	async fn update_process(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		max_process_depth: Option<u64>,
	) -> tg::Result<ProcessOutput> {
		let process_key = crate::fdb::Key::Process(crate::fdb::process::Key::Process(id.clone()));
		let process_key = Self::pack(subspace, &process_key);
		let bytes = txn
			.get(&process_key, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?;
		let Some(bytes) = bytes else {
			let output = ProcessOutput {
				changed: false,
				depth_exceeded: false,
			};
			return Ok(output);
		};
		let mut process = crate::process::Process::deserialize(&bytes)?;

		let children = Self::get_process_children_with_transaction(txn, subspace, id).await?;
		let children = future::try_join_all(
			children
				.iter()
				.map(|child| Self::try_get_process_with_transaction(txn, subspace, child)),
		)
		.await?;

		let objects = Self::get_process_objects_with_transaction(txn, subspace, id).await?;
		let mut command_object: Option<crate::object::Object> = None;
		let mut error_objects: Vec<Option<crate::object::Object>> = Vec::new();
		let mut log_object: Option<Option<crate::object::Object>> = None;
		let mut output_objects: Vec<Option<crate::object::Object>> = Vec::new();
		for (object_id, kind) in &objects {
			let object = Self::try_get_object_with_transaction(txn, subspace, object_id).await?;
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
			let value = process
				.serialize()
				.map_err(|error| tg::error!(!error, "failed to serialize the process"))?;
			txn.set(&process_key, &value);
		}

		let output = ProcessOutput {
			changed,
			depth_exceeded,
		};

		Ok(output)
	}

	async fn enqueue_parents(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		kind: &Kind,
		version: &fdbt::Versionstamp,
		partition_total: u64,
	) -> tg::Result<()> {
		match id {
			tg::Either::Left(id) => {
				let parents = Self::get_object_parents_with_transaction(txn, subspace, id).await?;
				for parent in parents {
					Self::enqueue_update_propagate(
						txn,
						subspace,
						&tg::Either::Left(parent),
						kind,
						version,
						partition_total,
					)
					.await?;
				}
				let process_parents =
					Self::get_object_processes_with_transaction(txn, subspace, id).await?;
				for (process, _kind) in process_parents {
					Self::enqueue_update_propagate(
						txn,
						subspace,
						&tg::Either::Right(process),
						kind,
						version,
						partition_total,
					)
					.await?;
				}
			},
			tg::Either::Right(id) => {
				let parents = Self::get_process_parents_with_transaction(txn, subspace, id).await?;
				for parent in parents {
					Self::enqueue_update_propagate(
						txn,
						subspace,
						&tg::Either::Right(parent),
						kind,
						version,
						partition_total,
					)
					.await?;
				}
			},
		}
		Ok(())
	}

	async fn enqueue_update_propagate(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		kind: &Kind,
		version: &fdbt::Versionstamp,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Self::pack(
			subspace,
			&crate::fdb::Key::Update(crate::fdb::update::Key::Update {
				id: id.clone(),
				kind: kind.clone(),
			}),
		);
		let source = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get update key"))?
			.map(|bytes| deserialize_source_update(kind, &bytes))
			.transpose()?;
		if !matches!(source, Some(Source::Put)) {
			let value = serialize_update(kind, Source::Propagate)?;
			txn.set(&key, &value);
		}

		let partition = rand::random_range(0..partition_total);
		let key = Self::pack(
			subspace,
			&crate::fdb::Key::Update(crate::fdb::update::Key::UpdateVersion {
				id: id.clone(),
				kind: kind.clone(),
				partition,
				version: version.clone(),
			}),
		);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.set(&key, &[]);

		Ok(())
	}

	fn clear_update_version(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		kind: &Kind,
		partition: u64,
		version: &fdbt::Versionstamp,
	) {
		let key = Self::pack(
			subspace,
			&crate::fdb::Key::Update(crate::fdb::update::Key::UpdateVersion {
				id: id.clone(),
				kind: kind.clone(),
				partition,
				version: version.clone(),
			}),
		);
		txn.clear(&key);
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
