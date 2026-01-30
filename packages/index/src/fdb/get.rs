use {
	super::{
		Index, Key, KeyKind, ObjectCoreField, ObjectField, ObjectMetadataField, ObjectStoredField,
		ProcessCoreField, ProcessField, ProcessMetadataField, ProcessStoredField,
	},
	crate::{Object, ObjectStored, Process, ProcessObjectKind, ProcessStored},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_util::varint,
};

impl Index {
	pub async fn try_get_objects(&self, ids: &[tg::object::Id]) -> tg::Result<Vec<Option<Object>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create the transaction"))?;

		let outputs = futures::future::try_join_all(
			ids.iter()
				.map(|id| Self::try_get_object_with_transaction(&txn, &self.subspace, id)),
		)
		.await?;

		Ok(outputs)
	}

	pub async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create the transaction"))?;

		let outputs = futures::future::try_join_all(
			ids.iter()
				.map(|id| Self::try_get_process_with_transaction(&txn, &self.subspace, id)),
		)
		.await?;

		Ok(outputs)
	}

	pub(super) async fn try_get_object_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<Option<Object>> {
		let bytes = id.to_bytes();
		let key = (KeyKind::Object.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};

		let entries = txn
			.get_ranges_keyvalues(range, true)
			.try_collect::<Vec<_>>()
			.await
			.map_err(|source| tg::error!(!source, "failed to scan object fields"))?;

		let key = Key::Object {
			id: id.clone(),
			field: ObjectField::Core(ObjectCoreField::Exists),
		};
		let exists_key = Self::pack(subspace, &key);
		let mut exists_key_end = exists_key.clone();
		exists_key_end.push(0x00);
		txn.add_conflict_range(
			&exists_key,
			&exists_key_end,
			fdb::options::ConflictRangeType::Read,
		)
		.map_err(|source| tg::error!(!source, "failed to add read conflict range"))?;

		let mut exists = false;
		let mut touched_at: Option<i64> = None;
		let mut reference_count: u64 = 0;
		let mut cache_entry: Option<tg::artifact::Id> = None;
		let mut metadata = tg::object::Metadata::default();
		let mut stored = ObjectStored::default();

		for entry in entries {
			let Key::Object { field, .. } = Self::unpack(subspace, entry.key())? else {
				return Err(tg::error!("unexpected key type"));
			};
			let value = entry.value();

			match field {
				ObjectField::Core(field) => match field {
					ObjectCoreField::Exists => {
						exists = true;
					},
					ObjectCoreField::TouchedAt => {
						touched_at = Some(
							varint::decode_ivarint(value)
								.ok_or_else(|| tg::error!("invalid touched at"))?,
						);
					},
					ObjectCoreField::ReferenceCount => {
						reference_count = varint::decode_uvarint(value)
							.ok_or_else(|| tg::error!("invalid reference count"))?;
					},
					ObjectCoreField::CacheEntry => {
						cache_entry = Some(
							tg::artifact::Id::from_slice(value)
								.map_err(|source| tg::error!(!source, "invalid cache entry"))?,
						);
					},
				},
				ObjectField::Metadata(field) => {
					Self::apply_object_metadata_field(&mut metadata, field, value)?;
				},
				ObjectField::Stored(field) => match field {
					ObjectStoredField::Subtree => {
						stored.subtree = true;
					},
				},
			}
		}

		if !exists {
			return Ok(None);
		}

		let touched_at =
			touched_at.ok_or_else(|| tg::error!("object exists but touched_at is not set"))?;

		Ok(Some(Object {
			cache_entry,
			metadata,
			reference_count,
			stored,
			touched_at,
		}))
	}

	pub(super) async fn try_get_process_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<Option<Process>> {
		let bytes = id.to_bytes();
		let key = (KeyKind::Process.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};

		let entries = txn
			.get_ranges_keyvalues(range, true)
			.try_collect::<Vec<_>>()
			.await
			.map_err(|source| tg::error!(!source, "failed to scan process fields"))?;

		let key = Key::Process {
			id: id.clone(),
			field: ProcessField::Core(ProcessCoreField::Exists),
		};
		let exists_key = Self::pack(subspace, &key);
		let mut exists_key_end = exists_key.clone();
		exists_key_end.push(0x00);
		txn.add_conflict_range(
			&exists_key,
			&exists_key_end,
			fdb::options::ConflictRangeType::Read,
		)
		.map_err(|source| tg::error!(!source, "failed to add read conflict range"))?;

		let mut exists = false;
		let mut touched_at: Option<i64> = None;
		let mut reference_count: u64 = 0;
		let mut metadata = tg::process::Metadata::default();
		let mut stored = ProcessStored::default();

		for entry in entries {
			let Key::Process { field, .. } = Self::unpack(subspace, entry.key())? else {
				return Err(tg::error!("unexpected key type"));
			};
			let value = entry.value();

			match field {
				ProcessField::Core(field) => match field {
					ProcessCoreField::Exists => {
						exists = true;
					},
					ProcessCoreField::TouchedAt => {
						touched_at = Some(
							varint::decode_ivarint(value)
								.ok_or_else(|| tg::error!("invalid touched_at"))?,
						);
					},
					ProcessCoreField::ReferenceCount => {
						reference_count = varint::decode_uvarint(value)
							.ok_or_else(|| tg::error!("invalid reference_count"))?;
					},
				},
				ProcessField::Metadata(field) => {
					Self::apply_process_metadata_field(&mut metadata, field, value)?;
				},
				ProcessField::Stored(field) => {
					Self::apply_process_stored_field(&mut stored, field);
				},
			}
		}

		if !exists {
			return Ok(None);
		}

		let touched_at =
			touched_at.ok_or_else(|| tg::error!("process exists but touched_at is not set"))?;

		Ok(Some(Process {
			metadata,
			reference_count,
			stored,
			touched_at,
		}))
	}

	fn apply_object_metadata_field(
		metadata: &mut tg::object::Metadata,
		field: ObjectMetadataField,
		value: &[u8],
	) -> tg::Result<()> {
		match field {
			ObjectMetadataField::NodeSize => {
				metadata.node.size =
					varint::decode_uvarint(value).ok_or_else(|| tg::error!("invalid node size"))?;
			},
			ObjectMetadataField::NodeSolvable => {
				metadata.node.solvable = value.is_empty() || value[0] != 0;
			},
			ObjectMetadataField::NodeSolved => {
				metadata.node.solved = value.is_empty() || value[0] != 0;
			},
			ObjectMetadataField::SubtreeCount => {
				metadata.subtree.count = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid subtree count"))?,
				);
			},
			ObjectMetadataField::SubtreeDepth => {
				metadata.subtree.depth = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid subtree depth"))?,
				);
			},
			ObjectMetadataField::SubtreeSize => {
				metadata.subtree.size = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid subtree size"))?,
				);
			},
			ObjectMetadataField::SubtreeSolvable => {
				metadata.subtree.solvable = Some(value.is_empty() || value[0] != 0);
			},
			ObjectMetadataField::SubtreeSolved => {
				metadata.subtree.solved = Some(value.is_empty() || value[0] != 0);
			},
		}
		Ok(())
	}

	fn apply_process_stored_field(stored: &mut ProcessStored, field: ProcessStoredField) {
		match field {
			ProcessStoredField::NodeCommand => {
				stored.node_command = true;
			},
			ProcessStoredField::NodeError => {
				stored.node_error = true;
			},
			ProcessStoredField::NodeLog => {
				stored.node_log = true;
			},
			ProcessStoredField::NodeOutput => {
				stored.node_output = true;
			},
			ProcessStoredField::Subtree => {
				stored.subtree = true;
			},
			ProcessStoredField::SubtreeCommand => {
				stored.subtree_command = true;
			},
			ProcessStoredField::SubtreeError => {
				stored.subtree_error = true;
			},
			ProcessStoredField::SubtreeLog => {
				stored.subtree_log = true;
			},
			ProcessStoredField::SubtreeOutput => {
				stored.subtree_output = true;
			},
		}
	}

	fn apply_process_metadata_field(
		metadata: &mut tg::process::Metadata,
		field: ProcessMetadataField,
		value: &[u8],
	) -> tg::Result<()> {
		match field {
			ProcessMetadataField::NodeCommandCount => {
				metadata.node.command.count = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::NodeCommandDepth => {
				metadata.node.command.depth = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::NodeCommandSize => {
				metadata.node.command.size = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::NodeCommandSolvable => {
				metadata.node.command.solvable = Some(value.is_empty() || value[0] != 0);
			},
			ProcessMetadataField::NodeCommandSolved => {
				metadata.node.command.solved = Some(value.is_empty() || value[0] != 0);
			},

			ProcessMetadataField::NodeErrorCount => {
				metadata.node.error.count = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::NodeErrorDepth => {
				metadata.node.error.depth = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::NodeErrorSize => {
				metadata.node.error.size = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::NodeErrorSolvable => {
				metadata.node.error.solvable = Some(value.is_empty() || value[0] != 0);
			},
			ProcessMetadataField::NodeErrorSolved => {
				metadata.node.error.solved = Some(value.is_empty() || value[0] != 0);
			},

			ProcessMetadataField::NodeLogCount => {
				metadata.node.log.count = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::NodeLogDepth => {
				metadata.node.log.depth = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::NodeLogSize => {
				metadata.node.log.size = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::NodeLogSolvable => {
				metadata.node.log.solvable = Some(value.is_empty() || value[0] != 0);
			},
			ProcessMetadataField::NodeLogSolved => {
				metadata.node.log.solved = Some(value.is_empty() || value[0] != 0);
			},

			ProcessMetadataField::NodeOutputCount => {
				metadata.node.output.count = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::NodeOutputDepth => {
				metadata.node.output.depth = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::NodeOutputSize => {
				metadata.node.output.size = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::NodeOutputSolvable => {
				metadata.node.output.solvable = Some(value.is_empty() || value[0] != 0);
			},
			ProcessMetadataField::NodeOutputSolved => {
				metadata.node.output.solved = Some(value.is_empty() || value[0] != 0);
			},

			ProcessMetadataField::SubtreeCommandCount => {
				metadata.subtree.command.count = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::SubtreeCommandDepth => {
				metadata.subtree.command.depth = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::SubtreeCommandSize => {
				metadata.subtree.command.size = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::SubtreeCommandSolvable => {
				metadata.subtree.command.solvable = Some(value.is_empty() || value[0] != 0);
			},
			ProcessMetadataField::SubtreeCommandSolved => {
				metadata.subtree.command.solved = Some(value.is_empty() || value[0] != 0);
			},

			ProcessMetadataField::SubtreeCount => {
				metadata.subtree.count = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},

			ProcessMetadataField::SubtreeErrorCount => {
				metadata.subtree.error.count = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::SubtreeErrorDepth => {
				metadata.subtree.error.depth = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::SubtreeErrorSize => {
				metadata.subtree.error.size = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::SubtreeErrorSolvable => {
				metadata.subtree.error.solvable = Some(value.is_empty() || value[0] != 0);
			},
			ProcessMetadataField::SubtreeErrorSolved => {
				metadata.subtree.error.solved = Some(value.is_empty() || value[0] != 0);
			},

			ProcessMetadataField::SubtreeLogCount => {
				metadata.subtree.log.count = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::SubtreeLogDepth => {
				metadata.subtree.log.depth = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::SubtreeLogSize => {
				metadata.subtree.log.size = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::SubtreeLogSolvable => {
				metadata.subtree.log.solvable = Some(value.is_empty() || value[0] != 0);
			},
			ProcessMetadataField::SubtreeLogSolved => {
				metadata.subtree.log.solved = Some(value.is_empty() || value[0] != 0);
			},

			ProcessMetadataField::SubtreeOutputCount => {
				metadata.subtree.output.count = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::SubtreeOutputDepth => {
				metadata.subtree.output.depth = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::SubtreeOutputSize => {
				metadata.subtree.output.size = Some(
					varint::decode_uvarint(value)
						.ok_or_else(|| tg::error!("invalid field value"))?,
				);
			},
			ProcessMetadataField::SubtreeOutputSolvable => {
				metadata.subtree.output.solvable = Some(value.is_empty() || value[0] != 0);
			},
			ProcessMetadataField::SubtreeOutputSolved => {
				metadata.subtree.output.solved = Some(value.is_empty() || value[0] != 0);
			},
		}

		Ok(())
	}

	pub(super) async fn get_object_children_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		let bytes = id.to_bytes();
		let key = (KeyKind::ObjectChild.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object children"))?;

		let children = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::ObjectChild { child, .. } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(children)
	}

	pub(super) async fn get_object_parents_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		let bytes = id.to_bytes();
		let key = (KeyKind::ChildObject.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object parents"))?;

		let parents = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::ChildObject { object, .. } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(object)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(parents)
	}

	pub(super) async fn get_object_processes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, ProcessObjectKind)>> {
		let bytes = id.to_bytes();
		let key = (KeyKind::ObjectProcess.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object process parents"))?;

		let processes = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::ObjectProcess { kind, process, .. } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((process, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(processes)
	}

	pub(super) async fn get_process_children_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let bytes = id.to_bytes();
		let key = (KeyKind::ProcessChild.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process children"))?;

		let children = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::ProcessChild { child, .. } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(children)
	}

	pub(super) async fn get_process_parents_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let bytes = id.to_bytes();
		let key = (KeyKind::ChildProcess.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process parents"))?;

		let parents = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::ChildProcess { parent, .. } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(parent)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(parents)
	}

	pub(super) async fn get_process_objects_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<Vec<(tg::object::Id, ProcessObjectKind)>> {
		let bytes = id.to_bytes();
		let key = (KeyKind::ProcessObject.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process objects"))?;

		let objects = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::ProcessObject { kind, object, .. } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((object, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(objects)
	}
}
