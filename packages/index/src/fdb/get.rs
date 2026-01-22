use {
	super::{
		Index, Key, Kind, ObjectCoreField, ObjectField, ObjectMetadataField, ObjectStoredField,
		ProcessCoreField, ProcessField, ProcessMetadataField, ProcessStoredField,
	},
	crate::{Object, ObjectStored, Process, ProcessStored},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
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
				.map(|id| self.try_get_object_with_transaction(&txn, id)),
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
				.map(|id| self.try_get_process_with_transaction(&txn, id)),
		)
		.await?;

		Ok(outputs)
	}

	pub async fn try_get_object_with_transaction(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
	) -> tg::Result<Option<Object>> {
		let prefix = self.pack_tuple(&(Kind::Object.to_i32().unwrap(), id.to_bytes().as_ref()));
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};

		let entries = txn
			.get_ranges_keyvalues(range, true)
			.try_collect::<Vec<_>>()
			.await
			.map_err(|source| tg::error!(!source, "failed to scan object fields"))?;

		let exists_key = self.pack(&Key::Object {
			id: id.clone(),
			field: ObjectField::Core(ObjectCoreField::Exists),
		});
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
		let mut metadata = tg::object::Metadata::default();
		let mut stored = ObjectStored::default();

		for entry in entries {
			let Key::Object { field, .. } = self.unpack(entry.key())? else {
				return Err(tg::error!("unexpected key type"));
			};
			let value = entry.value();

			match field {
				ObjectField::Core(field) => match field {
					ObjectCoreField::Exists => {
						exists = true;
					},
					ObjectCoreField::TouchedAt => {
						let value = value
							.try_into()
							.map_err(|_| tg::error!("invalid touched_at"))?;
						touched_at = Some(i64::from_le_bytes(value));
					},
					ObjectCoreField::ReferenceCount | ObjectCoreField::CacheEntry => {},
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
			cache_entry: None,
			metadata,
			reference_count: 0,
			stored,
			touched_at,
		}))
	}

	pub async fn try_get_process_with_transaction(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
	) -> tg::Result<Option<Process>> {
		let prefix = self.pack_tuple(&(Kind::Process.to_i32().unwrap(), id.to_bytes().as_ref()));
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};

		let entries = txn
			.get_ranges_keyvalues(range, true)
			.try_collect::<Vec<_>>()
			.await
			.map_err(|source| tg::error!(!source, "failed to scan process fields"))?;

		let exists_key = self.pack(&Key::Process {
			id: id.clone(),
			field: ProcessField::Core(ProcessCoreField::Exists),
		});
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
		let mut metadata = tg::process::Metadata::default();
		let mut stored = ProcessStored::default();

		for entry in entries {
			let Key::Process { field, .. } = self.unpack(entry.key())? else {
				return Err(tg::error!("unexpected key type"));
			};
			let value = entry.value();

			match field {
				ProcessField::Core(field) => match field {
					ProcessCoreField::Exists => {
						exists = true;
					},
					ProcessCoreField::TouchedAt => {
						let value = value
							.try_into()
							.map_err(|_| tg::error!("invalid touched_at"))?;
						touched_at = Some(i64::from_le_bytes(value));
					},
					ProcessCoreField::ReferenceCount => {},
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
			reference_count: 0,
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
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid node size"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.size = value;
			},
			ObjectMetadataField::NodeSolvable => {
				metadata.node.solvable = true;
			},
			ObjectMetadataField::NodeSolved => {
				metadata.node.solved = true;
			},
			ObjectMetadataField::SubtreeCount => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid subtree count"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.count = Some(value);
			},
			ObjectMetadataField::SubtreeDepth => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid subtree depth"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.depth = Some(value);
			},
			ObjectMetadataField::SubtreeSize => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid subtree size"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.size = Some(value);
			},
			ObjectMetadataField::SubtreeSolvable => {
				metadata.subtree.solvable = Some(true);
			},
			ObjectMetadataField::SubtreeSolved => {
				metadata.subtree.solved = Some(true);
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
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.command.count = Some(value);
			},
			ProcessMetadataField::NodeCommandDepth => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.command.depth = Some(value);
			},
			ProcessMetadataField::NodeCommandSize => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.command.size = Some(value);
			},
			ProcessMetadataField::NodeCommandSolvable => {
				metadata.node.command.solvable = Some(true);
			},
			ProcessMetadataField::NodeCommandSolved => {
				metadata.node.command.solved = Some(true);
			},

			ProcessMetadataField::NodeErrorCount => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.error.count = Some(value);
			},
			ProcessMetadataField::NodeErrorDepth => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.error.depth = Some(value);
			},
			ProcessMetadataField::NodeErrorSize => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.error.size = Some(value);
			},
			ProcessMetadataField::NodeErrorSolvable => {
				metadata.node.error.solvable = Some(true);
			},
			ProcessMetadataField::NodeErrorSolved => {
				metadata.node.error.solved = Some(true);
			},

			ProcessMetadataField::NodeLogCount => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.log.count = Some(value);
			},
			ProcessMetadataField::NodeLogDepth => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.log.depth = Some(value);
			},
			ProcessMetadataField::NodeLogSize => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.log.size = Some(value);
			},
			ProcessMetadataField::NodeLogSolvable => {
				metadata.node.log.solvable = Some(true);
			},
			ProcessMetadataField::NodeLogSolved => {
				metadata.node.log.solved = Some(true);
			},

			ProcessMetadataField::NodeOutputCount => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.output.count = Some(value);
			},
			ProcessMetadataField::NodeOutputDepth => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.output.depth = Some(value);
			},
			ProcessMetadataField::NodeOutputSize => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.output.size = Some(value);
			},
			ProcessMetadataField::NodeOutputSolvable => {
				metadata.node.output.solvable = Some(true);
			},
			ProcessMetadataField::NodeOutputSolved => {
				metadata.node.output.solved = Some(true);
			},

			ProcessMetadataField::SubtreeCommandCount => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.command.count = Some(value);
			},
			ProcessMetadataField::SubtreeCommandDepth => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.command.depth = Some(value);
			},
			ProcessMetadataField::SubtreeCommandSize => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.command.size = Some(value);
			},
			ProcessMetadataField::SubtreeCommandSolvable => {
				metadata.subtree.command.solvable = Some(true);
			},
			ProcessMetadataField::SubtreeCommandSolved => {
				metadata.subtree.command.solved = Some(true);
			},

			ProcessMetadataField::SubtreeCount => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.count = Some(value);
			},

			ProcessMetadataField::SubtreeErrorCount => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.error.count = Some(value);
			},
			ProcessMetadataField::SubtreeErrorDepth => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.error.depth = Some(value);
			},
			ProcessMetadataField::SubtreeErrorSize => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.error.size = Some(value);
			},
			ProcessMetadataField::SubtreeErrorSolvable => {
				metadata.subtree.error.solvable = Some(true);
			},
			ProcessMetadataField::SubtreeErrorSolved => {
				metadata.subtree.error.solved = Some(true);
			},

			ProcessMetadataField::SubtreeLogCount => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.log.count = Some(value);
			},
			ProcessMetadataField::SubtreeLogDepth => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.log.depth = Some(value);
			},
			ProcessMetadataField::SubtreeLogSize => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.log.size = Some(value);
			},
			ProcessMetadataField::SubtreeLogSolvable => {
				metadata.subtree.log.solvable = Some(true);
			},
			ProcessMetadataField::SubtreeLogSolved => {
				metadata.subtree.log.solved = Some(true);
			},

			ProcessMetadataField::SubtreeOutputCount => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.output.count = Some(value);
			},
			ProcessMetadataField::SubtreeOutputDepth => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.output.depth = Some(value);
			},
			ProcessMetadataField::SubtreeOutputSize => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.output.size = Some(value);
			},
			ProcessMetadataField::SubtreeOutputSolvable => {
				metadata.subtree.output.solvable = Some(true);
			},
			ProcessMetadataField::SubtreeOutputSolved => {
				metadata.subtree.output.solved = Some(true);
			},
		}

		Ok(())
	}
}
