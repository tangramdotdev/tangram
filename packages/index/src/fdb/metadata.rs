use {
	super::{Index, Key, Kind, ObjectKind, ProcessKind},
	crate::{ObjectStored, ProcessStored},
	foundationdb as fdb,
	foundationdb_tuple::TuplePack as _,
	futures::TryStreamExt as _,
	num_traits::{FromPrimitive as _, ToPrimitive as _},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Check if the object exists by looking for the touched_at field.
		let touched_at_key = Key::ObjectField {
			id,
			field: ObjectKind::TouchedAt,
		}
		.pack_to_vec();
		let touched_at_value = txn
			.get(&touched_at_key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object touched_at"))?;
		if touched_at_value.is_none() {
			return Ok(None);
		}

		// Read metadata fields.
		let metadata = Self::read_object_metadata(&txn, id).await?;

		Ok(Some(metadata))
	}

	pub async fn try_get_object_metadata_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::object::Metadata>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let outputs = futures::future::try_join_all(ids.iter().map(|id| async {
			// Check if the object exists by looking for the touched_at field.
			let touched_at_key = Key::ObjectField {
				id,
				field: ObjectKind::TouchedAt,
			}
			.pack_to_vec();
			let touched_at_value = txn
				.get(&touched_at_key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get object touched_at"))?;
			if touched_at_value.is_none() {
				return Ok(None);
			}

			// Read metadata fields.
			let metadata = Self::read_object_metadata(&txn, id).await?;

			Ok::<_, tg::Error>(Some(metadata))
		}))
		.await?;

		Ok(outputs)
	}

	pub async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Check if the process exists by looking for the touched_at field.
		let touched_at_key = Key::ProcessField {
			id,
			field: ProcessKind::TouchedAt,
		}
		.pack_to_vec();
		let touched_at_value = txn
			.get(&touched_at_key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process touched_at"))?;
		if touched_at_value.is_none() {
			return Ok(None);
		}

		// Read metadata fields.
		let metadata = Self::read_process_metadata(&txn, id).await?;

		Ok(Some(metadata))
	}

	pub async fn try_get_process_metadata_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::Metadata>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let outputs = futures::future::try_join_all(ids.iter().map(|id| async {
			// Check if the process exists by looking for the touched_at field.
			let touched_at_key = Key::ProcessField {
				id,
				field: ProcessKind::TouchedAt,
			}
			.pack_to_vec();
			let touched_at_value = txn
				.get(&touched_at_key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get process touched_at"))?;
			if touched_at_value.is_none() {
				return Ok(None);
			}

			// Read metadata fields.
			let metadata = Self::read_process_metadata(&txn, id).await?;
			Ok::<_, tg::Error>(Some(metadata))
		}))
		.await?;

		Ok(outputs)
	}

	/// Read object metadata from a range of field keys.
	pub async fn read_object_metadata(
		txn: &fdb::Transaction,
		id: &tg::object::Id,
	) -> tg::Result<tg::object::Metadata> {
		let start = (
			Kind::Object.to_i32().unwrap(),
			id.to_bytes().as_ref(),
			i32::from(ObjectKind::METADATA_START),
		)
			.pack_to_vec();
		let end = (
			Kind::Object.to_i32().unwrap(),
			id.to_bytes().as_ref(),
			i32::from(ObjectKind::METADATA_END),
		)
			.pack_to_vec();

		let entries: Vec<_> = txn
			.get_ranges_keyvalues(
				fdb::RangeOption {
					begin: fdb::KeySelector::first_greater_or_equal(start),
					end: fdb::KeySelector::first_greater_or_equal(end),
					mode: fdb::options::StreamingMode::WantAll,
					..Default::default()
				},
				false,
			)
			.try_collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to scan object metadata"))?;

		let mut metadata = tg::object::Metadata::default();

		for entry in entries {
			let (_, _, field): (i32, Vec<u8>, i32) = foundationdb_tuple::unpack(entry.key())
				.map_err(|source| tg::error!(!source, "failed to unpack object metadata key"))?;
			let value = entry.value();

			match ObjectKind::from_i32(field) {
				Some(ObjectKind::MetadataNodeSize) => {
					let value = value
						.try_into()
						.map_err(|_| tg::error!("invalid node size"))?;
					let value = u64::from_le_bytes(value);
					metadata.node.size = value;
				},
				Some(ObjectKind::MetadataNodeSolvable) => {
					metadata.node.solvable = true;
				},
				Some(ObjectKind::MetadataNodeSolved) => {
					metadata.node.solved = true;
				},
				Some(ObjectKind::MetadataSubtreeCount) => {
					let value = value
						.try_into()
						.map_err(|_| tg::error!("invalid subtree count"))?;
					let value = u64::from_le_bytes(value);
					metadata.subtree.count = Some(value);
				},
				Some(ObjectKind::MetadataSubtreeDepth) => {
					let value = value
						.try_into()
						.map_err(|_| tg::error!("invalid subtree depth"))?;
					let value = u64::from_le_bytes(value);
					metadata.subtree.depth = Some(value);
				},
				Some(ObjectKind::MetadataSubtreeSize) => {
					let value = value
						.try_into()
						.map_err(|_| tg::error!("invalid subtree size"))?;
					let value = u64::from_le_bytes(value);
					metadata.subtree.size = Some(value);
				},
				Some(ObjectKind::MetadataSubtreeSolvable) => {
					metadata.subtree.solvable = Some(true);
				},
				Some(ObjectKind::MetadataSubtreeSolved) => {
					metadata.subtree.solved = Some(true);
				},
				_ => {},
			}
		}

		Ok(metadata)
	}

	/// Read object stored fields from a range of field keys.
	pub async fn read_object_stored(
		txn: &fdb::Transaction,
		id: &tg::object::Id,
	) -> tg::Result<ObjectStored> {
		let start = (
			Kind::Object.to_i32().unwrap(),
			id.to_bytes().as_ref(),
			i32::from(ObjectKind::STORED_START),
		)
			.pack_to_vec();
		let end = (
			Kind::Object.to_i32().unwrap(),
			id.to_bytes().as_ref(),
			i32::from(ObjectKind::STORED_END),
		)
			.pack_to_vec();

		let entries: Vec<_> = txn
			.get_ranges_keyvalues(
				fdb::RangeOption {
					begin: fdb::KeySelector::first_greater_or_equal(start),
					end: fdb::KeySelector::first_greater_or_equal(end),
					mode: fdb::options::StreamingMode::WantAll,
					..Default::default()
				},
				false,
			)
			.try_collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to scan object stored"))?;

		let mut stored = ObjectStored::default();

		for entry in entries {
			let (_, _, field): (i32, Vec<u8>, i32) = foundationdb_tuple::unpack(entry.key())
				.map_err(|source| tg::error!(!source, "failed to unpack object stored key"))?;

			if ObjectKind::from_i32(field) == Some(ObjectKind::StoredSubtree) {
				stored.subtree = true;
			}
		}

		Ok(stored)
	}

	/// Read object stored and metadata from a combined range scan.
	pub async fn read_object_stored_and_metadata(
		txn: &fdb::Transaction,
		id: &tg::object::Id,
	) -> tg::Result<(ObjectStored, tg::object::Metadata)> {
		let start = (
			Kind::Object.to_i32().unwrap(),
			id.to_bytes().as_ref(),
			i32::from(ObjectKind::STORED_START),
		)
			.pack_to_vec();
		let end = (
			Kind::Object.to_i32().unwrap(),
			id.to_bytes().as_ref(),
			i32::from(ObjectKind::METADATA_END),
		)
			.pack_to_vec();

		let entries: Vec<_> = txn
			.get_ranges_keyvalues(
				fdb::RangeOption {
					begin: fdb::KeySelector::first_greater_or_equal(start),
					end: fdb::KeySelector::first_greater_or_equal(end),
					mode: fdb::options::StreamingMode::WantAll,
					..Default::default()
				},
				false,
			)
			.try_collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to scan object stored and metadata"))?;

		let mut stored = ObjectStored::default();
		let mut metadata = tg::object::Metadata::default();

		for entry in entries {
			let (_, _, field): (i32, Vec<u8>, i32) = foundationdb_tuple::unpack(entry.key())
				.map_err(|source| tg::error!(!source, "failed to unpack object field key"))?;
			let value = entry.value();

			match ObjectKind::from_i32(field) {
				Some(ObjectKind::StoredSubtree) => {
					stored.subtree = true;
				},
				Some(ObjectKind::MetadataNodeSize) => {
					let value = value
						.try_into()
						.map_err(|_| tg::error!("invalid node size"))?;
					let value = u64::from_le_bytes(value);
					metadata.node.size = value;
				},
				Some(ObjectKind::MetadataNodeSolvable) => {
					metadata.node.solvable = true;
				},
				Some(ObjectKind::MetadataNodeSolved) => {
					metadata.node.solved = true;
				},
				Some(ObjectKind::MetadataSubtreeCount) => {
					let value = value
						.try_into()
						.map_err(|_| tg::error!("invalid subtree count"))?;
					let value = u64::from_le_bytes(value);
					metadata.subtree.count = Some(value);
				},
				Some(ObjectKind::MetadataSubtreeDepth) => {
					let value = value
						.try_into()
						.map_err(|_| tg::error!("invalid subtree depth"))?;
					let value = u64::from_le_bytes(value);
					metadata.subtree.depth = Some(value);
				},
				Some(ObjectKind::MetadataSubtreeSize) => {
					let value = value
						.try_into()
						.map_err(|_| tg::error!("invalid subtree size"))?;
					let value = u64::from_le_bytes(value);
					metadata.subtree.size = Some(value);
				},
				Some(ObjectKind::MetadataSubtreeSolvable) => {
					metadata.subtree.solvable = Some(true);
				},
				Some(ObjectKind::MetadataSubtreeSolved) => {
					metadata.subtree.solved = Some(true);
				},
				_ => {},
			}
		}

		Ok((stored, metadata))
	}

	/// Read process metadata from a range of field keys.
	pub async fn read_process_metadata(
		txn: &fdb::Transaction,
		id: &tg::process::Id,
	) -> tg::Result<tg::process::Metadata> {
		let start = (
			Kind::Process.to_i32().unwrap(),
			id.to_bytes().as_ref(),
			i32::from(ProcessKind::METADATA_START),
		)
			.pack_to_vec();
		let end = (
			Kind::Process.to_i32().unwrap(),
			id.to_bytes().as_ref(),
			i32::from(ProcessKind::METADATA_END),
		)
			.pack_to_vec();

		let entries: Vec<_> = txn
			.get_ranges_keyvalues(
				fdb::RangeOption {
					begin: fdb::KeySelector::first_greater_or_equal(start),
					end: fdb::KeySelector::first_greater_or_equal(end),
					mode: fdb::options::StreamingMode::WantAll,
					..Default::default()
				},
				false,
			)
			.try_collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to scan process metadata"))?;

		let mut metadata = tg::process::Metadata::default();

		for entry in entries {
			let (_, _, field): (i32, Vec<u8>, i32) = foundationdb_tuple::unpack(entry.key())
				.map_err(|source| tg::error!(!source, "failed to unpack process metadata key"))?;
			let value = entry.value();

			Self::apply_process_metadata_field(&mut metadata, field, value)?;
		}

		Ok(metadata)
	}

	/// Read process stored fields from a range of field keys.
	pub async fn read_process_stored(
		txn: &fdb::Transaction,
		id: &tg::process::Id,
	) -> tg::Result<ProcessStored> {
		let start = (
			Kind::Process.to_i32().unwrap(),
			id.to_bytes().as_ref(),
			i32::from(ProcessKind::STORED_START),
		)
			.pack_to_vec();
		let end = (
			Kind::Process.to_i32().unwrap(),
			id.to_bytes().as_ref(),
			i32::from(ProcessKind::STORED_END),
		)
			.pack_to_vec();

		let entries: Vec<_> = txn
			.get_ranges_keyvalues(
				fdb::RangeOption {
					begin: fdb::KeySelector::first_greater_or_equal(start),
					end: fdb::KeySelector::first_greater_or_equal(end),
					mode: fdb::options::StreamingMode::WantAll,
					..Default::default()
				},
				false,
			)
			.try_collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to scan process stored"))?;

		let mut stored = ProcessStored::default();

		for entry in entries {
			let (_, _, field): (i32, Vec<u8>, i32) = foundationdb_tuple::unpack(entry.key())
				.map_err(|source| tg::error!(!source, "failed to unpack process stored key"))?;

			match ProcessKind::from_i32(field) {
				Some(ProcessKind::StoredNodeCommand) => stored.node_command = true,
				Some(ProcessKind::StoredNodeError) => stored.node_error = true,
				Some(ProcessKind::StoredNodeLog) => stored.node_log = true,
				Some(ProcessKind::StoredNodeOutput) => stored.node_output = true,
				Some(ProcessKind::StoredSubtree) => stored.subtree = true,
				Some(ProcessKind::StoredSubtreeCommand) => stored.subtree_command = true,
				Some(ProcessKind::StoredSubtreeError) => stored.subtree_error = true,
				Some(ProcessKind::StoredSubtreeLog) => stored.subtree_log = true,
				Some(ProcessKind::StoredSubtreeOutput) => stored.subtree_output = true,
				_ => {},
			}
		}

		Ok(stored)
	}

	/// Read process stored and metadata from a combined range scan.
	pub async fn read_process_stored_and_metadata(
		txn: &fdb::Transaction,
		id: &tg::process::Id,
	) -> tg::Result<(ProcessStored, tg::process::Metadata)> {
		let start = (
			Kind::Process.to_i32().unwrap(),
			id.to_bytes().as_ref(),
			i32::from(ProcessKind::STORED_START),
		)
			.pack_to_vec();
		let end = (
			Kind::Process.to_i32().unwrap(),
			id.to_bytes().as_ref(),
			i32::from(ProcessKind::METADATA_END),
		)
			.pack_to_vec();

		let entries: Vec<_> = txn
			.get_ranges_keyvalues(
				fdb::RangeOption {
					begin: fdb::KeySelector::first_greater_or_equal(start),
					end: fdb::KeySelector::first_greater_or_equal(end),
					mode: fdb::options::StreamingMode::WantAll,
					..Default::default()
				},
				false,
			)
			.try_collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to scan process stored and metadata"))?;

		let mut stored = ProcessStored::default();
		let mut metadata = tg::process::Metadata::default();

		for entry in entries {
			let (_, _, field): (i32, Vec<u8>, i32) = foundationdb_tuple::unpack(entry.key())
				.map_err(|source| tg::error!(!source, "failed to unpack process field key"))?;
			let value = entry.value();

			// Stored fields.
			match ProcessKind::from_i32(field) {
				Some(ProcessKind::StoredNodeCommand) => {
					stored.node_command = true;
					continue;
				},
				Some(ProcessKind::StoredNodeError) => {
					stored.node_error = true;
					continue;
				},
				Some(ProcessKind::StoredNodeLog) => {
					stored.node_log = true;
					continue;
				},
				Some(ProcessKind::StoredNodeOutput) => {
					stored.node_output = true;
					continue;
				},
				Some(ProcessKind::StoredSubtree) => {
					stored.subtree = true;
					continue;
				},
				Some(ProcessKind::StoredSubtreeCommand) => {
					stored.subtree_command = true;
					continue;
				},
				Some(ProcessKind::StoredSubtreeError) => {
					stored.subtree_error = true;
					continue;
				},
				Some(ProcessKind::StoredSubtreeLog) => {
					stored.subtree_log = true;
					continue;
				},
				Some(ProcessKind::StoredSubtreeOutput) => {
					stored.subtree_output = true;
					continue;
				},
				_ => {},
			}

			// Metadata fields.
			Self::apply_process_metadata_field(&mut metadata, field, value)?;
		}

		Ok((stored, metadata))
	}

	/// Apply a process metadata field value to the metadata struct.
	fn apply_process_metadata_field(
		metadata: &mut tg::process::Metadata,
		field: i32,
		value: &[u8],
	) -> tg::Result<()> {
		match ProcessKind::from_i32(field) {
			Some(ProcessKind::MetadataNodeCommandCount) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.command.count = Some(value);
			},
			Some(ProcessKind::MetadataNodeCommandDepth) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.command.depth = Some(value);
			},
			Some(ProcessKind::MetadataNodeCommandSize) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.command.size = Some(value);
			},
			Some(ProcessKind::MetadataNodeCommandSolvable) => {
				metadata.node.command.solvable = Some(true);
			},
			Some(ProcessKind::MetadataNodeCommandSolved) => {
				metadata.node.command.solved = Some(true);
			},

			Some(ProcessKind::MetadataNodeErrorCount) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.error.count = Some(value);
			},
			Some(ProcessKind::MetadataNodeErrorDepth) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.error.depth = Some(value);
			},
			Some(ProcessKind::MetadataNodeErrorSize) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.error.size = Some(value);
			},
			Some(ProcessKind::MetadataNodeErrorSolvable) => {
				metadata.node.error.solvable = Some(true);
			},
			Some(ProcessKind::MetadataNodeErrorSolved) => {
				metadata.node.error.solved = Some(true);
			},

			Some(ProcessKind::MetadataNodeLogCount) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.log.count = Some(value);
			},
			Some(ProcessKind::MetadataNodeLogDepth) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.log.depth = Some(value);
			},
			Some(ProcessKind::MetadataNodeLogSize) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.log.size = Some(value);
			},
			Some(ProcessKind::MetadataNodeLogSolvable) => {
				metadata.node.log.solvable = Some(true);
			},
			Some(ProcessKind::MetadataNodeLogSolved) => {
				metadata.node.log.solved = Some(true);
			},

			Some(ProcessKind::MetadataNodeOutputCount) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.output.count = Some(value);
			},
			Some(ProcessKind::MetadataNodeOutputDepth) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.output.depth = Some(value);
			},
			Some(ProcessKind::MetadataNodeOutputSize) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.node.output.size = Some(value);
			},
			Some(ProcessKind::MetadataNodeOutputSolvable) => {
				metadata.node.output.solvable = Some(true);
			},
			Some(ProcessKind::MetadataNodeOutputSolved) => {
				metadata.node.output.solved = Some(true);
			},

			Some(ProcessKind::MetadataSubtreeCommandCount) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.command.count = Some(value);
			},
			Some(ProcessKind::MetadataSubtreeCommandDepth) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.command.depth = Some(value);
			},
			Some(ProcessKind::MetadataSubtreeCommandSize) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.command.size = Some(value);
			},
			Some(ProcessKind::MetadataSubtreeCommandSolvable) => {
				metadata.subtree.command.solvable = Some(true);
			},
			Some(ProcessKind::MetadataSubtreeCommandSolved) => {
				metadata.subtree.command.solved = Some(true);
			},

			Some(ProcessKind::MetadataSubtreeCount) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.count = Some(value);
			},

			Some(ProcessKind::MetadataSubtreeErrorCount) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.error.count = Some(value);
			},
			Some(ProcessKind::MetadataSubtreeErrorDepth) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.error.depth = Some(value);
			},
			Some(ProcessKind::MetadataSubtreeErrorSize) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.error.size = Some(value);
			},
			Some(ProcessKind::MetadataSubtreeErrorSolvable) => {
				metadata.subtree.error.solvable = Some(true);
			},
			Some(ProcessKind::MetadataSubtreeErrorSolved) => {
				metadata.subtree.error.solved = Some(true);
			},

			Some(ProcessKind::MetadataSubtreeLogCount) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.log.count = Some(value);
			},
			Some(ProcessKind::MetadataSubtreeLogDepth) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.log.depth = Some(value);
			},
			Some(ProcessKind::MetadataSubtreeLogSize) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.log.size = Some(value);
			},
			Some(ProcessKind::MetadataSubtreeLogSolvable) => {
				metadata.subtree.log.solvable = Some(true);
			},
			Some(ProcessKind::MetadataSubtreeLogSolved) => {
				metadata.subtree.log.solved = Some(true);
			},

			Some(ProcessKind::MetadataSubtreeOutputCount) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.output.count = Some(value);
			},
			Some(ProcessKind::MetadataSubtreeOutputDepth) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.output.depth = Some(value);
			},
			Some(ProcessKind::MetadataSubtreeOutputSize) => {
				let value = value
					.try_into()
					.map_err(|_| tg::error!("invalid field value"))?;
				let value = u64::from_le_bytes(value);
				metadata.subtree.output.size = Some(value);
			},
			Some(ProcessKind::MetadataSubtreeOutputSolvable) => {
				metadata.subtree.output.solvable = Some(true);
			},
			Some(ProcessKind::MetadataSubtreeOutputSolved) => {
				metadata.subtree.output.solved = Some(true);
			},

			_ => {},
		}

		Ok(())
	}
}
