use {
	super::{
		CacheEntryCoreField, CacheEntryField, Index, ItemKind, Key, KeyKind, ObjectCoreField,
		ObjectField, ObjectMetadataField, ObjectStoredField, ProcessCoreField, ProcessField,
		ProcessMetadataField, ProcessStoredField, Update,
	},
	crate::{ProcessObjectKind, PutArg, PutCacheEntryArg, PutObjectArg, PutProcessArg},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_util::varint,
};

impl Index {
	pub async fn put(&self, arg: PutArg) -> tg::Result<()> {
		self.database
			.run(|txn, _| {
				let this = self.clone();
				let arg = arg.clone();
				async move {
					for cache_entry in &arg.cache_entries {
						this.put_cache_entry(&txn, cache_entry);
					}
					for object in &arg.objects {
						this.put_object(&txn, object);
					}
					for process in &arg.processes {
						this.put_process(&txn, process);
					}
					Ok::<_, fdb::FdbBindingError>(())
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to put"))
	}

	fn put_cache_entry(&self, txn: &fdb::Transaction, arg: &PutCacheEntryArg) {
		let id = &arg.id;

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = self.pack(&Key::CacheEntry {
			id: id.clone(),
			field: CacheEntryField::Core(CacheEntryCoreField::Exists),
		});
		txn.set(&key, &[]);

		let key = self.pack(&Key::CacheEntry {
			id: id.clone(),
			field: CacheEntryField::Core(CacheEntryCoreField::TouchedAt),
		});
		txn.atomic_op(
			&key,
			&arg.touched_at.to_le_bytes(),
			fdb::options::MutationType::Max,
		);

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = self.pack(&Key::Clean {
			touched_at: arg.touched_at,
			kind: ItemKind::CacheEntry,
			id: tg::Either::Left(arg.id.clone().into()),
		});
		txn.set(&key, &[]);
	}

	fn put_object(&self, txn: &fdb::Transaction, arg: &PutObjectArg) {
		let id = &arg.id;

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = self.pack(&Key::Object {
			id: id.clone(),
			field: ObjectField::Core(ObjectCoreField::Exists),
		});
		txn.set(&key, &[]);

		let key = self.pack(&Key::Object {
			id: id.clone(),
			field: ObjectField::Core(ObjectCoreField::TouchedAt),
		});
		txn.atomic_op(
			&key,
			&arg.touched_at.to_le_bytes(),
			fdb::options::MutationType::Max,
		);

		if let Some(cache_entry) = &arg.cache_entry {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Core(ObjectCoreField::CacheEntry),
			});
			txn.set(&key, cache_entry.to_bytes().as_ref());
		}

		// Always write node.size, node.solvable, and node.solved.
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = self.pack(&Key::Object {
			id: id.clone(),
			field: ObjectField::Metadata(ObjectMetadataField::NodeSize),
		});
		txn.set(&key, &varint::encode_uvarint(arg.metadata.node.size));

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = self.pack(&Key::Object {
			id: id.clone(),
			field: ObjectField::Metadata(ObjectMetadataField::NodeSolvable),
		});
		txn.set(&key, &[u8::from(arg.metadata.node.solvable)]);

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = self.pack(&Key::Object {
			id: id.clone(),
			field: ObjectField::Metadata(ObjectMetadataField::NodeSolved),
		});
		txn.set(&key, &[u8::from(arg.metadata.node.solved)]);

		if let Some(count) = arg.metadata.subtree.count {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeCount),
			});
			txn.set(&key, &varint::encode_uvarint(count));
		}
		if let Some(depth) = arg.metadata.subtree.depth {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeDepth),
			});
			txn.set(&key, &varint::encode_uvarint(depth));
		}
		if let Some(size) = arg.metadata.subtree.size {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeSize),
			});
			txn.set(&key, &varint::encode_uvarint(size));
		}
		if let Some(solvable) = arg.metadata.subtree.solvable {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeSolvable),
			});
			txn.set(&key, &[u8::from(solvable)]);
		}
		if let Some(solved) = arg.metadata.subtree.solved {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeSolved),
			});
			txn.set(&key, &[u8::from(solved)]);
		}

		if arg.stored.subtree {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Stored(ObjectStoredField::Subtree),
			});
			txn.set(&key, &[]);
		}

		for child in &arg.children {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::ObjectChild {
				object: id.clone(),
				child: child.clone(),
			});
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::ChildObject {
				child: child.clone(),
				object: id.clone(),
			});
			txn.set(&key, &[]);
		}

		if let Some(cache_entry) = &arg.cache_entry {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::ObjectCacheEntry {
				object: id.clone(),
				cache_entry: cache_entry.clone(),
			});
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::CacheEntryObject {
				cache_entry: cache_entry.clone(),
				object: id.clone(),
			});
			txn.set(&key, &[]);
		}

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = self.pack(&Key::Clean {
			touched_at: arg.touched_at,
			kind: ItemKind::Object,
			id: tg::Either::Left(id.clone()),
		});
		txn.set(&key, &[]);

		self.enqueue_put_update(txn, &tg::Either::Left(id.clone()));
	}

	fn put_process(&self, txn: &fdb::Transaction, arg: &PutProcessArg) {
		let id = &arg.id;

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = self.pack(&Key::Process {
			id: id.clone(),
			field: ProcessField::Core(ProcessCoreField::Exists),
		});
		txn.set(&key, &[]);

		let key = self.pack(&Key::Process {
			id: id.clone(),
			field: ProcessField::Core(ProcessCoreField::TouchedAt),
		});
		txn.atomic_op(
			&key,
			&arg.touched_at.to_le_bytes(),
			fdb::options::MutationType::Max,
		);

		self.put_process_object_metadata(
			txn,
			id,
			&arg.metadata.node.command,
			ProcessObjectKind::Command,
			false,
		);
		self.put_process_object_metadata(
			txn,
			id,
			&arg.metadata.node.error,
			ProcessObjectKind::Error,
			false,
		);
		self.put_process_object_metadata(
			txn,
			id,
			&arg.metadata.node.log,
			ProcessObjectKind::Log,
			false,
		);
		self.put_process_object_metadata(
			txn,
			id,
			&arg.metadata.node.output,
			ProcessObjectKind::Output,
			false,
		);

		if let Some(count) = arg.metadata.subtree.count {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Metadata(ProcessMetadataField::SubtreeCount),
			});
			txn.set(&key, &varint::encode_uvarint(count));
		}
		self.put_process_object_metadata(
			txn,
			id,
			&arg.metadata.subtree.command,
			ProcessObjectKind::Command,
			true,
		);
		self.put_process_object_metadata(
			txn,
			id,
			&arg.metadata.subtree.error,
			ProcessObjectKind::Error,
			true,
		);
		self.put_process_object_metadata(
			txn,
			id,
			&arg.metadata.subtree.log,
			ProcessObjectKind::Log,
			true,
		);
		self.put_process_object_metadata(
			txn,
			id,
			&arg.metadata.subtree.output,
			ProcessObjectKind::Output,
			true,
		);

		if arg.stored.node_command {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::NodeCommand),
			});
			txn.set(&key, &[]);
		}
		if arg.stored.node_error {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::NodeError),
			});
			txn.set(&key, &[]);
		}
		if arg.stored.node_log {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::NodeLog),
			});
			txn.set(&key, &[]);
		}
		if arg.stored.node_output {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::NodeOutput),
			});
			txn.set(&key, &[]);
		}
		if arg.stored.subtree {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::Subtree),
			});
			txn.set(&key, &[]);
		}
		if arg.stored.subtree_command {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::SubtreeCommand),
			});
			txn.set(&key, &[]);
		}
		if arg.stored.subtree_error {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::SubtreeError),
			});
			txn.set(&key, &[]);
		}
		if arg.stored.subtree_log {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::SubtreeLog),
			});
			txn.set(&key, &[]);
		}
		if arg.stored.subtree_output {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::SubtreeOutput),
			});
			txn.set(&key, &[]);
		}

		for child in &arg.children {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::ProcessChild {
				process: id.clone(),
				child: child.clone(),
			});
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::ChildProcess {
				child: child.clone(),
				parent: id.clone(),
			});
			txn.set(&key, &[]);
		}

		for (object, kind) in &arg.objects {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::ProcessObject {
				process: id.clone(),
				kind: *kind,
				object: object.clone(),
			});
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::ObjectProcess {
				object: object.clone(),
				kind: *kind,
				process: id.clone(),
			});
			txn.set(&key, &[]);
		}

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = self.pack(&Key::Clean {
			touched_at: arg.touched_at,
			kind: ItemKind::Process,
			id: tg::Either::Right(id.clone()),
		});
		txn.set(&key, &[]);

		self.enqueue_put_update(txn, &tg::Either::Right(id.clone()));
	}

	fn enqueue_put_update(
		&self,
		txn: &fdb::Transaction,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
	) {
		// Write the Update key with Put value.
		let key = self.pack(&Key::Update { id: id.clone() });
		let value = Update::Put.serialize().unwrap();
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.set(&key, &value);

		// Write UpdateVersion for queue ordering. Update key has the authoritative value.
		let id_bytes = match &id {
			tg::Either::Left(id) => id.to_bytes(),
			tg::Either::Right(id) => id.to_bytes(),
		};
		let key = self.pack_with_versionstamp(&(
			KeyKind::UpdateVersion.to_i32().unwrap(),
			fdbt::Versionstamp::incomplete(0),
			id_bytes.as_ref(),
		));
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.atomic_op(&key, &[], fdb::options::MutationType::SetVersionstampedKey);
	}

	fn put_process_object_metadata(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		metadata: &tg::object::metadata::Subtree,
		kind: ProcessObjectKind,
		subtree: bool,
	) {
		if let Some(count) = metadata.count {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Metadata(ProcessMetadataField::from_object_metadata_field(
					ObjectSubtreeMetadataField::Count,
					kind,
					subtree,
				)),
			});
			txn.set(&key, &varint::encode_uvarint(count));
		}
		if let Some(depth) = metadata.depth {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Metadata(ProcessMetadataField::from_object_metadata_field(
					ObjectSubtreeMetadataField::Depth,
					kind,
					subtree,
				)),
			});
			txn.set(&key, &varint::encode_uvarint(depth));
		}
		if let Some(size) = metadata.size {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Metadata(ProcessMetadataField::from_object_metadata_field(
					ObjectSubtreeMetadataField::Size,
					kind,
					subtree,
				)),
			});
			txn.set(&key, &varint::encode_uvarint(size));
		}
		if let Some(solvable) = metadata.solvable {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Metadata(ProcessMetadataField::from_object_metadata_field(
					ObjectSubtreeMetadataField::Solvable,
					kind,
					subtree,
				)),
			});
			txn.set(&key, &[u8::from(solvable)]);
		}
		if let Some(solved) = metadata.solved {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Metadata(ProcessMetadataField::from_object_metadata_field(
					ObjectSubtreeMetadataField::Solved,
					kind,
					subtree,
				)),
			});
			txn.set(&key, &[u8::from(solved)]);
		}
	}
}

enum ObjectSubtreeMetadataField {
	Count,
	Depth,
	Size,
	Solvable,
	Solved,
}

impl ProcessMetadataField {
	fn from_object_metadata_field(
		field: ObjectSubtreeMetadataField,
		kind: ProcessObjectKind,
		subtree: bool,
	) -> Self {
		match (kind, subtree, field) {
			(ProcessObjectKind::Command, false, ObjectSubtreeMetadataField::Count) => {
				Self::NodeCommandCount
			},
			(ProcessObjectKind::Command, false, ObjectSubtreeMetadataField::Depth) => {
				Self::NodeCommandDepth
			},
			(ProcessObjectKind::Command, false, ObjectSubtreeMetadataField::Size) => {
				Self::NodeCommandSize
			},
			(ProcessObjectKind::Command, false, ObjectSubtreeMetadataField::Solvable) => {
				Self::NodeCommandSolvable
			},
			(ProcessObjectKind::Command, false, ObjectSubtreeMetadataField::Solved) => {
				Self::NodeCommandSolved
			},
			(ProcessObjectKind::Error, false, ObjectSubtreeMetadataField::Count) => {
				Self::NodeErrorCount
			},
			(ProcessObjectKind::Error, false, ObjectSubtreeMetadataField::Depth) => {
				Self::NodeErrorDepth
			},
			(ProcessObjectKind::Error, false, ObjectSubtreeMetadataField::Size) => {
				Self::NodeErrorSize
			},
			(ProcessObjectKind::Error, false, ObjectSubtreeMetadataField::Solvable) => {
				Self::NodeErrorSolvable
			},
			(ProcessObjectKind::Error, false, ObjectSubtreeMetadataField::Solved) => {
				Self::NodeErrorSolved
			},
			(ProcessObjectKind::Log, false, ObjectSubtreeMetadataField::Count) => {
				Self::NodeLogCount
			},
			(ProcessObjectKind::Log, false, ObjectSubtreeMetadataField::Depth) => {
				Self::NodeLogDepth
			},
			(ProcessObjectKind::Log, false, ObjectSubtreeMetadataField::Size) => Self::NodeLogSize,
			(ProcessObjectKind::Log, false, ObjectSubtreeMetadataField::Solvable) => {
				Self::NodeLogSolvable
			},
			(ProcessObjectKind::Log, false, ObjectSubtreeMetadataField::Solved) => {
				Self::NodeLogSolved
			},
			(ProcessObjectKind::Output, false, ObjectSubtreeMetadataField::Count) => {
				Self::NodeOutputCount
			},
			(ProcessObjectKind::Output, false, ObjectSubtreeMetadataField::Depth) => {
				Self::NodeOutputDepth
			},
			(ProcessObjectKind::Output, false, ObjectSubtreeMetadataField::Size) => {
				Self::NodeOutputSize
			},
			(ProcessObjectKind::Output, false, ObjectSubtreeMetadataField::Solvable) => {
				Self::NodeOutputSolvable
			},
			(ProcessObjectKind::Output, false, ObjectSubtreeMetadataField::Solved) => {
				Self::NodeOutputSolved
			},
			(ProcessObjectKind::Command, true, ObjectSubtreeMetadataField::Count) => {
				Self::SubtreeCommandCount
			},
			(ProcessObjectKind::Command, true, ObjectSubtreeMetadataField::Depth) => {
				Self::SubtreeCommandDepth
			},
			(ProcessObjectKind::Command, true, ObjectSubtreeMetadataField::Size) => {
				Self::SubtreeCommandSize
			},
			(ProcessObjectKind::Command, true, ObjectSubtreeMetadataField::Solvable) => {
				Self::SubtreeCommandSolvable
			},
			(ProcessObjectKind::Command, true, ObjectSubtreeMetadataField::Solved) => {
				Self::SubtreeCommandSolved
			},
			(ProcessObjectKind::Error, true, ObjectSubtreeMetadataField::Count) => {
				Self::SubtreeErrorCount
			},
			(ProcessObjectKind::Error, true, ObjectSubtreeMetadataField::Depth) => {
				Self::SubtreeErrorDepth
			},
			(ProcessObjectKind::Error, true, ObjectSubtreeMetadataField::Size) => {
				Self::SubtreeErrorSize
			},
			(ProcessObjectKind::Error, true, ObjectSubtreeMetadataField::Solvable) => {
				Self::SubtreeErrorSolvable
			},
			(ProcessObjectKind::Error, true, ObjectSubtreeMetadataField::Solved) => {
				Self::SubtreeErrorSolved
			},
			(ProcessObjectKind::Log, true, ObjectSubtreeMetadataField::Count) => {
				Self::SubtreeLogCount
			},
			(ProcessObjectKind::Log, true, ObjectSubtreeMetadataField::Depth) => {
				Self::SubtreeLogDepth
			},
			(ProcessObjectKind::Log, true, ObjectSubtreeMetadataField::Size) => {
				Self::SubtreeLogSize
			},
			(ProcessObjectKind::Log, true, ObjectSubtreeMetadataField::Solvable) => {
				Self::SubtreeLogSolvable
			},
			(ProcessObjectKind::Log, true, ObjectSubtreeMetadataField::Solved) => {
				Self::SubtreeLogSolved
			},
			(ProcessObjectKind::Output, true, ObjectSubtreeMetadataField::Count) => {
				Self::SubtreeOutputCount
			},
			(ProcessObjectKind::Output, true, ObjectSubtreeMetadataField::Depth) => {
				Self::SubtreeOutputDepth
			},
			(ProcessObjectKind::Output, true, ObjectSubtreeMetadataField::Size) => {
				Self::SubtreeOutputSize
			},
			(ProcessObjectKind::Output, true, ObjectSubtreeMetadataField::Solvable) => {
				Self::SubtreeOutputSolvable
			},
			(ProcessObjectKind::Output, true, ObjectSubtreeMetadataField::Solved) => {
				Self::SubtreeOutputSolved
			},
		}
	}
}
