use {
	super::{
		CacheEntryCoreField, CacheEntryField, Index, Key, ObjectCoreField, ObjectField,
		ObjectMetadataField, ObjectStoredField, ProcessCoreField, ProcessField,
		ProcessMetadataField, ProcessStoredField, TagCoreField, TagField,
	},
	crate::{ProcessObjectKind, PutArg, PutCacheEntryArg, PutObjectArg, PutProcessArg, PutTagArg},
	foundationdb as fdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put(&self, arg: PutArg) -> tg::Result<()> {
		let this = self.clone();
		self.database
			.run(|txn, _| {
				let this = this.clone();
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
					for tag in &arg.tags {
						this.put_tag(&txn, tag);
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
		let exists_key = self.pack(&Key::CacheEntry {
			id: id.clone(),
			field: CacheEntryField::Core(CacheEntryCoreField::Exists),
		});
		txn.set(&exists_key, &[]);

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let touched_at_key = self.pack(&Key::CacheEntry {
			id: id.clone(),
			field: CacheEntryField::Core(CacheEntryCoreField::TouchedAt),
		});
		txn.atomic_op(
			&touched_at_key,
			&arg.touched_at.to_le_bytes(),
			fdb::options::MutationType::Max,
		);
	}

	fn put_object(&self, txn: &fdb::Transaction, arg: &PutObjectArg) {
		let id = &arg.id;

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let exists_key = self.pack(&Key::Object {
			id: id.clone(),
			field: ObjectField::Core(ObjectCoreField::Exists),
		});
		txn.set(&exists_key, &[]);

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let touched_at_key = self.pack(&Key::Object {
			id: id.clone(),
			field: ObjectField::Core(ObjectCoreField::TouchedAt),
		});
		txn.atomic_op(
			&touched_at_key,
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

		if arg.metadata.node.size != 0 {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::NodeSize),
			});
			txn.set(&key, &arg.metadata.node.size.to_le_bytes());
		}
		if arg.metadata.node.solvable {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::NodeSolvable),
			});
			txn.set(&key, &[]);
		}
		if arg.metadata.node.solved {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::NodeSolved),
			});
			txn.set(&key, &[]);
		}

		if let Some(count) = arg.metadata.subtree.count {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeCount),
			});
			txn.set(&key, &count.to_le_bytes());
		}
		if let Some(depth) = arg.metadata.subtree.depth {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeDepth),
			});
			txn.set(&key, &depth.to_le_bytes());
		}
		if let Some(size) = arg.metadata.subtree.size {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeSize),
			});
			txn.set(&key, &size.to_le_bytes());
		}
		if let Some(true) = arg.metadata.subtree.solvable {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeSolvable),
			});
			txn.set(&key, &[]);
		}
		if let Some(true) = arg.metadata.subtree.solved {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeSolved),
			});
			txn.set(&key, &[]);
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
				cache_entry: cache_entry.clone(),
				object: id.clone(),
			});
			txn.set(&key, &[]);
		}
	}

	fn put_process(&self, txn: &fdb::Transaction, arg: &PutProcessArg) {
		let id = &arg.id;

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let exists_key = self.pack(&Key::Process {
			id: id.clone(),
			field: ProcessField::Core(ProcessCoreField::Exists),
		});
		txn.set(&exists_key, &[]);

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let touched_at_key = self.pack(&Key::Process {
			id: id.clone(),
			field: ProcessField::Core(ProcessCoreField::TouchedAt),
		});
		txn.atomic_op(
			&touched_at_key,
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
			txn.set(&key, &count.to_le_bytes());
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
				object: object.clone(),
				kind: *kind,
			});
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = self.pack(&Key::ObjectProcess {
				object: object.clone(),
				process: id.clone(),
				kind: *kind,
			});
			txn.set(&key, &[]);
		}
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
			txn.set(&key, &count.to_le_bytes());
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
			txn.set(&key, &depth.to_le_bytes());
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
			txn.set(&key, &size.to_le_bytes());
		}
		if let Some(true) = metadata.solvable {
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
			txn.set(&key, &[]);
		}
		if let Some(true) = metadata.solved {
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
			txn.set(&key, &[]);
		}
	}

	fn put_tag(&self, txn: &fdb::Transaction, arg: &PutTagArg) {
		let item_bytes: Vec<u8> = match &arg.item {
			tg::Either::Left(object_id) => object_id.to_bytes().to_vec(),
			tg::Either::Right(process_id) => process_id.to_bytes().to_vec(),
		};

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = self.pack(&Key::Tag {
			tag: arg.tag.clone(),
			field: TagField::Core(TagCoreField::Item),
		});
		txn.set(&key, &item_bytes);

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = self.pack(&Key::ItemTag {
			item: item_bytes,
			tag: arg.tag.clone(),
		});
		txn.set(&key, &[]);
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
