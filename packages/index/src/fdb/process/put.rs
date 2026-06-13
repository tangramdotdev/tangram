use {
	crate::fdb::{Index, ItemKind, Key},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_process(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::process::put::Arg,
		partition_total: u64,
	) -> Result<(), fdb::FdbBindingError> {
		let id = &arg.id;
		let key = Key::Process(crate::fdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);

		let existing = if arg.complete() {
			None
		} else {
			txn.get(&key, false)
				.await?
				.and_then(|bytes| crate::process::Process::deserialize(&bytes).ok())
		};

		let touched_at = existing.as_ref().map_or(arg.touched_at, |existing| {
			existing.touched_at.max(arg.touched_at)
		});

		let mut set = arg.set();
		if let Some(ref existing) = existing {
			set.merge(&existing.set);
		}

		let mut stored = arg.stored.clone();
		if let Some(ref existing) = existing {
			stored.merge(&existing.stored);
		}

		let mut metadata = arg.metadata.clone();
		if let Some(ref existing) = existing {
			metadata.merge(&existing.metadata);
		}

		let value = crate::process::Process {
			metadata,
			reference_count: 0,
			set,
			stored,
			touched_at,
		}
		.serialize()
		.map_err(|error| fdb::FdbBindingError::CustomError(error.into()))?;
		txn.set(&key, &value);

		if let Some(children) = &arg.children {
			for child in children {
				txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
					.unwrap();
				let key = Key::Process(crate::fdb::process::Key::ProcessChild {
					process: id.clone(),
					child: child.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &[]);

				txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
					.unwrap();
				let key = Key::Process(crate::fdb::process::Key::ChildProcess {
					child: child.clone(),
					parent: id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &[]);
			}
		}

		if let Some(parent) = &arg.parent {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process(crate::fdb::process::Key::ProcessChild {
				process: parent.clone(),
				child: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process(crate::fdb::process::Key::ChildProcess {
				child: id.clone(),
				parent: parent.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		let objects = std::iter::once((arg.command.clone(), crate::process::object::Kind::Command))
			.chain(
				arg.error
					.as_ref()
					.into_iter()
					.flatten()
					.flatten()
					.cloned()
					.map(|object| (object, crate::process::object::Kind::Error)),
			)
			.chain(
				arg.log
					.as_ref()
					.into_iter()
					.flatten()
					.cloned()
					.map(|object| (object, crate::process::object::Kind::Log)),
			)
			.chain(
				arg.output
					.as_ref()
					.into_iter()
					.flatten()
					.flatten()
					.cloned()
					.map(|object| (object, crate::process::object::Kind::Output)),
			);
		for (object, kind) in objects {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process(crate::fdb::process::Key::ProcessObject {
				process: id.clone(),
				kind,
				object: object.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object(crate::fdb::object::Key::ObjectProcess {
				object,
				kind,
				process: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = crate::fdb::Key::Clean(crate::fdb::clean::Key::Clean {
			partition,
			touched_at,
			kind: ItemKind::Process,
			id: tg::Either::Right(id.clone()),
		});
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);

		Self::enqueue_update(
			txn,
			subspace,
			&tg::Either::Right(id.clone()),
			partition_total,
		);

		Ok(())
	}

	pub(crate) async fn task_put_processes(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::process::put::Arg],
		partition_total: u64,
	) -> tg::Result<()> {
		for process in args {
			Self::put_process(txn, subspace, process, partition_total)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the process"))?;
		}
		Ok(())
	}
}
