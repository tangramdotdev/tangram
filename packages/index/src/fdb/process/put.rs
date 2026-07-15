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

		let existing = txn
			.get(&key, false)
			.await?
			.and_then(|bytes| crate::process::Process::deserialize(&bytes).ok());
		let merge = !arg.complete();

		let time_to_touch = i64::try_from(arg.time_to_touch.as_secs()).unwrap();
		let touch = existing.as_ref().is_none_or(|existing| {
			arg.touched_at.saturating_sub(existing.touched_at) >= time_to_touch
		});
		let touched_at = existing.as_ref().map_or(arg.touched_at, |existing| {
			if touch {
				existing.touched_at.max(arg.touched_at)
			} else {
				existing.touched_at
			}
		});
		let children_changed = arg.children.is_some()
			&& existing
				.as_ref()
				.is_none_or(|existing| !existing.set.children);
		let error_changed =
			arg.error.is_some() && existing.as_ref().is_none_or(|existing| !existing.set.error);
		let log_changed =
			arg.log.is_some() && existing.as_ref().is_none_or(|existing| !existing.set.log);
		let output_changed = arg.output.is_some()
			&& existing
				.as_ref()
				.is_none_or(|existing| !existing.set.output);
		let parent_changed = arg.parent.is_some();

		let mut set = arg.set();
		if merge && let Some(ref existing) = existing {
			set.merge(&existing.set);
		}

		let mut stored = arg.stored.clone();
		if merge && let Some(ref existing) = existing {
			stored.merge(&existing.stored);
		}

		let mut metadata = arg.metadata.clone();
		if merge && let Some(ref existing) = existing {
			metadata.merge(&existing.metadata);
		}

		let data = arg
			.data
			.clone()
			.or_else(|| existing.as_ref().and_then(|existing| existing.data.clone()));

		let sandbox = arg.sandbox.clone().or_else(|| {
			existing
				.as_ref()
				.and_then(|existing| existing.sandbox.clone())
		});
		let changed = parent_changed
			|| arg.data.is_some()
			|| existing.as_ref().is_none_or(|existing| {
				existing.metadata != metadata
					|| existing.sandbox != sandbox
					|| existing.set != set
					|| existing.stored != stored
			});
		if !changed && !touch {
			return Ok(());
		}

		let value = crate::process::Process {
			data: data.clone(),
			metadata,
			reference_count: 0,
			sandbox: sandbox.clone(),
			set,
			stored,
			touched_at,
		}
		.serialize()
		.map_err(|error| fdb::FdbBindingError::CustomError(error.into()))?;
		txn.set(&key, &value);

		if let Some(existing_sandbox) = existing
			.as_ref()
			.and_then(|existing| existing.sandbox.as_ref())
		{
			let key = Key::Sandbox(crate::fdb::sandbox::Key::SandboxProcess {
				sandbox: existing_sandbox.clone(),
				process: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);

			let key = Key::Process(crate::fdb::process::Key::ProcessSandbox {
				process: id.clone(),
				sandbox: existing_sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}

		if data.as_ref().is_some_and(|data| data.status.is_started())
			&& let Some(sandbox) = &sandbox
		{
			let key = Key::Sandbox(crate::fdb::sandbox::Key::SandboxProcess {
				sandbox: sandbox.clone(),
				process: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			let key = Key::Process(crate::fdb::process::Key::ProcessSandbox {
				process: id.clone(),
				sandbox: sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		if children_changed && let Some(children) = &arg.children {
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

		if parent_changed && let Some(parent) = &arg.parent {
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

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = Key::Process(crate::fdb::process::Key::CommandProcess {
			command: arg.command.clone(),
			process: id.clone(),
		});
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);

		let objects = existing
			.is_none()
			.then(|| (arg.command.clone(), crate::process::object::Kind::Command))
			.into_iter()
			.chain(
				arg.error
					.as_ref()
					.into_iter()
					.flatten()
					.flatten()
					.filter(|_| error_changed)
					.cloned()
					.map(|object| (object, crate::process::object::Kind::Error)),
			)
			.chain(
				arg.log
					.as_ref()
					.into_iter()
					.flatten()
					.filter(|_| log_changed)
					.cloned()
					.map(|object| (object, crate::process::object::Kind::Log)),
			)
			.chain(
				arg.output
					.as_ref()
					.into_iter()
					.flatten()
					.flatten()
					.filter(|_| output_changed)
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

		if changed {
			Self::enqueue_update(
				txn,
				subspace,
				&tg::Either::Right(id.clone()),
				partition_total,
			);
		}

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
