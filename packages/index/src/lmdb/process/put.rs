use {
	crate::lmdb::{Db, Index, ItemKind, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::process::put::Arg,
	) -> tg::Result<()> {
		let id = &arg.id;
		let key = Key::Process(crate::lmdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);

		let merge = !arg.complete();
		let existing = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?
			.and_then(|bytes| crate::process::Process::deserialize(bytes).ok());

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
		let sandbox_changed = existing
			.as_ref()
			.and_then(|existing| existing.sandbox.as_ref())
			!= sandbox.as_ref();
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
		.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, %id, "failed to put the process"))?;

		if sandbox_changed
			&& let Some(existing_sandbox) = existing
				.as_ref()
				.and_then(|existing| existing.sandbox.as_ref())
		{
			let key = Key::Sandbox(crate::lmdb::sandbox::Key::SandboxProcess {
				sandbox: existing_sandbox.clone(),
				process: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the sandbox process"))?;

			let key = Key::Process(crate::lmdb::process::Key::ProcessSandbox {
				process: id.clone(),
				sandbox: existing_sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the process sandbox"))?;

			Self::decrement_sandbox_reference_count(db, subspace, transaction, existing_sandbox)?;
		}

		if sandbox_changed && let Some(sandbox) = &sandbox {
			let key = Key::Sandbox(crate::lmdb::sandbox::Key::SandboxProcess {
				sandbox: sandbox.clone(),
				process: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the sandbox process"))?;

			let key = Key::Process(crate::lmdb::process::Key::ProcessSandbox {
				process: id.clone(),
				sandbox: sandbox.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the process sandbox"))?;
		}

		if children_changed && let Some(children) = &arg.children {
			for child in children {
				let key = Key::Process(crate::lmdb::process::Key::ProcessChild {
					process: id.clone(),
					child: child.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the process child"))?;

				let key = Key::Process(crate::lmdb::process::Key::ChildProcess {
					child: child.clone(),
					parent: id.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &[])
					.map_err(|error| tg::error!(!error, "failed to put the child process"))?;
			}
		}

		if parent_changed && let Some(parent) = &arg.parent {
			let key = Key::Process(crate::lmdb::process::Key::ProcessChild {
				process: parent.clone(),
				child: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the process child"))?;

			let key = Key::Process(crate::lmdb::process::Key::ChildProcess {
				child: id.clone(),
				parent: parent.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the child process"))?;
		}

		let key = Key::Process(crate::lmdb::process::Key::CommandProcess {
			command: arg.command.clone(),
			process: id.clone(),
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the command process"))?;

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
			let key = Key::Process(crate::lmdb::process::Key::ProcessObject {
				process: id.clone(),
				kind,
				object: object.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the process object"))?;

			let key = Key::Object(crate::lmdb::object::Key::ObjectProcess {
				object,
				kind,
				process: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to put the object process"))?;
		}

		let key = crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Clean {
			touched_at,
			kind: ItemKind::Process,
			id: id.clone().into(),
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the clean key"))?;

		if changed {
			Self::enqueue_update(
				db,
				subspace,
				transaction,
				tg::Either::Right(id.clone()),
				crate::lmdb::update::Source::Put,
				None,
			)?;
		}

		Ok(())
	}

	pub(crate) fn task_put_processes(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		args: &[crate::process::put::Arg],
	) -> tg::Result<()> {
		for process in args {
			Self::put_process(db, subspace, transaction, process)?;
		}
		Ok(())
	}
}
