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
		let existing = if merge {
			db.get(transaction, &key)
				.map_err(|error| tg::error!(!error, %id, "failed to get the process"))?
				.and_then(|bytes| crate::process::Process::deserialize(bytes).ok())
		} else {
			None
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
		.serialize()?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, %id, "failed to put the process"))?;

		if let Some(children) = &arg.children {
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

		if let Some(parent) = &arg.parent {
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
			id: tg::Either::Right(id.clone()),
		});
		let key = Self::pack(subspace, &key);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put the clean key"))?;

		Self::enqueue_update(
			db,
			subspace,
			transaction,
			tg::Either::Right(id.clone()),
			crate::lmdb::update::Source::Put,
			None,
		)?;

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
