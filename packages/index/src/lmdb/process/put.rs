use {
	crate::lmdb::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_process(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::process::put::Arg,
	) -> tg::Result<()> {
		arg.validate()?;
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

		let mut data = arg
			.data
			.clone()
			.or_else(|| existing.as_ref().and_then(|existing| existing.data.clone()));
		if let Some(data) = &mut data {
			data.children = None;
		}

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
			let id_bytes = id.to_bytes();
			let prefix = &(Kind::ProcessChild.to_i32().unwrap(), id_bytes.as_ref());
			let prefix = Self::pack(subspace, prefix);
			let entries = db
				.prefix_iter(transaction, &prefix)
				.map_err(|error| tg::error!(!error, "failed to get process children"))?
				.map(|entry| {
					let (key, _) = entry.map_err(|error| {
						tg::error!(!error, "failed to read a process child entry")
					})?;
					let unpacked = Self::unpack(subspace, key)?;
					let Key::Process(crate::lmdb::process::Key::ProcessChild { child, .. }) =
						unpacked
					else {
						return Err(tg::error!("unexpected key type"));
					};

					Ok((key.to_vec(), child))
				})
				.collect::<tg::Result<Vec<_>>>()?;
			for (key, child) in entries {
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete a process child"))?;
				let key = Key::Process(crate::lmdb::process::Key::ChildProcess {
					child,
					parent: id.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete a child process"))?;
			}
			for (position, child) in children.iter().enumerate() {
				let child = child.clone().without_tokens();
				let position = i64::try_from(position)
					.map_err(|_| tg::error!("the process has too many children"))?;
				let key = Key::Process(crate::lmdb::process::Key::ProcessChild {
					child: child.process.node.clone(),
					position,
					process: id.clone(),
				});
				let key = Self::pack(subspace, &key);
				let value = tangram_serialize::to_vec(&child)
					.map_err(|error| tg::error!(!error, "failed to serialize the process child"))?;
				db.put(transaction, &key, &value)
					.map_err(|error| tg::error!(!error, "failed to put the process child"))?;

				let key = Key::Process(crate::lmdb::process::Key::ChildProcess {
					child: child.process.node,
					parent: id.clone(),
				});
				let key = Self::pack(subspace, &key);
				db.put(transaction, &key, &position.to_be_bytes())
					.map_err(|error| tg::error!(!error, "failed to put the child process"))?;
			}
		}

		if parent_changed && let Some(parent) = &arg.parent {
			let key = Key::Process(crate::lmdb::process::Key::ChildProcess {
				child: id.clone(),
				parent: parent.clone(),
			});
			let key = Self::pack(subspace, &key);
			let exists = db
				.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the child process"))?
				.is_some();
			if !exists {
				let parent_bytes = parent.to_bytes();
				let prefix = &(Kind::ProcessChild.to_i32().unwrap(), parent_bytes.as_ref());
				let prefix = Self::pack(subspace, prefix);
				let position = db
					.rev_prefix_iter(transaction, &prefix)
					.map_err(|error| tg::error!(!error, "failed to get process children"))?
					.next()
					.transpose()
					.map_err(|error| tg::error!(!error, "failed to read a process child entry"))?
					.map(|(key, _)| {
						let key = Self::unpack(subspace, key)?;
						let Key::Process(crate::lmdb::process::Key::ProcessChild {
							position, ..
						}) = key
						else {
							return Err(tg::error!("unexpected key type"));
						};
						position
							.checked_add(1)
							.ok_or_else(|| tg::error!("the process has too many children"))
					})
					.transpose()?
					.unwrap_or(0);
				let child = tg::process::data::Child {
					cached: arg.cached,
					process: tg::Referent::new(id.clone(), arg.options.clone()),
				}
				.without_tokens();
				let process_child_key = Key::Process(crate::lmdb::process::Key::ProcessChild {
					child: id.clone(),
					position,
					process: parent.clone(),
				});
				let process_child_key = Self::pack(subspace, &process_child_key);
				let value = tangram_serialize::to_vec(&child)
					.map_err(|error| tg::error!(!error, "failed to serialize the process child"))?;
				db.put(transaction, &process_child_key, &value)
					.map_err(|error| tg::error!(!error, "failed to put the process child"))?;
				db.put(transaction, &key, &position.to_be_bytes())
					.map_err(|error| tg::error!(!error, "failed to put the child process"))?;
			}
		}

		let key = Key::Process(crate::lmdb::process::Key::CommandCacheableProcess {
			command: arg.command.clone(),
			process: id.clone(),
		});
		let key = Self::pack(subspace, &key);
		if data.as_ref().is_some_and(|data| data.cacheable) {
			db.put(transaction, &key, &[]).map_err(|error| {
				tg::error!(!error, "failed to put the command cacheable process")
			})?;
		} else {
			db.delete(transaction, &key).map_err(|error| {
				tg::error!(!error, "failed to delete the command cacheable process")
			})?;
		}

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

		let key = crate::lmdb::Key::Clean(crate::lmdb::clean::Key::Process {
			id: id.clone(),
			touched_at,
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
			Self::enqueue_account_process_from_parents(db, subspace, transaction, id, touched_at)?;
			Self::enqueue_account_process_relationships(db, subspace, transaction, id, touched_at)?;
		}

		Ok(())
	}

	pub(crate) fn put_processes_with_transaction(
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
