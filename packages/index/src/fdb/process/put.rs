use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_process(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::process::put::Arg,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		arg.validate()?;
		let id = &arg.id;
		let key = Key::Process(crate::fdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);

		let result = txn.get(&key, false).await;
		let existing = crate::fdb::retry!(result)
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

		let mut storage = arg.storage.clone();
		if merge && let Some(ref existing) = existing {
			storage.merge(&existing.storage);
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
					|| existing.storage != storage
			});
		if !changed && !touch {
			return Ok(ControlFlow::Break(()));
		}

		let value = crate::process::Process {
			data: data.clone(),
			metadata,
			reference_count: 0,
			sandbox: sandbox.clone(),
			set,
			storage,
			touched_at,
		}
		.serialize()?;
		txn.set(&key, &value);

		if sandbox_changed
			&& let Some(existing_sandbox) = existing
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

			crate::fdb::propagate!(
				Self::decrement_sandbox_reference_count(
					txn,
					subspace,
					existing_sandbox,
					partition_total,
				)
				.await
			);
		}

		if sandbox_changed && let Some(sandbox) = &sandbox {
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
			let id_bytes = id.to_bytes();
			let prefix = (Kind::ProcessChild.to_i32().unwrap(), id_bytes.as_ref());
			let prefix = Self::pack(subspace, &prefix);
			let range_subspace = fdbt::Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&range_subspace)
			};
			let result = txn
				.get_ranges_keyvalues(range, false)
				.try_collect::<Vec<_>>()
				.await;
			let entries = crate::fdb::retry!(result);
			for entry in &entries {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Process(crate::fdb::process::Key::ProcessChild { child, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				let key = Key::Process(crate::fdb::process::Key::ChildProcess {
					child,
					parent: id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.clear(&key);
			}
			let (begin, end) = range_subspace.range();
			txn.clear_range(&begin, &end);
			for (position, child) in children.iter().enumerate() {
				let child = child.clone().without_location_and_tokens();
				let position = i64::try_from(position)
					.map_err(|_| tg::error!("the process has too many children"))?;
				let key = Key::Process(crate::fdb::process::Key::ProcessChild {
					child: child.process.node.clone(),
					position,
					process: id.clone(),
				});
				let key = Self::pack(subspace, &key);
				let value = tangram_serialize::to_vec(&child)
					.map_err(|error| tg::error!(!error, "failed to serialize the process child"))?;
				txn.set(&key, &value);

				let key = Key::Process(crate::fdb::process::Key::ChildProcess {
					child: child.process.node,
					parent: id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &position.to_be_bytes());
			}
		}

		if parent_changed && let Some(parent) = &arg.parent {
			let key = Key::Process(crate::fdb::process::Key::ChildProcess {
				child: id.clone(),
				parent: parent.clone(),
			});
			let key = Self::pack(subspace, &key);
			let result = txn.get(&key, false).await;
			let exists = crate::fdb::retry!(result).is_some();
			if !exists {
				let parent_bytes = parent.to_bytes();
				let prefix = (Kind::ProcessChild.to_i32().unwrap(), parent_bytes.as_ref());
				let prefix = Self::pack(subspace, &prefix);
				let range = fdb::RangeOption {
					limit: Some(1),
					mode: fdb::options::StreamingMode::WantAll,
					reverse: true,
					..fdb::RangeOption::from(&fdbt::Subspace::from_bytes(prefix))
				};
				let result = txn.get_range(&range, 1, false).await;
				let entries = crate::fdb::retry!(result);
				let position = entries
					.first()
					.map(|entry| {
						let key = Self::unpack(subspace, entry.key())?;
						let Key::Process(crate::fdb::process::Key::ProcessChild {
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
				.without_location_and_tokens();
				let process_child_key = Key::Process(crate::fdb::process::Key::ProcessChild {
					child: id.clone(),
					position,
					process: parent.clone(),
				});
				let process_child_key = Self::pack(subspace, &process_child_key);
				let value = tangram_serialize::to_vec(&child)
					.map_err(|error| tg::error!(!error, "failed to serialize the process child"))?;
				txn.set(&process_child_key, &value);
				txn.set(&key, &position.to_be_bytes());
			}
		}

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = Key::Process(crate::fdb::process::Key::CommandCacheableProcess {
			command: arg.command.clone(),
			process: id.clone(),
		});
		let key = Self::pack(subspace, &key);
		if data.as_ref().is_some_and(|data| data.cacheable) {
			txn.set(&key, &[]);
		} else {
			txn.clear(&key);
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
				object: object.clone(),
				kind,
				process: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&tg::Either::Left(object),
				&crate::fdb::update::Kind::Grant(tg::authorization::Subject::Process(id.clone())),
				crate::fdb::update::Source::Put,
				partition_total,
			);
		}

		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = crate::fdb::Key::Clean(crate::fdb::clean::Key::Process {
			id: id.clone(),
			partition,
			touched_at,
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
			crate::fdb::propagate!(
				Self::enqueue_account_process_from_parents(
					txn,
					subspace,
					id,
					partition_total,
					touched_at,
				)
				.await
			);
			crate::fdb::propagate!(
				Self::enqueue_account_process_relationships(
					txn,
					subspace,
					id,
					partition_total,
					touched_at,
				)
				.await
			);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn put_processes_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[crate::process::put::Arg],
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		for process in args {
			crate::fdb::propagate!(
				Self::put_process(txn, subspace, process, partition_total).await
			);
		}
		Ok(ControlFlow::Break(()))
	}
}
