use {
	crate::{Index, Key, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{FutureExt as _, TryStreamExt as _, future},
	num_traits::ToPrimitive as _,
	std::{collections::BTreeSet, ops::ControlFlow},
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_process(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::process::put::Arg,
		partition_totals: crate::PartitionTotals,
		process_data_fragment_size: usize,
	) -> tg::Result<ControlFlow<tg::Result<()>, fdb::FdbError>> {
		let partition_total = partition_totals.cleaning;
		if let Some(registration) = &arg.registration {
			let result = crate::propagate!(
				Self::check_runner_registration_with_transaction(txn, subspace, registration).await
			);
			if let Err(error) = result {
				return Ok(ControlFlow::Break(Err(error)));
			}
		}

		let id = &arg.id;
		let key = Key::Process(crate::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);

		// Get the existing process, the existing data when the validation compares another principal's data, whether the parent accepts the process as a child, and the sandbox that the merge reads concurrently.
		let compare = arg.data.is_some() && arg.principal != tg::Principal::Process(id.clone());
		let process = Self::try_get_process_with_transaction(txn, subspace, id);
		let data = async {
			if !compare {
				return Ok(ControlFlow::Break(None));
			}
			Self::try_get_process_data_with_transaction(txn, subspace, id).await
		};
		let accepted = async {
			let Some(parent) = &arg.parent else {
				return Ok(ControlFlow::Break(None));
			};
			Self::try_get_process_accepts_child_with_transaction(txn, subspace, parent, id).await
		};
		let sandbox = async {
			let Some(sandbox) = arg.context_sandbox() else {
				return Ok(ControlFlow::Break(None));
			};
			let key = Key::Sandbox(crate::sandbox::Key::Sandbox(sandbox.clone()));
			let key = Self::pack(subspace, &key);
			let result = txn.get(&key, false).await;
			let sandbox = crate::retry!(result)
				.map(|bytes| tangram_index::sandbox::Sandbox::deserialize(&bytes))
				.transpose()?;
			Ok::<_, tg::Error>(ControlFlow::Break(sandbox))
		};
		let (existing, existing_data, accepted, sandbox) =
			futures::try_join!(process, data, accepted, sandbox)?;
		let existing = match existing {
			ControlFlow::Break(existing) => existing,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let mut existing_data = match existing_data {
			ControlFlow::Break(existing_data) => existing_data,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let accepted = match accepted {
			ControlFlow::Break(accepted) => accepted,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let sandbox = match sandbox {
			ControlFlow::Break(sandbox) => sandbox,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};

		// Get the existing data when the merge compares it and it was not read, which is when the process rewrites its own finished data.
		if !compare && arg.compares_data(existing.as_ref()) {
			existing_data = crate::propagate!(
				Self::try_get_process_data_with_transaction(txn, subspace, id).await
			);
		}

		// Get the existing children when the merge compares them.
		let existing_children = if arg.compares_children(existing.as_ref()) {
			let length = arg.children.as_ref().map_or(0, Vec::len) as u64 + 1;
			crate::propagate!(
				Self::try_get_process_children_page_with_transaction(
					txn,
					subspace,
					id,
					std::io::SeekFrom::Start(0),
					length
				)
				.await
			)
		} else {
			None
		};

		// Merge the put with the existing record in this write transaction.
		let context = tangram_index::process::put::Context {
			accepts_child: accepted,
			children: existing_children.as_deref(),
			data: existing_data.as_ref(),
			sandbox: sandbox.as_ref(),
		};
		let merge = match arg.merge(existing.as_ref(), context)? {
			Ok(merge) => merge,
			Err(error) => return Ok(ControlFlow::Break(Err(error))),
		};
		if merge.permissions {
			crate::propagate!(
				Self::put_permissions_with_transaction(
					txn,
					subspace,
					&arg.permissions,
					partition_totals
				)
				.await
			);
		}
		let Some(update) = merge.update else {
			return Ok(ControlFlow::Break(Ok(())));
		};
		let tangram_index::process::put::Update {
			added,
			changed,
			data,
			parent: parent_changed,
			process,
		} = update;
		let touched_at = process.touched_at;
		let status = process.status;
		let creator = process.creator.clone();
		let owner = process.owner.clone();
		let complete = process.complete();
		let value = process.serialize()?;
		txn.set(&key, &value);

		// Move the creator and owner keys when the creator, owner, or status changes.
		if let Some(status) = status {
			let previous = existing.as_ref().and_then(|existing| {
				existing
					.status
					.map(|status| (existing.creator.clone(), existing.owner.clone(), status))
			});
			let current = (creator, owner, status);
			if previous.as_ref() != Some(&current) {
				if let Some((creator, owner, status)) = &previous {
					for key in crate::process::Key::principals(creator, owner, *status, id) {
						let key = Self::pack(subspace, &Key::Process(key));
						txn.clear(&key);
					}
				}
				let (creator, owner, status) = &current;
				for key in crate::process::Key::principals(creator, owner, *status, id) {
					let key = Self::pack(subspace, &Key::Process(key));
					txn.set(&key, &[]);
				}
			}
		}

		// Replace the stored data with the written data.
		if let Some(data) = &data {
			let bytes = tangram_index::process::data::serialize(data)?;
			Self::put_process_data(txn, subspace, id, &bytes, process_data_fragment_size);
		}

		if added.children
			&& let Some(children) = &arg.children
		{
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
			let entries = crate::retry!(result);
			for entry in &entries {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Process(crate::process::Key::ProcessChild { child, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				let key = Key::Process(crate::process::Key::ChildProcess {
					child,
					parent: id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.clear(&key);
			}
			let (begin, end) = range_subspace.range();
			txn.clear_range(&begin, &end);
			for (position, child) in children.iter().enumerate() {
				let child = child.clone();
				let position = i64::try_from(position)
					.map_err(|_| tg::error!("the process has too many children"))?;
				let key = Key::Process(crate::process::Key::ProcessChild {
					child: child.process.node.clone(),
					position,
					process: id.clone(),
				});
				let key = Self::pack(subspace, &key);
				let value = tangram_serialize::to_vec(&child)
					.map_err(|error| tg::error!(!error, "failed to serialize the process child"))?;
				txn.set(&key, &value);

				let key = Key::Process(crate::process::Key::ChildProcess {
					child: child.process.node.clone(),
					parent: id.clone(),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &position.to_be_bytes());

				// Put a propagation from the child when the process is incomplete, so that the child's update enqueues an update on the process.
				if !complete {
					let key = Key::Update(crate::update::Key::Propagation {
						child: tg::Either::Right(child.process.node),
						parent: tg::Either::Right(id.clone()),
					});
					let key = Self::pack(subspace, &key);
					txn.set(&key, &[]);
				}
			}
		}

		if parent_changed && let Some(parent) = &arg.parent {
			let key = Key::Process(crate::process::Key::ChildProcess {
				child: id.clone(),
				parent: parent.clone(),
			});
			let key = Self::pack(subspace, &key);
			let parent_bytes = parent.to_bytes();
			let prefix = (Kind::ProcessChild.to_i32().unwrap(), parent_bytes.as_ref());
			let prefix = Self::pack(subspace, &prefix);
			let range = fdb::RangeOption {
				limit: Some(1),
				mode: fdb::options::StreamingMode::WantAll,
				reverse: true,
				..fdb::RangeOption::from(&fdbt::Subspace::from_bytes(prefix))
			};
			let (exists, entries) =
				futures::join!(txn.get(&key, false), txn.get_range(&range, 1, false));
			let exists = crate::retry!(exists).is_some();
			if !exists {
				let entries = crate::retry!(entries);
				let position = entries
					.first()
					.map(|entry| {
						let key = Self::unpack(subspace, entry.key())?;
						let Key::Process(crate::process::Key::ProcessChild { position, .. }) = key
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
				};
				let process_child_key = Key::Process(crate::process::Key::ProcessChild {
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

			// Put a propagation to the parent, which accepts the child and so is waiting on it, so that this process's update enqueues an update on the parent.
			let key = Key::Update(crate::update::Key::Propagation {
				child: tg::Either::Right(id.clone()),
				parent: tg::Either::Right(parent.clone()),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		if let Some(sandbox) = &arg.sandbox {
			crate::propagate!(
				Self::put_sandbox_process_with_transaction(txn, subspace, sandbox, id).await
			);
		}

		// Update the cacheable entry when the data is written, because it matches the stored data otherwise.
		if let Some(data) = &data {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process(crate::process::Key::CommandCacheableProcess {
				command: arg.command_id.clone(),
				process: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			if data.cacheable {
				txn.set(&key, &[]);
			} else {
				txn.clear(&key);
			}
		}

		let objects = arg
			.command
			.iter()
			.flatten()
			.cloned()
			.map(|object| (object, tangram_index::process::object::Kind::Command))
			.chain(
				arg.error
					.as_ref()
					.into_iter()
					.flatten()
					.flatten()
					.cloned()
					.map(|object| (object, tangram_index::process::object::Kind::Error)),
			)
			.chain(
				arg.log
					.as_ref()
					.into_iter()
					.flatten()
					.cloned()
					.map(|object| (object, tangram_index::process::object::Kind::Log)),
			)
			.chain(
				arg.output
					.as_ref()
					.into_iter()
					.flatten()
					.flatten()
					.cloned()
					.map(|object| (object, tangram_index::process::object::Kind::Output)),
			)
			.map(|(object, kind)| {
				let key = Key::Process(crate::process::Key::ProcessObject {
					process: id.clone(),
					kind,
					object: object.clone(),
				});
				let key = Self::pack(subspace, &key);
				(object, kind, key)
			})
			.collect::<Vec<_>>();

		// Read the existing process objects.
		let results = future::join_all(objects.iter().map(|(_, _, key)| txn.get(key, false))).await;

		// Add the process objects that do not exist.
		let mut added_keys = BTreeSet::new();
		for ((object, kind, key), result) in objects.into_iter().zip(results) {
			let previous = crate::retry!(result);
			let added = match kind {
				tangram_index::process::object::Kind::Command => added.command_objects,
				tangram_index::process::object::Kind::Error => added.error_objects,
				tangram_index::process::object::Kind::Log => added.log_objects,
				tangram_index::process::object::Kind::Output => added.output_objects,
			};
			if previous.is_some() || !added || !added_keys.insert(key.clone()) {
				continue;
			}
			let value = [];
			txn.set(&key, &value);

			let key = Key::Object(crate::object::Key::ObjectProcess {
				object: object.clone(),
				kind,
				process: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, &value);

			// Put a propagation from the object when the process is incomplete, so that the object's update enqueues an update on the process.
			if !complete {
				let key = Key::Update(crate::update::Key::Propagation {
					child: tg::Either::Left(object.clone()),
					parent: tg::Either::Right(id.clone()),
				});
				let key = Self::pack(subspace, &key);
				txn.set(&key, &[]);
			}

			Self::enqueue_update_with_kind(
				txn,
				subspace,
				&tg::Either::Left(object),
				&crate::update::Kind::Permission(tg::authorization::Subject::Process(id.clone())),
				crate::update::Source::Put,
				partition_totals.permission_update,
			);
		}

		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = crate::Key::Clean(crate::clean::Key::Process {
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
				crate::update::StorageAndMetadataSource::Put,
				partition_totals.storage_and_metadata_update,
			);
			let (parents, relationships) = futures::try_join!(
				Self::enqueue_account_process_from_parents(
					txn,
					subspace,
					id,
					partition_totals.usage_update,
					touched_at,
				),
				Self::enqueue_account_process_relationships(
					txn,
					subspace,
					id,
					partition_totals.usage_update,
					touched_at,
				),
			)?;
			if let ControlFlow::Continue(error) = parents {
				return Ok(ControlFlow::Continue(error));
			}
			if let ControlFlow::Continue(error) = relationships {
				return Ok(ControlFlow::Continue(error));
			}
		}

		Ok(ControlFlow::Break(Ok(())))
	}

	/// Get whether a process accepts a child, which it does unless it finished without listing the child, or `None` if the process is not indexed.
	pub(crate) async fn try_get_process_accepts_child_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		parent: &tg::process::Id,
		child: &tg::process::Id,
	) -> tg::Result<ControlFlow<Option<bool>, fdb::FdbError>> {
		let process =
			crate::propagate!(Self::try_get_process_with_transaction(txn, subspace, parent).await);
		let Some(process) = process else {
			return Ok(ControlFlow::Break(None));
		};
		if !process.set.children {
			return Ok(ControlFlow::Break(Some(true)));
		}
		let key = Key::Process(crate::process::Key::ChildProcess {
			child: child.clone(),
			parent: parent.clone(),
		});
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let listed = crate::retry!(result).is_some();

		Ok(ControlFlow::Break(Some(listed)))
	}

	pub(crate) async fn put_processes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		args: &[tangram_index::process::put::Arg],
		partition_totals: crate::PartitionTotals,
		process_data_fragment_size: usize,
	) -> tg::Result<ControlFlow<tg::Result<()>, fdb::FdbError>> {
		let mut rejection = None;
		for process in args {
			// Put the process.
			let result = crate::propagate!(
				Self::put_process(
					txn,
					subspace,
					process,
					partition_totals,
					process_data_fragment_size
				)
				.boxed()
				.await
			);
			if let Err(error) = result {
				rejection.get_or_insert(error);
				continue;
			}

			// Put the account's storage entry, which requires the accepted process.
			if let Some(account) = &process.account {
				let entry = tangram_index::usage::storage::put::ProcessArg {
					account: account.clone(),
					process: process.id.clone(),
					touched_at: process.touched_at,
				};
				crate::propagate!(
					Self::put_account_process(txn, subspace, &entry, partition_totals, None, None)
						.await
				);
			}

			// Put the captures, which require the accepted process.
			crate::propagate!(
				Self::put_captures_with_transaction(
					txn,
					subspace,
					&process.captures,
					partition_totals
				)
				.await
			);
		}

		// Return the first rejection, since a rejected process does not affect the other processes.
		let result = rejection.map_or(Ok(()), Err);

		Ok(ControlFlow::Break(result))
	}

	pub(crate) fn put_process_data(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::process::Id,
		bytes: &[u8],
		fragment_size: usize,
	) {
		// Clear the existing fragments, so that a shorter value does not leave stale fragments behind.
		Self::delete_process_data(txn, subspace, id);

		// Write the fragments.
		for (position, fragment) in bytes.chunks(fragment_size).enumerate() {
			let key = Key::Process(crate::process::Key::ProcessData {
				position: position.to_i64().unwrap(),
				process: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.set(&key, fragment);
		}
	}

	pub(crate) fn delete_process_data(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::process::Id,
	) {
		let id_bytes = id.to_bytes();
		let prefix = (Kind::ProcessData.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, &prefix);
		let (begin, end) = fdbt::Subspace::from_bytes(prefix).range();
		txn.clear_range(&begin, &end);
	}
}
