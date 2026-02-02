use {
	super::{Index, Key, KeyKind, Update},
	crate::{Object, Process, ProcessObjectKind},
	foundationdb as fdb,
	foundationdb_tuple::{self as fdbt, Subspace},
	futures::future,
	num_traits::{FromPrimitive as _, ToPrimitive as _},
	std::sync::atomic::{AtomicU64, Ordering},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn updates_finished(&self, transaction_id: u64) -> tg::Result<bool> {
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create the transaction"))?;

		let mut bytes = [0u8; 10];
		bytes[..8].copy_from_slice(&transaction_id.to_be_bytes());
		bytes[8..].copy_from_slice(&0xFFFFu16.to_be_bytes());
		let versionstamp = fdbt::Versionstamp::complete(bytes, 0);

		let key_kind = KeyKind::UpdateVersion.to_i32().unwrap();
		let futures = (0..self.partition_total).map(|partition| {
			let begin = Self::pack(&self.subspace, &(key_kind, partition));
			let end = Self::pack(&self.subspace, &(key_kind, partition, versionstamp.clone()));
			let range = fdb::RangeOption {
				begin: fdb::KeySelector::first_greater_or_equal(begin),
				end: fdb::KeySelector::first_greater_or_equal(end),
				limit: Some(1),
				mode: fdb::options::StreamingMode::WantAll,
				..Default::default()
			};
			txn.get_range(&range, 1, false)
		});
		let results = future::try_join_all(futures)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if updates are finished"))?;
		let finished = results.iter().all(|entries| entries.is_empty());

		Ok(finished)
	}

	pub async fn update_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<usize> {
		let start = std::time::Instant::now();
		let retry_count = AtomicU64::new(0);
		let partition_total = self.partition_total;

		let result = self
			.database
			.run(|txn, _maybe_committed| {
				retry_count.fetch_add(1, Ordering::Relaxed);
				let subspace = self.subspace.clone();
				async move {
					Self::update_batch_inner(
						&txn,
						&subspace,
						batch_size,
						partition_start,
						partition_count,
						partition_total,
					)
					.await
					.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))
				}
			})
			.await;

		let duration = start.elapsed().as_secs_f64();
		self.metrics.update_commit_duration.record(duration, &[]);
		self.metrics.update_transactions.add(1, &[]);

		let attempts = retry_count.load(Ordering::Relaxed);
		if attempts > 1 {
			self.metrics
				.update_transaction_conflict_retry
				.add(attempts - 1, &[]);
		}

		let count = match result {
			Ok(count) => count,
			Err(fdb::FdbBindingError::NonRetryableFdbError(error))
				if error.code() == 2101 && batch_size > 1 =>
			{
				self.metrics.update_transaction_too_large.add(1, &[]);
				let half = batch_size / 2;
				let first =
					Box::pin(self.update_batch(half, partition_start, partition_count)).await?;
				let second =
					Box::pin(self.update_batch(half, partition_start, partition_count)).await?;
				first + second
			},
			Err(error) => {
				return Err(tg::error!(!error, "failed to process update batch"));
			},
		};

		Ok(count)
	}

	async fn update_batch_inner(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
		partition_total: u64,
	) -> tg::Result<usize> {
		let mut entries = Vec::new();

		let key_kind = KeyKind::UpdateVersion.to_i32().unwrap();
		let partition_end = partition_start.saturating_add(partition_count);
		for partition in partition_start..partition_end {
			let remaining = batch_size.saturating_sub(entries.len());
			if remaining == 0 {
				break;
			}
			let begin = Self::pack(subspace, &(key_kind, partition));
			let end = Self::pack(subspace, &(key_kind, partition + 1));
			let range = fdb::RangeOption {
				begin: fdb::KeySelector::first_greater_or_equal(begin),
				end: fdb::KeySelector::first_greater_or_equal(end),
				limit: Some(remaining),
				mode: fdb::options::StreamingMode::WantAll,
				..Default::default()
			};
			let partition_entries = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get update version range"))?;
			for entry in partition_entries {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::UpdateVersion {
					partition,
					version,
					id,
				} = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				entries.push((partition, version, id));
			}
		}

		let mut count = 0;
		for (partition, version, id) in entries {
			let key = Self::pack(subspace, &Key::Update { id: id.clone() });
			let value = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get update key"))?;

			let Some(value) = value else {
				let key = Self::pack(
					subspace,
					&Key::UpdateVersion {
						partition,
						version: version.clone(),
						id: id.clone(),
					},
				);
				txn.clear(&key);
				count += 1;
				continue;
			};

			let update = value
				.first()
				.and_then(|value| Update::from_u8(*value))
				.ok_or_else(|| tg::error!("invalid update value"))?;

			let changed = match &id {
				tg::Either::Left(id) => Self::update_object(txn, subspace, id).await?,
				tg::Either::Right(id) => Self::update_process(txn, subspace, id).await?,
			};

			if match update {
				Update::Put => true,
				Update::Propagate => changed,
			} {
				Self::enqueue_parents(txn, subspace, &id, &version, partition_total).await?;
			}

			let key = Self::pack(subspace, &Key::Update { id: id.clone() });
			txn.clear(&key);
			let key = Self::pack(
				subspace,
				&Key::UpdateVersion {
					partition,
					version: version.clone(),
					id: id.clone(),
				},
			);
			txn.clear(&key);

			count += 1;
		}

		Ok(count)
	}

	async fn update_object(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<bool> {
		let key = Key::Object(id.clone());
		let key = Self::pack(subspace, &key);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
			.ok_or_else(|| tg::error!(%id, "object not found"))?;
		let mut object = Object::deserialize(&bytes)?;

		let children = Self::get_object_children_with_transaction(txn, subspace, id).await?;

		let child_objects: Vec<Option<Object>> = future::try_join_all(
			children
				.iter()
				.map(|child| Self::try_get_object_with_transaction(txn, subspace, child)),
		)
		.await?;

		let mut changed = false;

		if !object.stored.subtree {
			let value = child_objects
				.iter()
				.all(|child| child.as_ref().is_some_and(|object| object.stored.subtree));
			if value {
				object.stored.subtree = true;
				changed = true;
			}
		}

		if object.metadata.subtree.count.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.count)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				let value = 1 + value;
				object.metadata.subtree.count = Some(value);
				changed = true;
			}
		}

		if object.metadata.subtree.depth.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.depth)
				})
				.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
			if let Some(value) = value {
				let value = 1 + value;
				object.metadata.subtree.depth = Some(value);
				changed = true;
			}
		}

		if object.metadata.subtree.size.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.size)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				let value = object.metadata.node.size + value;
				object.metadata.subtree.size = Some(value);
				changed = true;
			}
		}

		if object.metadata.subtree.solvable.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.solvable)
				})
				.try_fold(object.metadata.node.solvable, |output, value| {
					value.map(|value| output || value)
				});
			if let Some(value) = value {
				object.metadata.subtree.solvable = Some(value);
				changed = true;
			}
		}

		if object.metadata.subtree.solved.is_none() {
			let value = child_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.solved)
				})
				.try_fold(object.metadata.node.solved, |output, value| {
					value.map(|value| output && value)
				});
			if let Some(value) = value {
				object.metadata.subtree.solved = Some(value);
				changed = true;
			}
		}

		if changed {
			let value = object
				.serialize()
				.map_err(|source| tg::error!(!source, "failed to serialize the object"))?;
			txn.set(&key, &value);
		}

		Ok(changed)
	}

	async fn update_process(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<bool> {
		let key = Key::Process(id.clone());
		let key = Self::pack(subspace, &key);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
			.ok_or_else(|| tg::error!(%id, "process not found"))?;
		let mut process = Process::deserialize(&bytes)?;

		let children = Self::get_process_children_with_transaction(txn, subspace, id).await?;
		let children = future::try_join_all(
			children
				.iter()
				.map(|child| Self::try_get_process_with_transaction(txn, subspace, child)),
		)
		.await?;

		let objects = Self::get_process_objects_with_transaction(txn, subspace, id).await?;
		let mut command_object: Option<Object> = None;
		let mut error_objects: Vec<Option<Object>> = Vec::new();
		let mut log_object: Option<Option<Object>> = None;
		let mut output_objects: Vec<Option<Object>> = Vec::new();
		for (object_id, kind) in &objects {
			let object = Self::try_get_object_with_transaction(txn, subspace, object_id).await?;
			match kind {
				ProcessObjectKind::Command => {
					command_object = object;
				},
				ProcessObjectKind::Error => {
					error_objects.push(object);
				},
				ProcessObjectKind::Log => {
					log_object = Some(object);
				},
				ProcessObjectKind::Output => {
					output_objects.push(object);
				},
			}
		}

		let mut changed = false;

		if let Some(object) = &command_object {
			if process.metadata.node.command.count.is_none()
				&& let Some(value) = object.metadata.subtree.count
			{
				process.metadata.node.command.count = Some(value);
				changed = true;
			}
			if process.metadata.node.command.depth.is_none()
				&& let Some(value) = object.metadata.subtree.depth
			{
				process.metadata.node.command.depth = Some(value);
				changed = true;
			}
			if process.metadata.node.command.size.is_none()
				&& let Some(value) = object.metadata.subtree.size
			{
				process.metadata.node.command.size = Some(value);
				changed = true;
			}
			if process.metadata.node.command.solvable.is_none()
				&& let Some(value) = object.metadata.subtree.solvable
			{
				process.metadata.node.command.solvable = Some(value);
				changed = true;
			}
			if process.metadata.node.command.solved.is_none()
				&& let Some(value) = object.metadata.subtree.solved
			{
				process.metadata.node.command.solved = Some(value);
				changed = true;
			}
		}

		if process.metadata.node.error.count.is_none() {
			let value = error_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.count)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				process.metadata.node.error.count = Some(value);
				changed = true;
			}
		}

		if process.metadata.node.error.depth.is_none() {
			let value = error_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.depth)
				})
				.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
			if let Some(value) = value {
				process.metadata.node.error.depth = Some(value);
				changed = true;
			}
		}

		if process.metadata.node.error.size.is_none() {
			let value = error_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.size)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				process.metadata.node.error.size = Some(value);
				changed = true;
			}
		}

		if process.metadata.node.error.solvable.is_none() {
			let value = error_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.solvable)
				})
				.try_fold(false, |output, value| value.map(|value| output || value));
			if let Some(value) = value {
				process.metadata.node.error.solvable = Some(value);
				changed = true;
			}
		}

		if process.metadata.node.error.solved.is_none() {
			let value = error_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.solved)
				})
				.try_fold(true, |output, value| value.map(|value| output && value));
			if let Some(value) = value {
				process.metadata.node.error.solved = Some(value);
				changed = true;
			}
		}

		if let Some(Some(object)) = &log_object {
			if process.metadata.node.log.count.is_none()
				&& let Some(value) = object.metadata.subtree.count
			{
				process.metadata.node.log.count = Some(value);
				changed = true;
			}
			if process.metadata.node.log.depth.is_none()
				&& let Some(value) = object.metadata.subtree.depth
			{
				process.metadata.node.log.depth = Some(value);
				changed = true;
			}
			if process.metadata.node.log.size.is_none()
				&& let Some(value) = object.metadata.subtree.size
			{
				process.metadata.node.log.size = Some(value);
				changed = true;
			}
			if process.metadata.node.log.solvable.is_none()
				&& let Some(value) = object.metadata.subtree.solvable
			{
				process.metadata.node.log.solvable = Some(value);
				changed = true;
			}
			if process.metadata.node.log.solved.is_none()
				&& let Some(value) = object.metadata.subtree.solved
			{
				process.metadata.node.log.solved = Some(value);
				changed = true;
			}
		} else if log_object.is_none() {
			if process.metadata.node.log.count.is_none() {
				process.metadata.node.log.count = Some(0);
				changed = true;
			}
			if process.metadata.node.log.depth.is_none() {
				process.metadata.node.log.depth = Some(0);
				changed = true;
			}
			if process.metadata.node.log.size.is_none() {
				process.metadata.node.log.size = Some(0);
				changed = true;
			}
			if process.metadata.node.log.solvable.is_none() {
				process.metadata.node.log.solvable = Some(false);
				changed = true;
			}
			if process.metadata.node.log.solved.is_none() {
				process.metadata.node.log.solved = Some(true);
				changed = true;
			}
		}

		if process.metadata.node.output.count.is_none() {
			let value = output_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.count)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				process.metadata.node.output.count = Some(value);
				changed = true;
			}
		}

		if process.metadata.node.output.depth.is_none() {
			let value = output_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.depth)
				})
				.try_fold(0u64, |output, value| value.map(|value| output.max(value)));
			if let Some(value) = value {
				process.metadata.node.output.depth = Some(value);
				changed = true;
			}
		}

		if process.metadata.node.output.size.is_none() {
			let value = output_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.size)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				process.metadata.node.output.size = Some(value);
				changed = true;
			}
		}

		if process.metadata.node.output.solvable.is_none() {
			let value = output_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.solvable)
				})
				.try_fold(false, |output, value| value.map(|value| output || value));
			if let Some(value) = value {
				process.metadata.node.output.solvable = Some(value);
				changed = true;
			}
		}

		if process.metadata.node.output.solved.is_none() {
			let value = output_objects
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|object| object.metadata.subtree.solved)
				})
				.try_fold(true, |output, value| value.map(|value| output && value));
			if let Some(value) = value {
				process.metadata.node.output.solved = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.count.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.count)
				})
				.sum::<Option<u64>>();
			if let Some(value) = value {
				let value = 1 + value;
				process.metadata.subtree.count = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.command.count.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.command.count)
				})
				.fold(process.metadata.node.command.count, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.command.count = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.command.depth.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.command.depth)
				})
				.fold(process.metadata.node.command.depth, |output, value| {
					output.and_then(|output| value.map(|value| output.max(value)))
				});
			if let Some(value) = value {
				process.metadata.subtree.command.depth = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.command.size.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.command.size)
				})
				.fold(process.metadata.node.command.size, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.command.size = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.command.solvable.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.command.solvable)
				})
				.fold(process.metadata.node.command.solvable, |output, value| {
					output.and_then(|output| value.map(|value| output || value))
				});
			if let Some(value) = value {
				process.metadata.subtree.command.solvable = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.command.solved.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.command.solved)
				})
				.fold(process.metadata.node.command.solved, |output, value| {
					output.and_then(|output| value.map(|value| output && value))
				});
			if let Some(value) = value {
				process.metadata.subtree.command.solved = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.error.count.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.error.count)
				})
				.fold(process.metadata.node.error.count, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.error.count = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.error.depth.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.error.depth)
				})
				.fold(process.metadata.node.error.depth, |output, value| {
					output.and_then(|output| value.map(|value| output.max(value)))
				});
			if let Some(value) = value {
				process.metadata.subtree.error.depth = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.error.size.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.error.size)
				})
				.fold(process.metadata.node.error.size, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.error.size = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.error.solvable.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.error.solvable)
				})
				.fold(process.metadata.node.error.solvable, |output, value| {
					output.and_then(|output| value.map(|value| output || value))
				});
			if let Some(value) = value {
				process.metadata.subtree.error.solvable = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.error.solved.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.error.solved)
				})
				.fold(process.metadata.node.error.solved, |output, value| {
					output.and_then(|output| value.map(|value| output && value))
				});
			if let Some(value) = value {
				process.metadata.subtree.error.solved = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.log.count.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.log.count)
				})
				.fold(process.metadata.node.log.count, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.log.count = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.log.depth.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.log.depth)
				})
				.fold(process.metadata.node.log.depth, |output, value| {
					output.and_then(|output| value.map(|value| output.max(value)))
				});
			if let Some(value) = value {
				process.metadata.subtree.log.depth = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.log.size.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.log.size)
				})
				.fold(process.metadata.node.log.size, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.log.size = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.log.solvable.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.log.solvable)
				})
				.fold(process.metadata.node.log.solvable, |output, value| {
					output.and_then(|output| value.map(|value| output || value))
				});
			if let Some(value) = value {
				process.metadata.subtree.log.solvable = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.log.solved.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.log.solved)
				})
				.fold(process.metadata.node.log.solved, |output, value| {
					output.and_then(|output| value.map(|value| output && value))
				});
			if let Some(value) = value {
				process.metadata.subtree.log.solved = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.output.count.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.output.count)
				})
				.fold(process.metadata.node.output.count, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.output.count = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.output.depth.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.output.depth)
				})
				.fold(process.metadata.node.output.depth, |output, value| {
					output.and_then(|output| value.map(|value| output.max(value)))
				});
			if let Some(value) = value {
				process.metadata.subtree.output.depth = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.output.size.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.output.size)
				})
				.fold(process.metadata.node.output.size, |output, value| {
					output.and_then(|output| value.map(|value| output + value))
				});
			if let Some(value) = value {
				process.metadata.subtree.output.size = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.output.solvable.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.output.solvable)
				})
				.fold(process.metadata.node.output.solvable, |output, value| {
					output.and_then(|output| value.map(|value| output || value))
				});
			if let Some(value) = value {
				process.metadata.subtree.output.solvable = Some(value);
				changed = true;
			}
		}

		if process.metadata.subtree.output.solved.is_none() {
			let value = children
				.iter()
				.map(|option| {
					option
						.as_ref()
						.and_then(|child| child.metadata.subtree.output.solved)
				})
				.fold(process.metadata.node.output.solved, |output, value| {
					output.and_then(|output| value.map(|value| output && value))
				});
			if let Some(value) = value {
				process.metadata.subtree.output.solved = Some(value);
				changed = true;
			}
		}

		if let Some(object) = &command_object
			&& !process.stored.node_command
			&& object.stored.subtree
		{
			process.stored.node_command = true;
			changed = true;
		}

		if !process.stored.node_error {
			let value = error_objects
				.iter()
				.all(|option| option.as_ref().is_some_and(|object| object.stored.subtree));
			if value {
				process.stored.node_error = true;
				changed = true;
			}
		}

		if let Some(Some(object)) = &log_object {
			if !process.stored.node_log && object.stored.subtree {
				process.stored.node_log = true;
				changed = true;
			}
		} else if log_object.is_none() && !process.stored.node_log {
			process.stored.node_log = true;
			changed = true;
		}

		if !process.stored.node_output {
			let value = output_objects
				.iter()
				.all(|option| option.as_ref().is_some_and(|object| object.stored.subtree));
			if value {
				process.stored.node_output = true;
				changed = true;
			}
		}

		if !process.stored.subtree {
			let value = children
				.iter()
				.all(|child| child.as_ref().is_some_and(|child| child.stored.subtree));
			if value {
				process.stored.subtree = true;
				changed = true;
			}
		}

		if !process.stored.subtree_command && process.stored.node_command {
			let value = children.iter().all(|child| {
				child
					.as_ref()
					.is_some_and(|child| child.stored.subtree_command)
			});
			if value {
				process.stored.subtree_command = true;
				changed = true;
			}
		}

		if !process.stored.subtree_error && process.stored.node_error {
			let value = children.iter().all(|child| {
				child
					.as_ref()
					.is_some_and(|child| child.stored.subtree_error)
			});
			if value {
				process.stored.subtree_error = true;
				changed = true;
			}
		}

		if !process.stored.subtree_log && process.stored.node_log {
			let value = children
				.iter()
				.all(|child| child.as_ref().is_some_and(|child| child.stored.subtree_log));
			if value {
				process.stored.subtree_log = true;
				changed = true;
			}
		}

		if !process.stored.subtree_output && process.stored.node_output {
			let value = children.iter().all(|child| {
				child
					.as_ref()
					.is_some_and(|child| child.stored.subtree_output)
			});
			if value {
				process.stored.subtree_output = true;
				changed = true;
			}
		}

		if changed {
			let value = process
				.serialize()
				.map_err(|source| tg::error!(!source, "failed to serialize the process"))?;
			txn.set(&key, &value);
		}

		Ok(changed)
	}

	async fn enqueue_parents(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		version: &fdbt::Versionstamp,
		partition_total: u64,
	) -> tg::Result<()> {
		match id {
			tg::Either::Left(id) => {
				let parents = Self::get_object_parents_with_transaction(txn, subspace, id).await?;
				for parent in parents {
					Self::enqueue_update_propagate(
						txn,
						subspace,
						&tg::Either::Left(parent),
						version,
						partition_total,
					);
				}
				let process_parents =
					Self::get_object_processes_with_transaction(txn, subspace, id).await?;
				for (process, _kind) in process_parents {
					Self::enqueue_update_propagate(
						txn,
						subspace,
						&tg::Either::Right(process),
						version,
						partition_total,
					);
				}
			},
			tg::Either::Right(id) => {
				let parents = Self::get_process_parents_with_transaction(txn, subspace, id).await?;
				for parent in parents {
					Self::enqueue_update_propagate(
						txn,
						subspace,
						&tg::Either::Right(parent),
						version,
						partition_total,
					);
				}
			},
		}
		Ok(())
	}

	fn enqueue_update_propagate(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		version: &fdbt::Versionstamp,
		partition_total: u64,
	) {
		let key = Self::pack(subspace, &Key::Update { id: id.clone() });
		let value = [Update::Propagate.to_u8().unwrap()];
		txn.atomic_op(&key, &value, fdb::options::MutationType::Min);

		let id_bytes = match &id {
			tg::Either::Left(id) => id.to_bytes(),
			tg::Either::Right(id) => id.to_bytes(),
		};
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		let key = Self::pack(
			subspace,
			&(
				KeyKind::UpdateVersion.to_i32().unwrap(),
				partition,
				version.clone(),
				id_bytes.as_ref(),
			),
		);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.set(&key, &[]);
	}
}
