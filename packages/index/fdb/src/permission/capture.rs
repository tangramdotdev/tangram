use {
	crate::{Index, Key, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_index::permission::capture::{Entry, enqueue, put},
};

impl Index {
	pub(crate) async fn enqueue_permission_capture_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &enqueue::Arg,
		partition_total: u64,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		arg.validate()?;
		let resource = &arg.resource;
		let partition = Self::partition_for_id(resource.to_bytes().as_ref(), partition_total);
		let key = Self::pack(
			subspace,
			&Key::PermissionCaptureQueue {
				partition,
				resource: resource.clone(),
			},
		);
		// Use a fresh attempt even when an earlier queue entry was completed and removed.
		let attempt = rand::random::<u128>();
		let value = attempt.to_be_bytes();
		txn.set(&key, &value);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn permission_capture_batch_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<ControlFlow<Vec<Entry>, fdb::FdbError>> {
		if batch_size == 0 || partition_start >= partition_end {
			return Ok(ControlFlow::Break(Vec::new()));
		}
		let begin = subspace.pack(&(
			Kind::PermissionCaptureQueue.to_i32().unwrap(),
			partition_start,
		));
		let end = subspace.pack(&(
			Kind::PermissionCaptureQueue.to_i32().unwrap(),
			partition_end,
		));
		let range = fdb::RangeOption {
			limit: Some(batch_size),
			..fdb::RangeOption::from((begin, end))
		};
		let result = txn.get_range(&range, 1, true).await;
		let rows = crate::retry!(result);
		let rows = rows.iter().map(|row| (row.key(), row.value()));
		let entries = rows
			.map(|(key, value)| {
				let Key::PermissionCaptureQueue {
					partition,
					resource,
				} = Self::unpack(subspace, key)
					.map_err(|error| tg::error!(!error, "failed to decode a capture queue key"))?
				else {
					return Err(tg::error!("invalid capture queue key"));
				};
				let attempt = u128::from_be_bytes(
					value
						.try_into()
						.map_err(|error| tg::error!(!error, "invalid capture attempt"))?,
				);
				Ok(Entry {
					attempt,
					partition,
					resource,
				})
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(entries))
	}

	pub(crate) async fn complete_permission_capture_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		entry: &Entry,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = Self::pack(
			subspace,
			&Key::PermissionCaptureQueue {
				partition: entry.partition,
				resource: entry.resource.clone(),
			},
		);
		let result = txn.get(&key, false).await;
		let value = crate::retry!(result).map(|value| value.to_vec());
		if value.as_deref() == Some(entry.attempt.to_be_bytes().as_slice()) {
			txn.clear(&key);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn get_permission_captures_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		resource: &tg::Id,
	) -> tg::Result<ControlFlow<Vec<put::Arg>, fdb::FdbError>> {
		let prefix = subspace.pack(&(
			Kind::PermissionCapture.to_i32().unwrap(),
			resource.to_bytes().as_ref(),
		));
		let range = fdb::RangeOption::from(fdbt::Subspace::from_bytes(prefix).range());
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let rows = crate::retry!(result);
		let mut records = Vec::new();
		for row in rows {
			let contexts: Vec<put::Arg> = tangram_serialize::from_slice(row.value())
				.map_err(|error| tg::error!(!error, "failed to decode captures"))?;
			records.extend(contexts);
		}

		Ok(ControlFlow::Break(records))
	}

	pub(crate) async fn put_permission_capture_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &put::Arg,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		arg.validate()?;
		if !crate::propagate!(Self::permission_capture_subject_current(txn, subspace, arg).await) {
			return Ok(ControlFlow::Break(()));
		}

		// Retire all contexts once the recipient permanently covers every capturable permission.
		let subject = match arg.subject.kind() {
			tg::id::Kind::Tag => {
				tg::authorization::Subject::Tag(tg::tag::Id::try_from(arg.subject.clone())?)
			},
			tg::id::Kind::Process => {
				tg::authorization::Subject::Process(tg::process::Id::try_from(arg.subject.clone())?)
			},
			_ => return Err(tg::error!("invalid capture subject")),
		};
		let entries = crate::propagate!(
			Self::get_resource_permission_entries_for_subject_with_transaction(
				txn,
				subspace,
				&arg.resource.node,
				&subject
			)
			.await
		);
		let requested =
			tangram_index::permission::capture::requested_permissions(&arg.resource.node)?;
		let terminal = requested.iter().all(|permission| {
			entries
				.iter()
				.any(|entry| entry.direct == Some(None) && entry.permission.implies(permission))
		});
		if terminal {
			for key in [
				Key::PermissionCapture {
					resource: arg.resource.node.clone(),
					subject: arg.subject.clone(),
				},
				Key::PermissionCaptureSubject {
					resource: arg.resource.node.clone(),
					subject: arg.subject.clone(),
				},
			] {
				let key = Self::pack(subspace, &key);
				txn.clear(&key);
			}
			return Ok(ControlFlow::Break(()));
		}

		let root = &arg.resource;
		let resource = root.node.clone();
		let subject = arg.subject.clone();
		let key = Self::pack(
			subspace,
			&Key::PermissionCapture {
				resource: resource.clone(),
				subject: subject.clone(),
			},
		);
		let result = txn.get(&key, false).await;
		let value = crate::retry!(result).map(|value| value.to_vec());
		let mut contexts: Vec<put::Arg> = value
			.as_deref()
			.map(tangram_serialize::from_slice)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode captures"))?
			.unwrap_or_default();
		if contexts.contains(arg) {
			return Ok(ControlFlow::Break(()));
		}
		contexts.push(arg.clone());
		let value = tangram_serialize::to_vec(&contexts)
			.map_err(|error| tg::error!(!error, "failed to encode captures"))?;
		txn.set(&key, &value);
		let key = Self::pack(
			subspace,
			&Key::PermissionCaptureSubject { resource, subject },
		);
		txn.set(&key, &[]);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn delete_permission_capture_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &put::Arg,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let root = &arg.resource;
		let resource = root.node.clone();
		let subject = arg.subject.clone();
		let key = Self::pack(
			subspace,
			&Key::PermissionCapture {
				resource: resource.clone(),
				subject: subject.clone(),
			},
		);
		let result = txn.get(&key, false).await;
		let value = crate::retry!(result).map(|value| value.to_vec());
		let Some(value) = value else {
			return Ok(ControlFlow::Break(()));
		};
		let contexts: Vec<put::Arg> = tangram_serialize::from_slice(&value)
			.map_err(|error| tg::error!(!error, "failed to decode captures"))?;
		let remaining = contexts
			.into_iter()
			.filter(|context| context != arg)
			.collect::<Vec<_>>();
		if remaining.is_empty() {
			txn.clear(&key);
			let key = Self::pack(
				subspace,
				&Key::PermissionCaptureSubject { resource, subject },
			);
			txn.clear(&key);
		} else {
			let value = tangram_serialize::to_vec(&remaining)
				.map_err(|error| tg::error!(!error, "failed to encode captures"))?;
			txn.set(&key, &value);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn delete_permission_captures_for_subject_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		subject: &tg::Id,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let prefix = subspace.pack(&(
			Kind::PermissionCaptureSubject.to_i32().unwrap(),
			subject.to_bytes().as_ref(),
		));
		let range = fdb::RangeOption::from(fdbt::Subspace::from_bytes(prefix).range());
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let keys = crate::retry!(result)
			.into_iter()
			.map(|row| row.key().to_vec())
			.collect::<Vec<_>>();
		for key in keys {
			let Key::PermissionCaptureSubject { resource, subject } = Self::unpack(subspace, &key)
				.map_err(|error| tg::error!(!error, "failed to decode a capture key"))?
			else {
				return Err(tg::error!("invalid capture subject key"));
			};
			txn.clear(&key);
			let key = Self::pack(subspace, &Key::PermissionCapture { resource, subject });
			txn.clear(&key);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn permission_capture_subject_current(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &put::Arg,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let Some(version) = &arg.version else {
			return Ok(ControlFlow::Break(true));
		};
		let id = tg::tag::Id::try_from(arg.subject.clone())?;
		let tag = crate::propagate!(Self::try_get_tag_with_transaction(txn, subspace, &id).await);

		Ok(ControlFlow::Break(
			tag.is_some_and(|tag| tag.version == *version),
		))
	}
}
