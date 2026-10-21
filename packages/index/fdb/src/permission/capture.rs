use {
	crate::{Index, Key, Kind},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_index::permission::capture::{Capture, Entry, enqueue},
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
	) -> tg::Result<ControlFlow<Vec<Capture>, fdb::FdbError>> {
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
		let captures = rows
			.iter()
			.map(|row| {
				tangram_serialize::from_slice(row.value())
					.map_err(|error| tg::error!(!error, "failed to decode a capture"))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(captures))
	}

	pub(crate) async fn has_permission_captures_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		resource: &tg::Id,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let prefix = subspace.pack(&(
			Kind::PermissionCapture.to_i32().unwrap(),
			resource.to_bytes().as_ref(),
		));
		let range = fdb::RangeOption {
			limit: Some(1),
			..fdb::RangeOption::from(fdbt::Subspace::from_bytes(prefix).range())
		};
		let result = txn.get_range(&range, 1, false).await;
		let rows = crate::retry!(result);

		Ok(ControlFlow::Break(!rows.is_empty()))
	}

	pub(crate) async fn has_permission_captures_for_other_subjects_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		resource: &tg::Id,
		subject: &tg::Id,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		// Read the ranges before and after the subject's captures on the resource.
		let prefix = subspace.pack(&(
			Kind::PermissionCapture.to_i32().unwrap(),
			resource.to_bytes().as_ref(),
		));
		let (resource_begin, resource_end) = fdbt::Subspace::from_bytes(prefix).range();
		let prefix = subspace.pack(&(
			Kind::PermissionCapture.to_i32().unwrap(),
			resource.to_bytes().as_ref(),
			subject.to_bytes().as_ref(),
		));
		let (subject_begin, subject_end) = fdbt::Subspace::from_bytes(prefix).range();
		for (begin, end) in [(resource_begin, subject_begin), (subject_end, resource_end)] {
			let range = fdb::RangeOption {
				limit: Some(1),
				..fdb::RangeOption::from((begin, end))
			};
			let result = txn.get_range(&range, 1, false).await;
			let rows = crate::retry!(result);
			if !rows.is_empty() {
				return Ok(ControlFlow::Break(true));
			}
		}

		Ok(ControlFlow::Break(false))
	}

	pub(crate) async fn has_permission_captures_for_source_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		resource: &tg::Id,
		source: &tg::authorization::Subject,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let prefix = subspace.pack(&(
			Kind::PermissionCaptureSource.to_i32().unwrap(),
			resource.to_bytes().as_ref(),
			source.to_string(),
		));
		let range = fdb::RangeOption {
			limit: Some(1),
			..fdb::RangeOption::from(fdbt::Subspace::from_bytes(prefix).range())
		};
		let result = txn.get_range(&range, 1, false).await;
		let rows = crate::retry!(result);

		Ok(ControlFlow::Break(!rows.is_empty()))
	}

	pub(crate) async fn put_permission_capture_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		capture: &Capture,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		capture.validate()?;
		if !crate::propagate!(
			Self::permission_capture_subject_current(txn, subspace, capture).await
		) {
			return Ok(ControlFlow::Break(()));
		}

		// Retire every capture for the pair once the subject permanently covers every capturable permission.
		let subject = match capture.subject.kind() {
			tg::id::Kind::Tag => {
				tg::authorization::Subject::Tag(tg::tag::Id::try_from(capture.subject.clone())?)
			},
			tg::id::Kind::Process => tg::authorization::Subject::Process(
				tg::process::Id::try_from(capture.subject.clone())?,
			),
			_ => return Err(tg::error!("invalid capture subject")),
		};
		let entries = crate::propagate!(
			Self::get_resource_permission_entries_for_subject_with_transaction(
				txn,
				subspace,
				&capture.resource.node,
				&subject
			)
			.await
		);
		let requested =
			tangram_index::permission::capture::requested_permissions(&capture.resource.node)?;
		let terminal = requested.iter().all(|permission| {
			entries
				.iter()
				.any(|entry| entry.direct == Some(None) && entry.permission.implies(permission))
		});
		if terminal {
			crate::propagate!(
				Self::delete_permission_captures_for_resource_and_subject_with_transaction(
					txn,
					subspace,
					&capture.resource.node,
					&capture.subject
				)
				.await
			);
			return Ok(ControlFlow::Break(()));
		}

		// Keep the later expiration when the capture already exists.
		let digest = capture.digest()?;
		let key = Self::pack(
			subspace,
			&Key::PermissionCapture {
				digest,
				resource: capture.resource.node.clone(),
				subject: capture.subject.clone(),
			},
		);
		let result = txn.get(&key, false).await;
		let value = crate::retry!(result);
		if let Some(value) = value {
			let old: Capture = tangram_serialize::from_slice(&value)
				.map_err(|error| tg::error!(!error, "failed to decode the capture"))?;
			if old.expires_at >= capture.expires_at {
				return Ok(ControlFlow::Break(()));
			}
			let key = Self::pack(
				subspace,
				&Key::PermissionCaptureExpiresAt {
					digest,
					expires_at: old.expires_at,
					resource: capture.resource.node.clone(),
					subject: capture.subject.clone(),
				},
			);
			txn.clear(&key);
		}

		// Write the capture and its index keys.
		let value = tangram_serialize::to_vec(capture)
			.map_err(|error| tg::error!(!error, "failed to encode the capture"))?;
		txn.set(&key, &value);
		for key in index_keys(capture, digest) {
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn delete_permission_capture_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		capture: &Capture,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let digest = capture.digest()?;
		crate::propagate!(
			Self::delete_permission_capture_keys_with_transaction(
				txn,
				subspace,
				&capture.resource.node,
				&capture.subject,
				digest
			)
			.await
		);

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
		let rows = crate::retry!(result);
		let keys = rows
			.iter()
			.map(|row| {
				let Key::PermissionCaptureSubject {
					digest,
					resource,
					subject,
				} = Self::unpack(subspace, row.key())
					.map_err(|error| tg::error!(!error, "failed to decode a capture key"))?
				else {
					return Err(tg::error!("invalid capture subject key"));
				};
				Ok((digest, resource, subject))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		drop(rows);
		for (digest, resource, subject) in keys {
			crate::propagate!(
				Self::delete_permission_capture_keys_with_transaction(
					txn, subspace, &resource, &subject, digest
				)
				.await
			);
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn delete_expired_permission_captures(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		now: i64,
		limit: usize,
	) -> tg::Result<ControlFlow<usize, fdb::FdbError>> {
		if limit == 0 {
			return Ok(ControlFlow::Break(0));
		}
		let prefix = subspace.pack(&(Kind::PermissionCaptureExpiresAt.to_i32().unwrap(),));
		let range = fdb::RangeOption {
			limit: Some(limit),
			..fdb::RangeOption::from(fdbt::Subspace::from_bytes(prefix).range())
		};
		let result = txn.get_range(&range, 1, false).await;
		let rows = crate::retry!(result);
		let keys = rows
			.iter()
			.map(|row| {
				let Key::PermissionCaptureExpiresAt {
					digest,
					expires_at,
					resource,
					subject,
				} = Self::unpack(subspace, row.key()).map_err(|error| {
					tg::error!(!error, "failed to decode a capture expiration key")
				})?
				else {
					return Err(tg::error!("invalid capture expiration key"));
				};
				Ok((digest, expires_at, resource, subject))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		drop(rows);
		let mut count = 0;
		for (digest, expires_at, resource, subject) in keys {
			if expires_at > now {
				break;
			}
			crate::propagate!(
				Self::delete_permission_capture_keys_with_transaction(
					txn, subspace, &resource, &subject, digest
				)
				.await
			);
			count += 1;
		}

		Ok(ControlFlow::Break(count))
	}

	async fn delete_permission_captures_for_resource_and_subject_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		resource: &tg::Id,
		subject: &tg::Id,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let prefix = subspace.pack(&(
			Kind::PermissionCapture.to_i32().unwrap(),
			resource.to_bytes().as_ref(),
			subject.to_bytes().as_ref(),
		));
		let range = fdb::RangeOption::from(fdbt::Subspace::from_bytes(prefix).range());
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let rows = crate::retry!(result);
		for row in &rows {
			let Key::PermissionCapture { digest, .. } = Self::unpack(subspace, row.key())
				.map_err(|error| tg::error!(!error, "failed to decode a capture key"))?
			else {
				return Err(tg::error!("invalid capture key"));
			};
			let capture = tangram_serialize::from_slice(row.value())
				.map_err(|error| tg::error!(!error, "failed to decode a capture"))?;
			Self::clear_permission_capture(txn, subspace, &capture, digest);
		}

		Ok(ControlFlow::Break(()))
	}

	async fn delete_permission_capture_keys_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		resource: &tg::Id,
		subject: &tg::Id,
		digest: [u8; 32],
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = Self::pack(
			subspace,
			&Key::PermissionCapture {
				digest,
				resource: resource.clone(),
				subject: subject.clone(),
			},
		);
		let result = txn.get(&key, false).await;
		let value = crate::retry!(result);
		let Some(value) = value else {
			return Ok(ControlFlow::Break(()));
		};
		let capture = tangram_serialize::from_slice(&value)
			.map_err(|error| tg::error!(!error, "failed to decode the capture"))?;
		Self::clear_permission_capture(txn, subspace, &capture, digest);

		Ok(ControlFlow::Break(()))
	}

	fn clear_permission_capture(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		capture: &Capture,
		digest: [u8; 32],
	) {
		let key = Self::pack(
			subspace,
			&Key::PermissionCapture {
				digest,
				resource: capture.resource.node.clone(),
				subject: capture.subject.clone(),
			},
		);
		txn.clear(&key);
		for key in index_keys(capture, digest) {
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
	}

	pub(crate) async fn permission_capture_current(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		capture: &Capture,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let digest = capture.digest()?;
		let key = Self::pack(
			subspace,
			&Key::PermissionCapture {
				digest,
				resource: capture.resource.node.clone(),
				subject: capture.subject.clone(),
			},
		);
		let result = txn.get(&key, false).await;
		let value = crate::retry!(result);

		Ok(ControlFlow::Break(value.is_some()))
	}

	pub(crate) async fn permission_capture_subject_current(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		capture: &Capture,
	) -> tg::Result<ControlFlow<bool, fdb::FdbError>> {
		let Some(version) = &capture.version else {
			return Ok(ControlFlow::Break(true));
		};
		let id = tg::tag::Id::try_from(capture.subject.clone())?;
		let tag = crate::propagate!(Self::try_get_tag_with_transaction(txn, subspace, &id).await);

		Ok(ControlFlow::Break(
			tag.is_some_and(|tag| tag.version == *version),
		))
	}
}

fn index_keys(capture: &Capture, digest: [u8; 32]) -> Vec<Key> {
	let resource = &capture.resource.node;
	let subject = &capture.subject;
	let mut keys = vec![
		Key::PermissionCaptureExpiresAt {
			digest,
			expires_at: capture.expires_at,
			resource: resource.clone(),
			subject: subject.clone(),
		},
		Key::PermissionCaptureSubject {
			digest,
			resource: resource.clone(),
			subject: subject.clone(),
		},
	];
	for source in &capture.sources {
		keys.push(Key::PermissionCaptureSource {
			digest,
			resource: resource.clone(),
			source: source.clone(),
			subject: subject.clone(),
		});
	}
	keys
}
