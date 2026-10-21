use {
	crate::{Db, Index, Key, Kind},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	std::ops::Bound,
	tangram_client::prelude::*,
	tangram_index::permission::capture::{Capture, Entry, enqueue},
};

impl Index {
	pub(crate) fn enqueue_permission_capture_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &enqueue::Arg,
	) -> tg::Result<()> {
		arg.validate()?;
		let resource = &arg.resource;
		let partition = 0;
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
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to write a capture"))?;

		Ok(())
	}

	pub(crate) fn permission_capture_batch_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<Vec<Entry>> {
		if batch_size == 0 || partition_start >= partition_end {
			return Ok(Vec::new());
		}
		let begin = subspace.pack(&(
			Kind::PermissionCaptureQueue.to_i32().unwrap(),
			partition_start,
		));
		let end = subspace.pack(&(
			Kind::PermissionCaptureQueue.to_i32().unwrap(),
			partition_end,
		));
		let range = (
			Bound::Included(begin.as_slice()),
			Bound::Excluded(end.as_slice()),
		);
		let rows = db
			.range(transaction, &range)
			.map_err(|error| tg::error!(!error, "failed to read capture entries"))?
			.take(batch_size)
			.collect::<Result<Vec<_>, _>>()
			.map_err(|error| tg::error!(!error, "failed to read a capture entry"))?;
		let rows = rows.into_iter();
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

		Ok(entries)
	}

	pub(crate) fn complete_permission_capture_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		entry: &Entry,
	) -> tg::Result<()> {
		let key = Self::pack(
			subspace,
			&Key::PermissionCaptureQueue {
				partition: entry.partition,
				resource: entry.resource.clone(),
			},
		);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to read a capture"))?
			.map(<[u8]>::to_vec);
		if value.as_deref() == Some(entry.attempt.to_be_bytes().as_slice()) {
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete a capture"))?;
		}

		Ok(())
	}

	pub(crate) fn get_permission_captures_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
	) -> tg::Result<Vec<Capture>> {
		let prefix = subspace.pack(&(
			Kind::PermissionCapture.to_i32().unwrap(),
			resource.to_bytes().as_ref(),
		));
		let rows = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to read the captures"))?;
		let mut captures = Vec::new();
		for row in rows {
			let (_, value) = row.map_err(|error| tg::error!(!error, "failed to read a capture"))?;
			let capture = tangram_serialize::from_slice(value)
				.map_err(|error| tg::error!(!error, "failed to decode a capture"))?;
			captures.push(capture);
		}

		Ok(captures)
	}

	pub(crate) fn has_permission_captures_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
	) -> tg::Result<bool> {
		let prefix = subspace.pack(&(
			Kind::PermissionCapture.to_i32().unwrap(),
			resource.to_bytes().as_ref(),
		));
		let row = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to read the captures"))?
			.next()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to read a capture"))?;

		Ok(row.is_some())
	}

	pub(crate) fn has_permission_captures_for_other_subjects_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		subject: &tg::Id,
	) -> tg::Result<bool> {
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
			let range = (
				Bound::Included(begin.as_slice()),
				Bound::Excluded(end.as_slice()),
			);
			let row = db
				.range(transaction, &range)
				.map_err(|error| tg::error!(!error, "failed to read the captures"))?
				.next()
				.transpose()
				.map_err(|error| tg::error!(!error, "failed to read a capture"))?;
			if row.is_some() {
				return Ok(true);
			}
		}

		Ok(false)
	}

	pub(crate) fn has_permission_captures_for_source_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::Id,
		source: &tg::authorization::Subject,
	) -> tg::Result<bool> {
		let prefix = subspace.pack(&(
			Kind::PermissionCaptureSource.to_i32().unwrap(),
			resource.to_bytes().as_ref(),
			source.to_string(),
		));
		let row = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to read the captures"))?
			.next()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to read a capture"))?;

		Ok(row.is_some())
	}

	pub(crate) fn put_permission_capture_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		capture: &Capture,
	) -> tg::Result<()> {
		capture.validate()?;
		if !Self::permission_capture_subject_current(db, subspace, transaction, capture)? {
			return Ok(());
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
		let entries = Self::get_resource_permission_entries_for_subject_with_transaction(
			db,
			subspace,
			transaction,
			&capture.resource.node,
			&subject,
		)?;
		let requested =
			tangram_index::permission::capture::requested_permissions(&capture.resource.node)?;
		let terminal = requested.iter().all(|permission| {
			entries
				.iter()
				.any(|entry| entry.direct == Some(None) && entry.permission.implies(permission))
		});
		if terminal {
			Self::delete_permission_captures_for_resource_and_subject_with_transaction(
				db,
				subspace,
				transaction,
				&capture.resource.node,
				&capture.subject,
			)?;
			return Ok(());
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
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to read the capture"))?
			.map(<[u8]>::to_vec);
		if let Some(value) = value {
			let old: Capture = tangram_serialize::from_slice(&value)
				.map_err(|error| tg::error!(!error, "failed to decode the capture"))?;
			if old.expires_at >= capture.expires_at {
				return Ok(());
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
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the capture expiration"))?;
		}

		// Write the capture and its index keys.
		let value = tangram_serialize::to_vec(capture)
			.map_err(|error| tg::error!(!error, "failed to encode the capture"))?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to write the capture"))?;
		for key in index_keys(capture, digest) {
			let key = Self::pack(subspace, &key);
			db.put(transaction, &key, &[])
				.map_err(|error| tg::error!(!error, "failed to write the capture"))?;
		}

		Ok(())
	}

	pub(crate) fn delete_permission_capture_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		capture: &Capture,
	) -> tg::Result<()> {
		let digest = capture.digest()?;
		Self::delete_permission_capture_keys_with_transaction(
			db,
			subspace,
			transaction,
			&capture.resource.node,
			&capture.subject,
			digest,
		)?;
		Ok(())
	}

	pub(crate) fn delete_permission_captures_for_subject_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		subject: &tg::Id,
	) -> tg::Result<()> {
		let prefix = subspace.pack(&(
			Kind::PermissionCaptureSubject.to_i32().unwrap(),
			subject.to_bytes().as_ref(),
		));
		let keys = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to read the captures"))?
			.map(|row| {
				row.map(|(key, _)| key.to_vec())
					.map_err(|error| tg::error!(!error, "failed to read a capture"))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		for key in keys {
			let Key::PermissionCaptureSubject {
				digest,
				resource,
				subject,
			} = Self::unpack(subspace, &key)
				.map_err(|error| tg::error!(!error, "failed to decode a capture key"))?
			else {
				return Err(tg::error!("invalid capture subject key"));
			};
			Self::delete_permission_capture_keys_with_transaction(
				db,
				subspace,
				transaction,
				&resource,
				&subject,
				digest,
			)?;
		}

		Ok(())
	}

	pub(crate) fn delete_expired_permission_captures(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		now: i64,
		limit: usize,
	) -> tg::Result<usize> {
		if limit == 0 {
			return Ok(0);
		}
		let prefix = subspace.pack(&(Kind::PermissionCaptureExpiresAt.to_i32().unwrap(),));
		let keys = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to read the capture expirations"))?
			.take(limit)
			.map(|row| {
				row.map(|(key, _)| key.to_vec())
					.map_err(|error| tg::error!(!error, "failed to read a capture expiration"))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let mut count = 0;
		for key in keys {
			let Key::PermissionCaptureExpiresAt {
				digest,
				expires_at,
				resource,
				subject,
			} = Self::unpack(subspace, &key)
				.map_err(|error| tg::error!(!error, "failed to decode a capture expiration key"))?
			else {
				return Err(tg::error!("invalid capture expiration key"));
			};
			if expires_at > now {
				break;
			}
			Self::delete_permission_capture_keys_with_transaction(
				db,
				subspace,
				transaction,
				&resource,
				&subject,
				digest,
			)?;
			count += 1;
		}

		Ok(count)
	}

	fn delete_permission_captures_for_resource_and_subject_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		resource: &tg::Id,
		subject: &tg::Id,
	) -> tg::Result<()> {
		let prefix = subspace.pack(&(
			Kind::PermissionCapture.to_i32().unwrap(),
			resource.to_bytes().as_ref(),
			subject.to_bytes().as_ref(),
		));
		let keys = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to read the captures"))?
			.map(|row| {
				row.map(|(key, _)| key.to_vec())
					.map_err(|error| tg::error!(!error, "failed to read a capture"))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		for key in keys {
			let Key::PermissionCapture { digest, .. } = Self::unpack(subspace, &key)
				.map_err(|error| tg::error!(!error, "failed to decode a capture key"))?
			else {
				return Err(tg::error!("invalid capture key"));
			};
			Self::delete_permission_capture_keys_with_transaction(
				db,
				subspace,
				transaction,
				resource,
				subject,
				digest,
			)?;
		}

		Ok(())
	}

	fn delete_permission_capture_keys_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		resource: &tg::Id,
		subject: &tg::Id,
		digest: [u8; 32],
	) -> tg::Result<()> {
		// Read the capture to find its index keys.
		let key = Self::pack(
			subspace,
			&Key::PermissionCapture {
				digest,
				resource: resource.clone(),
				subject: subject.clone(),
			},
		);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to read the capture"))?
			.map(<[u8]>::to_vec);
		let Some(value) = value else {
			return Ok(());
		};
		let capture: Capture = tangram_serialize::from_slice(&value)
			.map_err(|error| tg::error!(!error, "failed to decode the capture"))?;

		// Delete the capture and its index keys.
		db.delete(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to delete the capture"))?;
		for key in index_keys(&capture, digest) {
			let key = Self::pack(subspace, &key);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the capture"))?;
		}

		Ok(())
	}

	pub(crate) fn permission_capture_current(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		capture: &Capture,
	) -> tg::Result<bool> {
		let digest = capture.digest()?;
		let key = Self::pack(
			subspace,
			&Key::PermissionCapture {
				digest,
				resource: capture.resource.node.clone(),
				subject: capture.subject.clone(),
			},
		);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to read the capture"))?;

		Ok(value.is_some())
	}

	pub(crate) fn permission_capture_subject_current(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		capture: &Capture,
	) -> tg::Result<bool> {
		let Some(version) = &capture.version else {
			return Ok(true);
		};
		let id = tg::tag::Id::try_from(capture.subject.clone())?;
		let tag = Self::try_get_tag_with_transaction(db, subspace, transaction, &id)?;

		Ok(tag.is_some_and(|tag| tag.version == *version))
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
