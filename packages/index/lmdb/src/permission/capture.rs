use {crate::Db, heed as lmdb};
use {
	crate::{Index, Key, Kind},
	foundationdb_tuple as fdbt,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_index::permission::capture::{Entry, enqueue, put},
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
			std::ops::Bound::Included(begin.as_slice()),
			std::ops::Bound::Excluded(end.as_slice()),
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
	) -> tg::Result<Vec<put::Arg>> {
		let prefix = subspace.pack(&(
			Kind::PermissionCapture.to_i32().unwrap(),
			resource.to_bytes().as_ref(),
		));
		let rows = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to read captures"))?
			.map(|row| {
				row.map(|(key, value)| (key.to_vec(), value.to_vec()))
					.map_err(|error| tg::error!(!error, "failed to read a capture"))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let mut records = Vec::new();
		for (_, value) in rows {
			let contexts: Vec<put::Arg> = tangram_serialize::from_slice(&value)
				.map_err(|error| tg::error!(!error, "failed to decode captures"))?;
			records.extend(contexts);
		}

		Ok(records)
	}

	pub(crate) fn put_permission_capture_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &put::Arg,
	) -> tg::Result<()> {
		arg.validate()?;
		if !Self::permission_capture_subject_current(db, subspace, transaction, arg)? {
			return Ok(());
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
		let entries = Self::get_resource_permission_entries_for_subject_with_transaction(
			db,
			subspace,
			transaction,
			&arg.resource.node,
			&subject,
		)?;
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
				db.delete(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to delete a terminal capture"))?;
			}
			return Ok(());
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
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to read a capture"))?
			.map(<[u8]>::to_vec);
		let mut contexts: Vec<put::Arg> = value
			.as_deref()
			.map(tangram_serialize::from_slice)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to decode captures"))?
			.unwrap_or_default();
		if contexts.contains(arg) {
			return Ok(());
		}
		contexts.push(arg.clone());
		let value = tangram_serialize::to_vec(&contexts)
			.map_err(|error| tg::error!(!error, "failed to encode captures"))?;
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to write a capture"))?;
		let key = Self::pack(
			subspace,
			&Key::PermissionCaptureSubject { resource, subject },
		);
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to write a capture"))?;

		Ok(())
	}

	pub(crate) fn delete_permission_capture_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &put::Arg,
	) -> tg::Result<()> {
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
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to read a capture"))?
			.map(<[u8]>::to_vec);
		let Some(value) = value else {
			return Ok(());
		};
		let contexts: Vec<put::Arg> = tangram_serialize::from_slice(&value)
			.map_err(|error| tg::error!(!error, "failed to decode captures"))?;
		let remaining = contexts
			.into_iter()
			.filter(|context| context != arg)
			.collect::<Vec<_>>();
		if remaining.is_empty() {
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete a capture"))?;
			let key = Self::pack(
				subspace,
				&Key::PermissionCaptureSubject { resource, subject },
			);
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete a capture"))?;
		} else {
			let value = tangram_serialize::to_vec(&remaining)
				.map_err(|error| tg::error!(!error, "failed to encode captures"))?;
			db.put(transaction, &key, &value)
				.map_err(|error| tg::error!(!error, "failed to write a capture"))?;
		}

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
		let rows = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to read captures"))?
			.map(|row| {
				row.map(|(key, value)| (key.to_vec(), value.to_vec()))
					.map_err(|error| tg::error!(!error, "failed to read a capture"))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		for (key, _) in rows {
			let Key::PermissionCaptureSubject { resource, subject } = Self::unpack(subspace, &key)
				.map_err(|error| tg::error!(!error, "failed to decode a capture key"))?
			else {
				return Err(tg::error!("invalid capture subject key"));
			};
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete a capture"))?;
			let key = Self::pack(subspace, &Key::PermissionCapture { resource, subject });
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete a capture"))?;
		}

		Ok(())
	}

	pub(crate) fn permission_capture_subject_current(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		arg: &put::Arg,
	) -> tg::Result<bool> {
		let Some(version) = &arg.version else {
			return Ok(true);
		};
		let id = tg::tag::Id::try_from(arg.subject.clone())?;
		let tag = Self::try_get_tag_with_transaction(db, subspace, transaction, &id)?;

		Ok(tag.is_some_and(|tag| tag.version == *version))
	}
}
