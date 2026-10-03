use {
	crate::database::Database,
	fjall::Readable as _,
	std::{collections::BTreeMap, ops::Bound, sync::MutexGuard},
	tangram_client::prelude::*,
};

type Entry = (Box<[u8]>, Box<[u8]>);

pub struct Transaction<'a> {
	pub(super) database: &'a Database,
	// The overlay preserves reads of earlier writes within a batch.
	pub(super) pending: Option<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
	pub(super) snapshot: fjall::Snapshot,
	pub(super) writer_guard: Option<MutexGuard<'a, ()>>,
}

impl<'db> Transaction<'db> {
	pub(super) fn get(&self, key: &[u8]) -> tg::Result<Option<Vec<u8>>> {
		// Read the pending writes.
		if let Some(value) = self.pending.as_ref().and_then(|pending| pending.get(key)) {
			return Ok(value.clone());
		}

		// Read the database.
		let value = self
			.snapshot
			.get(&self.database.keyspace, key)
			.map_err(|error| tg::error!(!error, "failed to get a cache entry"))?
			.map(|value| value.to_vec());

		Ok(value)
	}

	pub(super) fn put(&mut self, key: &[u8], value: &[u8]) -> tg::Result<()> {
		let pending = self
			.pending
			.as_mut()
			.ok_or_else(|| tg::error!("the transaction is read only"))?;
		pending.insert(key.to_vec(), Some(value.to_vec()));
		Ok(())
	}

	pub(super) fn delete(&mut self, key: &[u8]) -> tg::Result<()> {
		let pending = self
			.pending
			.as_mut()
			.ok_or_else(|| tg::error!("the transaction is read only"))?;
		pending.insert(key.to_vec(), None);
		Ok(())
	}

	pub(super) fn commit(self) -> tg::Result<()> {
		// Create the write batch.
		let pending = self
			.pending
			.as_ref()
			.ok_or_else(|| tg::error!("the transaction is read only"))?;
		let mut batch = self.database.db.batch();
		for (key, value) in pending {
			match value {
				None => batch.remove(&self.database.keyspace, key.as_slice()),
				Some(value) => {
					batch.insert(&self.database.keyspace, key.as_slice(), value.as_slice());
				},
			}
		}

		// Leave the journal buffered because the cache does not require crash recovery.
		batch
			.commit()
			.map_err(|error| tg::error!(!error, "failed to write the cache batch"))?;

		Ok(())
	}

	pub(super) fn prefix_iter(
		&self,
		prefix: &[u8],
	) -> impl Iterator<Item = tg::Result<Entry>> + '_ {
		let prefix = prefix.to_vec();
		let range = (Bound::Included(prefix.as_slice()), Bound::Unbounded);
		self.range(&range).take_while(move |entry| {
			entry
				.as_ref()
				.map_or(true, |(key, _)| key.starts_with(&prefix))
		})
	}

	pub(super) fn get_greater_than(&self, key: &[u8]) -> tg::Result<Option<Entry>> {
		let range = (Bound::Excluded(key), Bound::Unbounded);
		let entry = self.range(&range).next().transpose()?;
		Ok(entry)
	}

	pub(super) fn get_greater_than_or_equal_to(&self, key: &[u8]) -> tg::Result<Option<Entry>> {
		let range = (Bound::Included(key), Bound::Unbounded);
		let entry = self.range(&range).next().transpose()?;
		Ok(entry)
	}

	pub(super) fn range<'a>(
		&'a self,
		range: &(Bound<&[u8]>, Bound<&[u8]>),
	) -> impl Iterator<Item = tg::Result<Entry>> + 'a + use<'a, 'db> {
		// Read the database entries in the range.
		let bounds = (range.0.map(<[u8]>::to_vec), range.1.map(<[u8]>::to_vec));
		let pending = self.pending.as_ref();
		let mut entries = self
			.snapshot
			.range(&self.database.keyspace, bounds)
			.map(|entry| {
				let (key, value) = entry
					.into_inner()
					.map_err(|error| tg::error!(!error, "failed to iterate the cache"))?;
				Ok((
					key.to_vec().into_boxed_slice(),
					value.to_vec().into_boxed_slice(),
				))
			})
			.filter(move |entry: &tg::Result<Entry>| {
				entry.as_ref().map_or(true, |(key, _)| {
					pending.is_none_or(|pending| !pending.contains_key(key.as_ref()))
				})
			})
			.peekable();

		// Read the pending writes in the range.
		let bounds = (range.0.map(<[u8]>::to_vec), range.1.map(<[u8]>::to_vec));
		let mut writes = pending
			.into_iter()
			.flat_map(move |pending| pending.range(bounds.clone()))
			.filter_map(|(key, value)| value.as_ref().map(|value| (key, value)))
			.peekable();

		// Merge the database entries and pending writes in key order.
		std::iter::from_fn(move || {
			let take_write = match (entries.peek(), writes.peek()) {
				(Some(Ok((key, _))), Some((write_key, _))) => write_key.as_slice() < key.as_ref(),
				(None, Some(_)) => true,
				_ => false,
			};
			if take_write {
				writes.next().map(|(key, value)| {
					Ok((
						key.clone().into_boxed_slice(),
						value.clone().into_boxed_slice(),
					))
				})
			} else {
				entries.next()
			}
		})
	}

	pub(super) fn get_lower_than_or_equal_to(&self, key: &[u8]) -> tg::Result<Option<Entry>> {
		let pending = self.pending.as_ref();
		let bounds = (Bound::Unbounded, Bound::Included(key));
		let mut entries = self
			.snapshot
			.range::<&[u8], _>(&self.database.keyspace, bounds)
			.rev()
			.map(|entry| {
				let (key, value) = entry
					.into_inner()
					.map_err(|error| tg::error!(!error, "failed to seek the cache"))?;
				Ok((
					key.to_vec().into_boxed_slice(),
					value.to_vec().into_boxed_slice(),
				))
			})
			.filter(|entry: &tg::Result<Entry>| {
				entry.as_ref().map_or(true, |(key, _)| {
					pending.is_none_or(|pending| !pending.contains_key(key.as_ref()))
				})
			});
		let entry = entries.next().transpose()?;
		let write = pending
			.into_iter()
			.flat_map(|pending| {
				pending
					.range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
					.rev()
			})
			.find_map(|(key, value)| value.as_ref().map(|value| (key, value)));

		// Select the last visible entry.
		let entry = match (entry, write) {
			(Some(entry), Some((key, value))) if key.as_slice() > entry.0.as_ref() => Some((
				key.clone().into_boxed_slice(),
				value.clone().into_boxed_slice(),
			)),
			(None, Some((key, value))) => Some((
				key.clone().into_boxed_slice(),
				value.clone().into_boxed_slice(),
			)),
			(entry, _) => entry,
		};

		Ok(entry)
	}

	#[cfg(test)]
	#[must_use]
	pub(super) fn id(&self) -> u64 {
		self.snapshot.seqno()
	}
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		crate::{Cache, Config},
	};

	#[test]
	fn batches_preserve_read_your_writes_and_snapshot_isolation() {
		let directory = tangram_util::fs::Temp::new().unwrap();
		let config = Config {
			path: directory.path().to_owned(),
			read_batch_size: 64,
			read_concurrency: 1,
			write_batch_size: 8_000,
		};
		let cache = Cache::new(&config).unwrap();
		let mut transaction = cache.db.write_transaction();
		for key in [b"a", b"c", b"e"] {
			transaction.put(key, b"old").unwrap();
		}
		transaction.commit().unwrap();

		// Observe pending puts and deletes through both point reads and ordered seeks.
		let snapshot = cache.read_transaction();
		let mut transaction = cache.db.write_transaction();
		transaction.put(b"b", b"new").unwrap();
		transaction.put(b"c", b"new").unwrap();
		transaction.delete(b"e").unwrap();
		assert_eq!(transaction.get(b"c").unwrap(), Some(b"new".to_vec()));
		assert_eq!(transaction.get(b"e").unwrap(), None);
		let range = (
			Bound::Included(b"a".as_slice()),
			Bound::Excluded(b"e".as_slice()),
		);
		let entries = transaction
			.range(&range)
			.collect::<tg::Result<Vec<_>>>()
			.unwrap();
		let keys = entries
			.iter()
			.map(|(key, _)| key.as_ref())
			.collect::<Vec<_>>();
		assert_eq!(keys, [b"a", b"b", b"c"]);
		assert_eq!(
			transaction
				.get_lower_than_or_equal_to(b"d")
				.unwrap()
				.unwrap()
				.0
				.as_ref(),
			b"c"
		);
		assert_eq!(
			transaction.get_greater_than_or_equal_to(b"e").unwrap(),
			None
		);
		transaction.commit().unwrap();

		// Preserve the old snapshot while subsequent reads observe the commit.
		assert_eq!(snapshot.get(b"c").unwrap(), Some(b"old".to_vec()));
		assert_eq!(snapshot.get(b"e").unwrap(), Some(b"old".to_vec()));
		let transaction = cache.read_transaction();
		assert_eq!(transaction.get(b"c").unwrap(), Some(b"new".to_vec()));
		assert_eq!(transaction.get(b"e").unwrap(), None);

		// Discard an uncommitted batch.
		let mut transaction = cache.db.write_transaction();
		transaction.delete(b"c").unwrap();
		drop(transaction);
		assert_eq!(
			cache.read_transaction().get(b"c").unwrap(),
			Some(b"new".to_vec())
		);
	}
}
