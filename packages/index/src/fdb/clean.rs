use {
	super::{CacheEntryValue, Index, Key, ObjectValue, ProcessValue, subspace},
	crate::{CleanOutput, ProcessObjectKind},
	foundationdb as fdb,
	foundationdb_tuple::TuplePack as _,
	futures::TryStreamExt as _,
	num_traits::FromPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn clean(&self, max_touched_at: i64, n: usize) -> tg::Result<CleanOutput> {
		let mut output = CleanOutput::default();
		let mut remaining = n;

		// Phase 1: Clean cache entries.
		if remaining > 0 {
			let cleaned = self.clean_cache_entries(max_touched_at, remaining).await?;
			remaining = remaining.saturating_sub(cleaned.cache_entries.len());
			output.cache_entries.extend(cleaned.cache_entries);
		}

		// Phase 2: Clean objects.
		if remaining > 0 {
			let cleaned = self.clean_objects(max_touched_at, remaining).await?;
			remaining = remaining.saturating_sub(cleaned.objects.len());
			output.bytes += cleaned.bytes;
			output.objects.extend(cleaned.objects);
		}

		// Phase 3: Clean processes.
		if remaining > 0 {
			let cleaned = self.clean_processes(max_touched_at, remaining).await?;
			output.processes.extend(cleaned.processes);
		}

		Ok(output)
	}

	async fn clean_cache_entries(&self, max_touched_at: i64, n: usize) -> tg::Result<CleanOutput> {
		let mut output = CleanOutput::default();

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Build range for CleanQueueCacheEntry where touched_at <= max_touched_at.
		let start = (subspace::CLEAN_QUEUE_CACHE_ENTRY,).pack_to_vec();
		let end = (subspace::CLEAN_QUEUE_CACHE_ENTRY, max_touched_at + 1).pack_to_vec();

		// Scan the clean queue for cache entries.
		let entries: Vec<_> = txn
			.get_ranges_keyvalues(
				fdb::RangeOption {
					begin: fdb::KeySelector::first_greater_or_equal(start),
					end: fdb::KeySelector::first_greater_or_equal(end),
					limit: Some(n),
					mode: fdb::options::StreamingMode::WantAll,
					..Default::default()
				},
				false,
			)
			.try_collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to scan clean queue"))?;

		// Extract the data from entries.
		let mut ids_to_delete = Vec::new();
		for entry in &entries {
			let key_bytes = entry.key();
			let (_, _, id_bytes): (i32, i64, Vec<u8>) = foundationdb_tuple::unpack(key_bytes)
				.map_err(|source| tg::error!(!source, "failed to unpack clean queue key"))?;
			let id = tg::artifact::Id::from_slice(&id_bytes)
				.map_err(|source| tg::error!(!source, "failed to parse artifact id"))?;
			ids_to_delete.push((key_bytes.to_vec(), id));
		}
		drop(entries);

		for (key_bytes, id) in ids_to_delete {
			// Delete the cache entry.
			let cache_entry_key = Key::CacheEntry(&id).pack_to_vec();
			txn.clear(&cache_entry_key);

			// Delete the clean queue entry.
			txn.clear(&key_bytes);

			output.cache_entries.push(id);
		}

		if !output.cache_entries.is_empty() {
			txn.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		}

		Ok(output)
	}

	async fn clean_objects(&self, max_touched_at: i64, n: usize) -> tg::Result<CleanOutput> {
		let mut output = CleanOutput::default();

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Build range for CleanQueueObject where touched_at <= max_touched_at.
		let start = (subspace::CLEAN_QUEUE_OBJECT,).pack_to_vec();
		let end = (subspace::CLEAN_QUEUE_OBJECT, max_touched_at + 1).pack_to_vec();

		// Scan the clean queue for objects.
		let entries: Vec<_> = txn
			.get_ranges_keyvalues(
				fdb::RangeOption {
					begin: fdb::KeySelector::first_greater_or_equal(start),
					end: fdb::KeySelector::first_greater_or_equal(end),
					limit: Some(n),
					mode: fdb::options::StreamingMode::WantAll,
					..Default::default()
				},
				false,
			)
			.try_collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to scan clean queue"))?;

		// Extract the data from entries.
		let mut ids_to_process = Vec::new();
		for entry in &entries {
			let key_bytes = entry.key();
			let (_, _, id_bytes): (i32, i64, Vec<u8>) = foundationdb_tuple::unpack(key_bytes)
				.map_err(|source| tg::error!(!source, "failed to unpack clean queue key"))?;
			let id = tg::object::Id::from_slice(&id_bytes)
				.map_err(|source| tg::error!(!source, "failed to parse object id"))?;
			ids_to_process.push((key_bytes.to_vec(), id));
		}
		drop(entries);

		for (key_bytes, id) in ids_to_process {
			// Get the object value to find children and cache entry.
			let object_key = Key::Object(&id).pack_to_vec();
			let object_value = txn
				.get(&object_key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get object"))?;
			let Some(object_value) = object_value else {
				// The object was already deleted, just clean up the queue entry.
				txn.clear(&key_bytes);
				continue;
			};
			let object_value = tangram_serialize::from_slice::<ObjectValue>(&object_value)
				.map_err(|source| tg::error!(!source, "failed to deserialize object value"))?;

			// Track bytes from metadata.
			if let Some(count) = object_value.metadata.subtree.count {
				output.bytes += count;
			}

			// Scan and process child objects via ObjectChild { object=id, * }.
			let child_start = (subspace::OBJECT_CHILD, id.to_bytes().as_ref()).pack_to_vec();
			let child_end = (
				subspace::OBJECT_CHILD,
				id.to_bytes().as_ref(),
				[0xffu8; 33].as_slice(),
			)
				.pack_to_vec();
			let child_entries: Vec<_> = txn
				.get_ranges_keyvalues(
					fdb::RangeOption {
						begin: fdb::KeySelector::first_greater_or_equal(child_start),
						end: fdb::KeySelector::first_greater_or_equal(child_end),
						mode: fdb::options::StreamingMode::WantAll,
						..Default::default()
					},
					false,
				)
				.try_collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to scan object children"))?;

			// Extract child data.
			let mut child_ids = Vec::new();
			for child_entry in &child_entries {
				let (_, _, child_id_bytes): (i32, Vec<u8>, Vec<u8>) =
					foundationdb_tuple::unpack(child_entry.key())
						.map_err(|source| tg::error!(!source, "failed to unpack child key"))?;
				let child_id = tg::object::Id::from_slice(&child_id_bytes)
					.map_err(|source| tg::error!(!source, "failed to parse child object id"))?;
				child_ids.push((child_entry.key().to_vec(), child_id));
			}
			drop(child_entries);

			for (child_key, child_id) in child_ids {
				// Decrement reference count of the child object.
				self.decrement_object_reference_count(&txn, &child_id)
					.await?;

				// Delete the ObjectChild key.
				txn.clear(&child_key);

				// Delete the ObjectChildReverse key.
				let reverse_key = Key::ObjectChildReverse {
					child: &child_id,
					parent: &id,
				}
				.pack_to_vec();
				txn.clear(&reverse_key);
			}

			// If cache entry present, decrement its reference count.
			if let Some(cache_entry_bytes) = &object_value.cache_entry {
				let cache_entry_id = tg::artifact::Id::from_slice(cache_entry_bytes)
					.map_err(|source| tg::error!(!source, "failed to parse cache entry id"))?;

				self.decrement_cache_entry_reference_count(&txn, &cache_entry_id)
					.await?;

				// Delete ObjectCacheEntry key.
				let object_cache_entry_key = Key::ObjectCacheEntry {
					cache_entry: &cache_entry_id,
					object: &id,
				}
				.pack_to_vec();
				txn.clear(&object_cache_entry_key);
			}

			// Delete the object entity.
			txn.clear(&object_key);

			// Delete the clean queue entry.
			txn.clear(&key_bytes);

			output.objects.push(id);
		}

		if !output.objects.is_empty() {
			txn.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		}

		Ok(output)
	}

	async fn clean_processes(&self, max_touched_at: i64, n: usize) -> tg::Result<CleanOutput> {
		let mut output = CleanOutput::default();

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Build range for CleanQueueProcess where touched_at <= max_touched_at.
		let start = (subspace::CLEAN_QUEUE_PROCESS,).pack_to_vec();
		let end = (subspace::CLEAN_QUEUE_PROCESS, max_touched_at + 1).pack_to_vec();

		// Scan the clean queue for processes.
		let entries: Vec<_> = txn
			.get_ranges_keyvalues(
				fdb::RangeOption {
					begin: fdb::KeySelector::first_greater_or_equal(start),
					end: fdb::KeySelector::first_greater_or_equal(end),
					limit: Some(n),
					mode: fdb::options::StreamingMode::WantAll,
					..Default::default()
				},
				false,
			)
			.try_collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to scan clean queue"))?;

		// Extract the data from entries.
		let mut ids_to_process = Vec::new();
		for entry in &entries {
			let key_bytes = entry.key();
			let (_, _, id_bytes): (i32, i64, Vec<u8>) = foundationdb_tuple::unpack(key_bytes)
				.map_err(|source| tg::error!(!source, "failed to unpack clean queue key"))?;
			let id = tg::process::Id::from_slice(&id_bytes)
				.map_err(|source| tg::error!(!source, "failed to parse process id"))?;
			ids_to_process.push((key_bytes.to_vec(), id));
		}
		drop(entries);

		for (key_bytes, id) in ids_to_process {
			// Get the process value.
			let process_key = Key::Process(&id).pack_to_vec();
			let process_value = txn
				.get(&process_key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get process"))?;
			if process_value.is_none() {
				// The process was already deleted, just clean up the queue entry.
				txn.clear(&key_bytes);
				continue;
			}

			// Scan and process child processes via ProcessChild { process=id, * }.
			let child_start = (subspace::PROCESS_CHILD, id.to_bytes().as_ref()).pack_to_vec();
			let child_end =
				(subspace::PROCESS_CHILD, id.to_bytes().as_ref(), u64::MAX).pack_to_vec();
			let child_entries: Vec<_> = txn
				.get_ranges_keyvalues(
					fdb::RangeOption {
						begin: fdb::KeySelector::first_greater_or_equal(child_start),
						end: fdb::KeySelector::first_greater_or_equal(child_end),
						mode: fdb::options::StreamingMode::WantAll,
						..Default::default()
					},
					false,
				)
				.try_collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to scan process children"))?;

			// Extract child data.
			let mut child_data = Vec::new();
			for child_entry in &child_entries {
				let child_id_bytes = child_entry.value();
				let child_id = tg::process::Id::from_slice(child_id_bytes)
					.map_err(|source| tg::error!(!source, "failed to parse child process id"))?;
				child_data.push((child_entry.key().to_vec(), child_id));
			}
			drop(child_entries);

			for (child_key, child_id) in child_data {
				// Decrement reference count of the child process.
				self.decrement_process_reference_count(&txn, &child_id)
					.await?;

				// Delete the ProcessChild key.
				txn.clear(&child_key);

				// Delete the ProcessChildReverse key.
				let reverse_key = Key::ProcessChildReverse {
					child: &child_id,
					parent: &id,
				}
				.pack_to_vec();
				txn.clear(&reverse_key);
			}

			// Scan and delete ProcessChildDedup { process=id, * } keys.
			let dedup_start = (subspace::PROCESS_CHILD_DEDUP, id.to_bytes().as_ref()).pack_to_vec();
			let dedup_end = (
				subspace::PROCESS_CHILD_DEDUP,
				id.to_bytes().as_ref(),
				[0xffu8; 33].as_slice(),
			)
				.pack_to_vec();
			let dedup_entries: Vec<_> = txn
				.get_ranges_keyvalues(
					fdb::RangeOption {
						begin: fdb::KeySelector::first_greater_or_equal(dedup_start),
						end: fdb::KeySelector::first_greater_or_equal(dedup_end),
						mode: fdb::options::StreamingMode::WantAll,
						..Default::default()
					},
					false,
				)
				.try_collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to scan process child dedup"))?;

			// Extract dedup keys.
			let dedup_keys: Vec<_> = dedup_entries.iter().map(|e| e.key().to_vec()).collect();
			drop(dedup_entries);

			for dedup_key in dedup_keys {
				txn.clear(&dedup_key);
			}

			// Scan and process objects via ProcessObject { process=id, *, * }.
			let object_start = (subspace::PROCESS_OBJECT, id.to_bytes().as_ref()).pack_to_vec();
			let object_end = (
				subspace::PROCESS_OBJECT,
				id.to_bytes().as_ref(),
				[0xffu8; 33].as_slice(),
				i32::MAX,
			)
				.pack_to_vec();
			let object_entries: Vec<_> = txn
				.get_ranges_keyvalues(
					fdb::RangeOption {
						begin: fdb::KeySelector::first_greater_or_equal(object_start),
						end: fdb::KeySelector::first_greater_or_equal(object_end),
						mode: fdb::options::StreamingMode::WantAll,
						..Default::default()
					},
					false,
				)
				.try_collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to scan process objects"))?;

			// Extract object data.
			let mut object_data = Vec::new();
			for object_entry in &object_entries {
				let (_, _, object_id_bytes, kind_i32): (i32, Vec<u8>, Vec<u8>, i32) =
					foundationdb_tuple::unpack(object_entry.key()).map_err(|source| {
						tg::error!(!source, "failed to unpack process object key")
					})?;
				let object_id = tg::object::Id::from_slice(&object_id_bytes)
					.map_err(|source| tg::error!(!source, "failed to parse object id"))?;
				let kind = ProcessObjectKind::from_i32(kind_i32)
					.ok_or_else(|| tg::error!("invalid process object kind"))?;
				object_data.push((object_entry.key().to_vec(), object_id, kind));
			}
			drop(object_entries);

			for (object_key, object_id, kind) in object_data {
				// Decrement reference count of the object.
				self.decrement_object_reference_count(&txn, &object_id)
					.await?;

				// Delete the ProcessObject key.
				txn.clear(&object_key);

				// Delete the ProcessObjectReverse key.
				let reverse_key = Key::ProcessObjectReverse {
					object: &object_id,
					process: &id,
					kind,
				}
				.pack_to_vec();
				txn.clear(&reverse_key);
			}

			// Delete the process entity.
			txn.clear(&process_key);

			// Delete the clean queue entry.
			txn.clear(&key_bytes);

			output.processes.push(id);
		}

		if !output.processes.is_empty() {
			txn.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		}

		Ok(output)
	}

	async fn decrement_object_reference_count(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
	) -> tg::Result<()> {
		let key = Key::Object(id).pack_to_vec();
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object"))?;
		let Some(value) = value else {
			return Ok(());
		};
		let mut value = tangram_serialize::from_slice::<ObjectValue>(&value)
			.map_err(|source| tg::error!(!source, "failed to deserialize object value"))?;

		// Decrement reference count.
		let new_count = value.reference_count.unwrap_or(0).saturating_sub(1);
		value.reference_count = Some(new_count);

		// If reference count becomes 0, add to clean queue.
		if new_count == 0 {
			let clean_queue_key = Key::CleanQueueObject {
				touched_at: value.touched_at,
				id,
			}
			.pack_to_vec();
			txn.set(&clean_queue_key, &[]);
		}

		// Update the object value.
		let serialized = tangram_serialize::to_vec(&value)
			.map_err(|source| tg::error!(!source, "failed to serialize object value"))?;
		txn.set(&key, &serialized);

		Ok(())
	}

	async fn decrement_process_reference_count(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
	) -> tg::Result<()> {
		let key = Key::Process(id).pack_to_vec();
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process"))?;
		let Some(value) = value else {
			return Ok(());
		};
		let mut value = tangram_serialize::from_slice::<ProcessValue>(&value)
			.map_err(|source| tg::error!(!source, "failed to deserialize process value"))?;

		// Decrement reference count.
		let new_count = value.reference_count.unwrap_or(0).saturating_sub(1);
		value.reference_count = Some(new_count);

		// If reference count becomes 0, add to clean queue.
		if new_count == 0 {
			let clean_queue_key = Key::CleanQueueProcess {
				touched_at: value.touched_at,
				id,
			}
			.pack_to_vec();
			txn.set(&clean_queue_key, &[]);
		}

		// Update the process value.
		let serialized = tangram_serialize::to_vec(&value)
			.map_err(|source| tg::error!(!source, "failed to serialize process value"))?;
		txn.set(&key, &serialized);

		Ok(())
	}

	async fn decrement_cache_entry_reference_count(
		&self,
		txn: &fdb::Transaction,
		id: &tg::artifact::Id,
	) -> tg::Result<()> {
		let key = Key::CacheEntry(id).pack_to_vec();
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get cache entry"))?;
		let Some(value) = value else {
			return Ok(());
		};
		let mut value = tangram_serialize::from_slice::<CacheEntryValue>(&value)
			.map_err(|source| tg::error!(!source, "failed to deserialize cache entry value"))?;

		// Decrement reference count.
		let new_count = value.reference_count.unwrap_or(0).saturating_sub(1);
		value.reference_count = Some(new_count);

		// If reference count becomes 0, add to clean queue.
		if new_count == 0 {
			let clean_queue_key = Key::CleanQueueCacheEntry {
				touched_at: value.touched_at,
				id,
			}
			.pack_to_vec();
			txn.set(&clean_queue_key, &[]);
		}

		// Update the cache entry value.
		let serialized = tangram_serialize::to_vec(&value)
			.map_err(|source| tg::error!(!source, "failed to serialize cache entry value"))?;
		txn.set(&key, &serialized);

		Ok(())
	}
}
