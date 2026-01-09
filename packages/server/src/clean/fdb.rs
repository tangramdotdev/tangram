use {
	super::{InnerOutput, Server},
	crate::index::fdb::{CacheEntryValue, Key, ObjectValue, ProcessValue},
	foundationdb::{self as fdb, RangeOption, options::StreamingMode},
	foundationdb_tuple::TuplePack as _,
	std::sync::Arc,
	tangram_client as tg,
};

// Helper to create prefix range for cache entries by touched_at.
fn cache_entry_by_touched_at_range(max_touched_at: i64) -> (Vec<u8>, Vec<u8>) {
	let start = (2, 0i64).pack_to_vec();
	let mut end = (2, max_touched_at).pack_to_vec();
	end.push(0xFF);
	(start, end)
}

// Helper to create prefix range for objects by touched_at with reference_count_zero=true.
fn object_by_touched_at_range(max_touched_at: i64) -> (Vec<u8>, Vec<u8>) {
	let start = (5, 0i64, true).pack_to_vec();
	let mut end = (5, max_touched_at, true).pack_to_vec();
	end.push(0xFF);
	(start, end)
}

// Helper to create prefix range for processes by touched_at with reference_count_zero=true.
fn process_by_touched_at_range(max_touched_at: i64) -> (Vec<u8>, Vec<u8>) {
	let start = (10, 0i64, true).pack_to_vec();
	let mut end = (10, max_touched_at, true).pack_to_vec();
	end.push(0xFF);
	(start, end)
}

// Helper to create prefix range for object children (parent -> child).
fn object_children_prefix(parent: &tg::object::Id) -> (Vec<u8>, Vec<u8>) {
	let start = (6, parent.to_bytes().as_ref()).pack_to_vec();
	let mut end = start.clone();
	end.push(0xFF);
	(start, end)
}

// Helper to create prefix range for process children (parent -> position).
fn process_children_prefix(parent: &tg::process::Id) -> (Vec<u8>, Vec<u8>) {
	let start = (11, parent.to_bytes().as_ref()).pack_to_vec();
	let mut end = start.clone();
	end.push(0xFF);
	(start, end)
}

// Helper to create prefix range for process objects (process -> object -> kind).
fn process_objects_prefix(process: &tg::process::Id) -> (Vec<u8>, Vec<u8>) {
	let start = (13, process.to_bytes().as_ref()).pack_to_vec();
	let mut end = start.clone();
	end.push(0xFF);
	(start, end)
}

// Helper to check if reference_count is zero.
fn is_ref_count_zero(reference_count: Option<i64>) -> bool {
	reference_count == Some(0)
}

// Helper to decrement reference count.
fn decrement_ref_count(reference_count: Option<i64>) -> Option<i64> {
	reference_count.map(|n| n.saturating_sub(1))
}

impl Server {
	pub(super) async fn cleaner_task_inner_fdb(
		&self,
		database: &Arc<fdb::Database>,
		max_touched_at: i64,
		mut n: usize,
	) -> tg::Result<InnerOutput> {
		let txn = database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		// Clean cache entries with reference_count=0 and old touched_at.
		let mut cache_entries = Vec::new();
		{
			let (start, end) = cache_entry_by_touched_at_range(max_touched_at);
			let range = RangeOption {
				mode: StreamingMode::WantAll,
				limit: Some(n),
				..RangeOption::from((start.as_slice(), end.as_slice()))
			};
			let entries = txn
				.get_range(&range, 0, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get range"))?;

			for entry in entries {
				let key_bytes = entry.key();
				// Parse the id from the key (2, touched_at, id).
				if key_bytes.len() < 10 {
					continue;
				}
				let id_start = 10;
				if key_bytes.len() <= id_start {
					continue;
				}
				let id_bytes = &key_bytes[id_start..];
				let actual_id_bytes = if id_bytes.len() > 2 && id_bytes[0] == 0x01 {
					&id_bytes[1..id_bytes.len().saturating_sub(1)]
				} else {
					id_bytes
				};
				let Ok(id) = tg::artifact::Id::from_slice(actual_id_bytes) else {
					continue;
				};

				// Re-check reference count by getting the cache entry.
				let cache_key = Key::CacheEntry(&id).pack_to_vec();
				if let Some(value_bytes) = txn
					.get(&cache_key, false)
					.await
					.map_err(|source| tg::error!(!source, "failed to get cache entry"))?
				{
					let value: CacheEntryValue = tangram_serialize::from_slice(&value_bytes)
						.map_err(|source| {
							tg::error!(!source, "failed to deserialize cache entry")
						})?;

					if is_ref_count_zero(value.reference_count) {
						// Delete the cache entry.
						txn.clear(&cache_key);

						// Delete the secondary index.
						let touched_at_key = Key::CacheEntryByTouchedAt {
							touched_at: value.touched_at,
							id: &id,
						}
						.pack_to_vec();
						txn.clear(&touched_at_key);

						cache_entries.push(id);
						n = n.saturating_sub(1);
					}
				}
			}
		}

		// Clean objects with reference_count=0 and old touched_at.
		let mut bytes = 0u64;
		let mut objects = Vec::new();
		{
			let (start, end) = object_by_touched_at_range(max_touched_at);
			let range = RangeOption {
				mode: StreamingMode::WantAll,
				limit: Some(n),
				..RangeOption::from((start.as_slice(), end.as_slice()))
			};
			let entries = txn
				.get_range(&range, 0, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get range"))?;

			for entry in entries {
				let key_bytes = entry.key();
				if key_bytes.len() < 12 {
					continue;
				}
				let id_start = 11;
				if key_bytes.len() <= id_start {
					continue;
				}
				let id_bytes = &key_bytes[id_start..];
				let actual_id_bytes = if id_bytes.len() > 2 && id_bytes[0] == 0x01 {
					&id_bytes[1..id_bytes.len().saturating_sub(1)]
				} else {
					id_bytes
				};
				let Ok(id) = tg::object::Id::from_slice(actual_id_bytes) else {
					continue;
				};

				// Get the object to check reference count.
				let object_key = Key::Object(&id).pack_to_vec();
				if let Some(value_bytes) = txn
					.get(&object_key, false)
					.await
					.map_err(|source| tg::error!(!source, "failed to get object"))?
				{
					let value: ObjectValue = tangram_serialize::from_slice(&value_bytes)
						.map_err(|source| tg::error!(!source, "failed to deserialize object"))?;

					if is_ref_count_zero(value.reference_count) {
						// Decrement reference counts of children.
						let (children_start, children_end) = object_children_prefix(&id);
						let children_range = RangeOption {
							mode: StreamingMode::WantAll,
							..RangeOption::from((
								children_start.as_slice(),
								children_end.as_slice(),
							))
						};
						let children = txn
							.get_range(&children_range, 0, false)
							.await
							.map_err(|source| tg::error!(!source, "failed to get children"))?;

						// Collect child IDs and keys before processing to avoid holding FdbKeyValue across awaits.
						let child_data: Vec<(Vec<u8>, Option<tg::object::Id>)> = children
							.iter()
							.map(|entry| {
								let key = entry.key().to_vec();
								let child_id = if key.len() > 35 {
									let child_id_bytes = &key[35..];
									let actual_child_id_bytes = if child_id_bytes.len() > 2
										&& child_id_bytes[0] == 0x01
									{
										&child_id_bytes[1..child_id_bytes.len().saturating_sub(1)]
									} else {
										child_id_bytes
									};
									tg::object::Id::from_slice(actual_child_id_bytes).ok()
								} else {
									None
								};
								(key, child_id)
							})
							.collect();
						drop(children);

						for (child_key, child_id_opt) in child_data {
							if let Some(child_id) = child_id_opt {
								let child_object_key = Key::Object(&child_id).pack_to_vec();
								if let Some(child_value_bytes) =
									txn.get(&child_object_key, false).await.map_err(|source| {
										tg::error!(!source, "failed to get child object")
									})? {
									let mut child_value: ObjectValue =
										tangram_serialize::from_slice(&child_value_bytes).map_err(
											|source| {
												tg::error!(
													!source,
													"failed to deserialize child object"
												)
											},
										)?;

									let old_ref_zero =
										is_ref_count_zero(child_value.reference_count);
									let old_index_key = Key::ObjectByTouchedAt {
										touched_at: child_value.touched_at,
										reference_count_zero: old_ref_zero,
										id: &child_id,
									}
									.pack_to_vec();
									txn.clear(&old_index_key);

									child_value.reference_count =
										decrement_ref_count(child_value.reference_count);

									let new_ref_zero =
										is_ref_count_zero(child_value.reference_count);
									let new_index_key = Key::ObjectByTouchedAt {
										touched_at: child_value.touched_at,
										reference_count_zero: new_ref_zero,
										id: &child_id,
									}
									.pack_to_vec();
									txn.set(&new_index_key, &[]);

									let updated_bytes = tangram_serialize::to_vec(&child_value)
										.map_err(|source| {
											tg::error!(!source, "failed to serialize child object")
										})?;
									txn.set(&child_object_key, &updated_bytes);
								}

								let reverse_key = Key::ObjectChildrenReverse {
									child: &child_id,
									parent: &id,
								}
								.pack_to_vec();
								txn.clear(&reverse_key);
							}

							txn.clear(&child_key);
						}

						// Delete the object.
						txn.clear(&object_key);

						// Delete the secondary index.
						let touched_at_key = Key::ObjectByTouchedAt {
							touched_at: value.touched_at,
							reference_count_zero: true,
							id: &id,
						}
						.pack_to_vec();
						txn.clear(&touched_at_key);

						// Decrement cache entry reference count if present.
						if let Some(ref cache_entry_id) = value.cache_entry {
							let cache_key = Key::CacheEntry(cache_entry_id).pack_to_vec();
							if let Some(cache_value_bytes) =
								txn.get(&cache_key, false).await.map_err(|source| {
									tg::error!(!source, "failed to get cache entry")
								})? {
								let mut cache_value: CacheEntryValue =
									tangram_serialize::from_slice(&cache_value_bytes).map_err(
										|source| {
											tg::error!(!source, "failed to deserialize cache entry")
										},
									)?;

								let old_index_key = Key::CacheEntryByTouchedAt {
									touched_at: cache_value.touched_at,
									id: cache_entry_id,
								}
								.pack_to_vec();
								txn.clear(&old_index_key);

								cache_value.reference_count =
									decrement_ref_count(cache_value.reference_count);

								let new_index_key = Key::CacheEntryByTouchedAt {
									touched_at: cache_value.touched_at,
									id: cache_entry_id,
								}
								.pack_to_vec();
								txn.set(&new_index_key, &[]);

								let updated_bytes = tangram_serialize::to_vec(&cache_value)
									.map_err(|source| {
										tg::error!(!source, "failed to serialize cache entry")
									})?;
								txn.set(&cache_key, &updated_bytes);
							}
						}

						bytes += value.node_size;
						objects.push(id);
						n = n.saturating_sub(1);
					}
				}
			}
		}

		// Clean processes with reference_count=0 and old touched_at.
		let mut processes = Vec::new();
		{
			let (start, end) = process_by_touched_at_range(max_touched_at);
			let range = RangeOption {
				mode: StreamingMode::WantAll,
				limit: Some(n),
				..RangeOption::from((start.as_slice(), end.as_slice()))
			};
			let entries = txn
				.get_range(&range, 0, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get range"))?;

			for entry in entries {
				let key_bytes = entry.key();
				if key_bytes.len() < 12 {
					continue;
				}
				let id_start = 11;
				if key_bytes.len() <= id_start {
					continue;
				}
				let id_bytes = &key_bytes[id_start..];
				let actual_id_bytes = if id_bytes.len() > 2 && id_bytes[0] == 0x01 {
					&id_bytes[1..id_bytes.len().saturating_sub(1)]
				} else {
					id_bytes
				};
				let Ok(id) = tg::process::Id::from_slice(actual_id_bytes) else {
					continue;
				};

				// Get the process to check reference count.
				let process_key = Key::Process(&id).pack_to_vec();
				if let Some(value_bytes) = txn
					.get(&process_key, false)
					.await
					.map_err(|source| tg::error!(!source, "failed to get process"))?
				{
					let value: ProcessValue = tangram_serialize::from_slice(&value_bytes)
						.map_err(|source| tg::error!(!source, "failed to deserialize process"))?;

					if is_ref_count_zero(value.reference_count) {
						// Decrement reference counts of child processes.
						let (children_start, children_end) = process_children_prefix(&id);
						let children_range = RangeOption {
							mode: StreamingMode::WantAll,
							..RangeOption::from((
								children_start.as_slice(),
								children_end.as_slice(),
							))
						};
						let children = txn
							.get_range(&children_range, 0, false)
							.await
							.map_err(|source| tg::error!(!source, "failed to get children"))?;

						// Collect child data before processing to avoid holding FdbKeyValue across awaits.
						let child_data: Vec<(Vec<u8>, Option<tg::process::Id>)> = children
							.iter()
							.map(|entry| {
								let key = entry.key().to_vec();
								let child_id = tg::process::Id::from_slice(entry.value()).ok();
								(key, child_id)
							})
							.collect();
						drop(children);

						for (child_key, child_id_opt) in child_data {
							if let Some(child_id) = child_id_opt {
								let child_process_key = Key::Process(&child_id).pack_to_vec();
								if let Some(child_value_bytes) =
									txn.get(&child_process_key, false).await.map_err(|source| {
										tg::error!(!source, "failed to get child process")
									})? {
									let mut child_value: ProcessValue =
										tangram_serialize::from_slice(&child_value_bytes).map_err(
											|source| {
												tg::error!(
													!source,
													"failed to deserialize child process"
												)
											},
										)?;

									let old_ref_zero =
										is_ref_count_zero(child_value.reference_count);
									let old_index_key = Key::ProcessByTouchedAt {
										touched_at: child_value.touched_at,
										reference_count_zero: old_ref_zero,
										id: &child_id,
									}
									.pack_to_vec();
									txn.clear(&old_index_key);

									child_value.reference_count =
										decrement_ref_count(child_value.reference_count);

									let new_ref_zero =
										is_ref_count_zero(child_value.reference_count);
									let new_index_key = Key::ProcessByTouchedAt {
										touched_at: child_value.touched_at,
										reference_count_zero: new_ref_zero,
										id: &child_id,
									}
									.pack_to_vec();
									txn.set(&new_index_key, &[]);

									let updated_bytes = tangram_serialize::to_vec(&child_value)
										.map_err(|source| {
											tg::error!(!source, "failed to serialize child process")
										})?;
									txn.set(&child_process_key, &updated_bytes);
								}

								let reverse_key = Key::ProcessChildrenReverse {
									child: &child_id,
									parent: &id,
								}
								.pack_to_vec();
								txn.clear(&reverse_key);
							}

							txn.clear(&child_key);
						}

						// Decrement reference counts of referenced objects.
						let (objects_start, objects_end) = process_objects_prefix(&id);
						let objects_range = RangeOption {
							mode: StreamingMode::WantAll,
							..RangeOption::from((objects_start.as_slice(), objects_end.as_slice()))
						};
						let process_objects = txn
							.get_range(&objects_range, 0, false)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to get process objects")
							})?;

						// Collect object data before processing to avoid holding FdbKeyValue across awaits.
						let obj_data: Vec<(Vec<u8>, Option<(tg::object::Id, i64)>)> =
							process_objects
								.iter()
								.map(|entry| {
									let key = entry.key().to_vec();
									let parsed = if key.len() > 70 {
										let object_id_bytes = &key[35..68];
										let actual_object_id_bytes = if object_id_bytes.len() > 2
											&& object_id_bytes[0] == 0x01
										{
											&object_id_bytes
												[1..object_id_bytes.len().saturating_sub(1)]
										} else {
											object_id_bytes
										};
										let kind = i64::from(key[key.len() - 1]);
										tg::object::Id::from_slice(actual_object_id_bytes)
											.ok()
											.map(|oid| (oid, kind))
									} else {
										None
									};
									(key, parsed)
								})
								.collect();
						drop(process_objects);

						for (obj_key, parsed_opt) in obj_data {
							if let Some((object_id, kind)) = parsed_opt {
								let object_key = Key::Object(&object_id).pack_to_vec();
								if let Some(object_value_bytes) = txn
									.get(&object_key, false)
									.await
									.map_err(|source| tg::error!(!source, "failed to get object"))?
								{
									let mut object_value: ObjectValue =
										tangram_serialize::from_slice(&object_value_bytes)
											.map_err(|source| {
												tg::error!(!source, "failed to deserialize object")
											})?;

									let old_ref_zero =
										is_ref_count_zero(object_value.reference_count);
									let old_index_key = Key::ObjectByTouchedAt {
										touched_at: object_value.touched_at,
										reference_count_zero: old_ref_zero,
										id: &object_id,
									}
									.pack_to_vec();
									txn.clear(&old_index_key);

									object_value.reference_count =
										decrement_ref_count(object_value.reference_count);

									let new_ref_zero =
										is_ref_count_zero(object_value.reference_count);
									let new_index_key = Key::ObjectByTouchedAt {
										touched_at: object_value.touched_at,
										reference_count_zero: new_ref_zero,
										id: &object_id,
									}
									.pack_to_vec();
									txn.set(&new_index_key, &[]);

									let updated_bytes = tangram_serialize::to_vec(&object_value)
										.map_err(|source| {
											tg::error!(!source, "failed to serialize object")
										})?;
									txn.set(&object_key, &updated_bytes);
								}

								let reverse_key = Key::ProcessObjectsReverse {
									object: &object_id,
									kind,
									process: &id,
								}
								.pack_to_vec();
								txn.clear(&reverse_key);
							}

							txn.clear(&obj_key);
						}

						// Delete the process.
						txn.clear(&process_key);

						// Delete the secondary index.
						let touched_at_key = Key::ProcessByTouchedAt {
							touched_at: value.touched_at,
							reference_count_zero: true,
							id: &id,
						}
						.pack_to_vec();
						txn.clear(&touched_at_key);

						processes.push(id);
					}
				}
			}
		}

		// Commit the transaction.
		txn.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;

		let output = InnerOutput {
			bytes,
			cache_entries,
			objects,
			processes,
		};

		Ok(output)
	}
}
