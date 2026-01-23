use {
	super::{
		CacheEntryCoreField, CacheEntryField, Index, ItemKind, Key, Kind, ObjectCoreField,
		ObjectField, ProcessCoreField, ProcessField,
	},
	crate::CleanOutput,
	foundationdb as fdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn clean(&self, max_touched_at: i64, n: usize) -> tg::Result<CleanOutput> {
		let this = self.clone();
		self.database
			.run(|txn, _| {
				let this = this.clone();
				async move { this.clean_inner(&txn, max_touched_at, n).await }
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to run clean transaction"))
	}

	async fn clean_inner(
		&self,
		txn: &fdb::Transaction,
		max_touched_at: i64,
		batch_size: usize,
	) -> Result<CleanOutput, fdb::FdbBindingError> {
		txn.set_option(fdb::options::TransactionOption::PriorityBatch)
			.unwrap();

		let mut output = CleanOutput::default();

		let start = self.pack_tuple(&(Kind::Clean.to_i32().unwrap(),));
		let end = self.pack_tuple(&(Kind::Clean.to_i32().unwrap(), max_touched_at + 1));

		let range = fdb::RangeOption {
			begin: fdb::KeySelector::first_greater_or_equal(&start),
			end: fdb::KeySelector::first_greater_or_equal(&end),
			limit: Some(batch_size),
			..Default::default()
		};

		let entries = txn.get_range(&range, 0, false).await?;
		let items: Vec<(i64, ItemKind, tg::Id)> = entries
			.iter()
			.filter_map(|entry| {
				let key: Key = self.unpack(entry.key()).ok()?;
				let Key::Clean {
					touched_at,
					kind,
					id,
				} = key
				else {
					return None;
				};
				Some((touched_at, kind, id))
			})
			.collect();

		for (touched_at, kind, id) in items {
			let reference_count = match kind {
				ItemKind::CacheEntry => {
					let artifact_id = tg::artifact::Id::try_from(id.clone()).ok();
					if let Some(artifact_id) = artifact_id {
						self.compute_cache_entry_reference_count(txn, &artifact_id)
							.await?
					} else {
						0
					}
				},
				ItemKind::Object => {
					let object_id = tg::object::Id::try_from(id.clone()).ok();
					if let Some(object_id) = object_id {
						self.compute_object_reference_count(txn, &object_id).await?
					} else {
						0
					}
				},
				ItemKind::Process => {
					let process_id = tg::process::Id::try_from(id.clone()).ok();
					if let Some(process_id) = process_id {
						self.compute_process_reference_count(txn, &process_id)
							.await?
					} else {
						0
					}
				},
			};

			if reference_count > 0 {
				self.set_reference_count(txn, kind, &id, reference_count);
			} else {
				self.delete_item(txn, kind, &id, touched_at, &mut output)
					.await?;
			}

			let clean_key = self.pack(&Key::Clean {
				touched_at,
				kind,
				id: id.clone(),
			});
			txn.clear(&clean_key);
		}

		Ok(output)
	}

	async fn compute_cache_entry_reference_count(
		&self,
		txn: &fdb::Transaction,
		id: &tg::artifact::Id,
	) -> Result<u64, fdb::FdbBindingError> {
		let prefix = self.pack_tuple(&(
			Kind::CacheEntryObject.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		));
		self.count_keys_with_prefix(txn, &prefix).await
	}

	async fn compute_object_reference_count(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
	) -> Result<u64, fdb::FdbBindingError> {
		let child_object_prefix =
			self.pack_tuple(&(Kind::ChildObject.to_i32().unwrap(), id.to_bytes().as_ref()));
		let child_object_count = self
			.count_keys_with_prefix(txn, &child_object_prefix)
			.await?;

		let object_process_prefix = self.pack_tuple(&(
			Kind::ObjectProcess.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		));
		let object_process_count = self
			.count_keys_with_prefix(txn, &object_process_prefix)
			.await?;

		Ok(child_object_count + object_process_count)
	}

	async fn compute_process_reference_count(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
	) -> Result<u64, fdb::FdbBindingError> {
		let prefix =
			self.pack_tuple(&(Kind::ChildProcess.to_i32().unwrap(), id.to_bytes().as_ref()));
		self.count_keys_with_prefix(txn, &prefix).await
	}

	async fn count_keys_with_prefix(
		&self,
		txn: &fdb::Transaction,
		prefix: &[u8],
	) -> Result<u64, fdb::FdbBindingError> {
		let mut end = prefix.to_vec();
		if let Some(last) = end.last_mut() {
			*last = last.saturating_add(1);
		} else {
			end.push(0);
		}

		let range = fdb::RangeOption {
			begin: fdb::KeySelector::first_greater_or_equal(prefix),
			end: fdb::KeySelector::first_greater_or_equal(&end),
			..Default::default()
		};

		let entries = txn.get_range(&range, 0, false).await?;
		Ok(entries.len() as u64)
	}

	fn set_reference_count(
		&self,
		txn: &fdb::Transaction,
		kind: ItemKind,
		id: &tg::Id,
		reference_count: u64,
	) {
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		match kind {
			ItemKind::CacheEntry => {
				if let Ok(artifact_id) = tg::artifact::Id::try_from(id.clone()) {
					let key = self.pack(&Key::CacheEntry {
						id: artifact_id,
						field: CacheEntryField::Core(CacheEntryCoreField::ReferenceCount),
					});
					txn.set(&key, &reference_count.to_le_bytes());
				}
			},
			ItemKind::Object => {
				if let Ok(object_id) = tg::object::Id::try_from(id.clone()) {
					let key = self.pack(&Key::Object {
						id: object_id,
						field: ObjectField::Core(ObjectCoreField::ReferenceCount),
					});
					txn.set(&key, &reference_count.to_le_bytes());
				}
			},
			ItemKind::Process => {
				if let Ok(process_id) = tg::process::Id::try_from(id.clone()) {
					let key = self.pack(&Key::Process {
						id: process_id,
						field: ProcessField::Core(ProcessCoreField::ReferenceCount),
					});
					txn.set(&key, &reference_count.to_le_bytes());
				}
			},
		}
	}

	async fn delete_item(
		&self,
		txn: &fdb::Transaction,
		kind: ItemKind,
		id: &tg::Id,
		_touched_at: i64,
		output: &mut CleanOutput,
	) -> Result<(), fdb::FdbBindingError> {
		match kind {
			ItemKind::CacheEntry => {
				if let Ok(artifact_id) = tg::artifact::Id::try_from(id.clone()) {
					self.delete_cache_entry(txn, &artifact_id, output).await?;
				}
			},
			ItemKind::Object => {
				if let Ok(object_id) = tg::object::Id::try_from(id.clone()) {
					self.delete_object(txn, &object_id, output).await?;
				}
			},
			ItemKind::Process => {
				if let Ok(process_id) = tg::process::Id::try_from(id.clone()) {
					self.delete_process(txn, &process_id, output).await?;
				}
			},
		}
		Ok(())
	}

	async fn delete_cache_entry(
		&self,
		txn: &fdb::Transaction,
		id: &tg::artifact::Id,
		output: &mut CleanOutput,
	) -> Result<(), fdb::FdbBindingError> {
		// Delete the cache entry keys.
		let prefix = self.pack_tuple(&(Kind::CacheEntry.to_i32().unwrap(), id.to_bytes().as_ref()));
		Self::clear_prefix(txn, &prefix);

		// Delete CacheEntryObject keys for this cache entry.
		let prefix = self.pack_tuple(&(
			Kind::CacheEntryObject.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		));
		Self::clear_prefix(txn, &prefix);

		// Add to output.
		output.cache_entries.push(id.clone());

		Ok(())
	}

	async fn delete_object(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
		output: &mut CleanOutput,
	) -> Result<(), fdb::FdbBindingError> {
		let cache_entry_key = self.pack(&Key::Object {
			id: id.clone(),
			field: ObjectField::Core(ObjectCoreField::CacheEntry),
		});
		let cache_entry = txn
			.get(&cache_entry_key, false)
			.await?
			.and_then(|bytes| tg::artifact::Id::from_slice(&bytes).ok());

		let prefix = self.pack_tuple(&(Kind::Object.to_i32().unwrap(), id.to_bytes().as_ref()));
		Self::clear_prefix(txn, &prefix);

		let prefix =
			self.pack_tuple(&(Kind::ObjectChild.to_i32().unwrap(), id.to_bytes().as_ref()));
		let children = self.collect_object_children_and_clear(txn, &prefix).await?;
		for child_id in children {
			self.decrement_object_reference_count(txn, &child_id)
				.await?;
		}

		let prefix =
			self.pack_tuple(&(Kind::ChildObject.to_i32().unwrap(), id.to_bytes().as_ref()));
		Self::clear_prefix(txn, &prefix);

		if let Some(cache_entry) = &cache_entry {
			let key = self.pack(&Key::ObjectCacheEntry {
				object: id.clone(),
				cache_entry: cache_entry.clone(),
			});
			txn.clear(&key);
		}

		if let Some(cache_entry) = &cache_entry {
			let key = self.pack(&Key::CacheEntryObject {
				cache_entry: cache_entry.clone(),
				object: id.clone(),
			});
			txn.clear(&key);
		}

		let prefix = self.pack_tuple(&(
			Kind::ObjectProcess.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		));
		Self::clear_prefix(txn, &prefix);

		output.objects.push(id.clone());

		Ok(())
	}

	async fn delete_process(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
		output: &mut CleanOutput,
	) -> Result<(), fdb::FdbBindingError> {
		let prefix = self.pack_tuple(&(Kind::Process.to_i32().unwrap(), id.to_bytes().as_ref()));
		Self::clear_prefix(txn, &prefix);

		let prefix =
			self.pack_tuple(&(Kind::ProcessChild.to_i32().unwrap(), id.to_bytes().as_ref()));
		let children = self
			.collect_process_children_and_clear(txn, &prefix)
			.await?;
		for child_id in children {
			self.decrement_process_reference_count(txn, &child_id)
				.await?;
		}

		let prefix =
			self.pack_tuple(&(Kind::ChildProcess.to_i32().unwrap(), id.to_bytes().as_ref()));
		Self::clear_prefix(txn, &prefix);

		let prefix = self.pack_tuple(&(
			Kind::ProcessObject.to_i32().unwrap(),
			id.to_bytes().as_ref(),
		));
		let objects = self.collect_process_objects_and_clear(txn, &prefix).await?;
		for object_id in objects {
			self.decrement_object_reference_count(txn, &object_id)
				.await?;
		}

		output.processes.push(id.clone());

		Ok(())
	}

	fn clear_prefix(txn: &fdb::Transaction, prefix: &[u8]) {
		let mut end = prefix.to_vec();
		if let Some(last) = end.last_mut() {
			*last = last.saturating_add(1);
		} else {
			end.push(0);
		}
		txn.clear_range(prefix, &end);
	}

	async fn collect_object_children_and_clear(
		&self,
		txn: &fdb::Transaction,
		prefix: &[u8],
	) -> Result<Vec<tg::object::Id>, fdb::FdbBindingError> {
		let mut end = prefix.to_vec();
		if let Some(last) = end.last_mut() {
			*last = last.saturating_add(1);
		} else {
			end.push(0);
		}

		let range = fdb::RangeOption {
			begin: fdb::KeySelector::first_greater_or_equal(prefix),
			end: fdb::KeySelector::first_greater_or_equal(&end),
			..Default::default()
		};

		let entries = txn.get_range(&range, 0, false).await?;
		let mut children = Vec::new();
		for entry in &entries {
			let key: Key = match self.unpack(entry.key()) {
				Ok(k) => k,
				Err(_) => continue,
			};
			if let Key::ObjectChild { child, .. } = key {
				children.push(child);
			}
		}

		// Clear the range.
		txn.clear_range(prefix, &end);

		Ok(children)
	}

	async fn collect_process_children_and_clear(
		&self,
		txn: &fdb::Transaction,
		prefix: &[u8],
	) -> Result<Vec<tg::process::Id>, fdb::FdbBindingError> {
		let mut end = prefix.to_vec();
		if let Some(last) = end.last_mut() {
			*last = last.saturating_add(1);
		} else {
			end.push(0);
		}

		let range = fdb::RangeOption {
			begin: fdb::KeySelector::first_greater_or_equal(prefix),
			end: fdb::KeySelector::first_greater_or_equal(&end),
			..Default::default()
		};

		let entries = txn.get_range(&range, 0, false).await?;
		let mut children = Vec::new();
		for entry in &entries {
			let key: Key = match self.unpack(entry.key()) {
				Ok(k) => k,
				Err(_) => continue,
			};
			if let Key::ProcessChild { child, .. } = key {
				children.push(child);
			}
		}

		txn.clear_range(prefix, &end);

		Ok(children)
	}

	async fn collect_process_objects_and_clear(
		&self,
		txn: &fdb::Transaction,
		prefix: &[u8],
	) -> Result<Vec<tg::object::Id>, fdb::FdbBindingError> {
		let mut end = prefix.to_vec();
		if let Some(last) = end.last_mut() {
			*last = last.saturating_add(1);
		} else {
			end.push(0);
		}

		let range = fdb::RangeOption {
			begin: fdb::KeySelector::first_greater_or_equal(prefix),
			end: fdb::KeySelector::first_greater_or_equal(&end),
			..Default::default()
		};

		let entries = txn.get_range(&range, 0, false).await?;
		let mut objects = Vec::new();
		for entry in &entries {
			let key: Key = match self.unpack(entry.key()) {
				Ok(k) => k,
				Err(_) => continue,
			};
			if let Key::ProcessObject { object, .. } = key {
				objects.push(object);
			}
		}

		txn.clear_range(prefix, &end);

		Ok(objects)
	}

	async fn decrement_object_reference_count(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
	) -> Result<(), fdb::FdbBindingError> {
		let reference_count_key = self.pack(&Key::Object {
			id: id.clone(),
			field: ObjectField::Core(ObjectCoreField::ReferenceCount),
		});
		let reference_count = txn
			.get(&reference_count_key, false)
			.await?
			.and_then(|bytes| {
				<[u8; 8]>::try_from(bytes.as_ref())
					.ok()
					.map(u64::from_le_bytes)
			})
			.unwrap_or(0);

		if reference_count <= 1 {
			txn.clear(&reference_count_key);

			let touched_at_key = self.pack(&Key::Object {
				id: id.clone(),
				field: ObjectField::Core(ObjectCoreField::TouchedAt),
			});
			let touched_at = txn
				.get(&touched_at_key, false)
				.await?
				.and_then(|bytes| {
					<[u8; 8]>::try_from(bytes.as_ref())
						.ok()
						.map(i64::from_le_bytes)
				})
				.unwrap_or(0);

			let clean_key = self.pack(&Key::Clean {
				touched_at,
				kind: ItemKind::Object,
				id: tg::Id::from(id.clone()),
			});
			txn.set(&clean_key, &[]);
		} else {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			txn.set(&reference_count_key, &(reference_count - 1).to_le_bytes());
		}

		Ok(())
	}

	async fn decrement_process_reference_count(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
	) -> Result<(), fdb::FdbBindingError> {
		let reference_count_key = self.pack(&Key::Process {
			id: id.clone(),
			field: ProcessField::Core(ProcessCoreField::ReferenceCount),
		});
		let reference_count = txn
			.get(&reference_count_key, false)
			.await?
			.and_then(|bytes| {
				<[u8; 8]>::try_from(bytes.as_ref())
					.ok()
					.map(u64::from_le_bytes)
			})
			.unwrap_or(0);

		if reference_count <= 1 {
			txn.clear(&reference_count_key);

			let touched_at_key = self.pack(&Key::Process {
				id: id.clone(),
				field: ProcessField::Core(ProcessCoreField::TouchedAt),
			});
			let touched_at = txn
				.get(&touched_at_key, false)
				.await?
				.and_then(|bytes| {
					<[u8; 8]>::try_from(bytes.as_ref())
						.ok()
						.map(i64::from_le_bytes)
				})
				.unwrap_or(0);

			let clean_key = self.pack(&Key::Clean {
				touched_at,
				kind: ItemKind::Process,
				id: tg::Id::from(id.clone()),
			});
			txn.set(&clean_key, &[]);
		} else {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			txn.set(&reference_count_key, &(reference_count - 1).to_le_bytes());
		}

		Ok(())
	}
}
