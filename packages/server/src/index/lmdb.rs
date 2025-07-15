use bytes::Bytes;
use fnv::FnvHashSet;
use foundationdb_tuple::TuplePack as _;
use heed::{self as lmdb, DatabaseFlags};
use http::header::CACHE_CONTROL;
use num::ToPrimitive as _;
use serde::de;
use tangram_client::{self as tg, value::print};

use crate::{
	index::PutObjectBatchArg,
	process::{complete, start},
};

pub struct Lmdb {
	db: lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>,
	env: lmdb::Env,
}

enum Message {
	PutObjects(PutObjectBatchMessage),
	PutObjectChildren(PutObjectChildrenBatchMessage),
	DecrementObjectIncompleteChildrenCountBatch(DecrementObjectIncompleteChildrenBatchArg),
}

struct PutObjectBatchMessage {
	objects: Vec<super::PutObjectArg>,
	response_sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

struct PutObjectChildrenBatchMessage {
	items: Vec<super::PutObjectChildrenArg>,
	response_sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

pub struct DecrementObjectIncompleteChildrenBatchArg {
	ids: Vec<tg::object::Id>,
	response_sender: tokio::sync::oneshot::Sender<tg::Result<Vec<u64>>>,
}

impl Lmdb {
	pub fn new(config: &crate::config::LmdbIndex) -> tg::Result<Self> {
		std::fs::OpenOptions::new()
			.create(true)
			.truncate(false)
			.read(true)
			.write(true)
			.open(&config.path)
			.map_err(|source| tg::error!(!source, "failed to create or open the database file"))?;
		let env = unsafe {
			heed::EnvOpenOptions::new()
				.map_size(1_099_511_627_776)
				.max_dbs(3)
				.max_readers(1_000)
				.flags(heed::EnvFlags::NO_SUB_DIR)
				.open(&config.path)
				.map_err(|source| tg::error!(!source, "failed to open the database"))?
		};
		let mut transaction = env.write_txn().unwrap();
		let db: heed::Database<heed::types::Bytes, heed::types::Bytes> = env
			.database_options()
			.types::<heed::types::Bytes, heed::types::Bytes>()
			.flags(DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED)
			.create(&mut transaction)
			.map_err(|source| tg::error!(!source, "failed to open the database"))?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(Self { db, env })
	}

	pub fn put_objects(&self, messages: &[super::PutObjectMessage]) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		// Insert the children
		let object_children_batch_arg = crate::index::PutObjectChildrenBatchArg {
			items: messages
				.iter()
				.map(|message| crate::index::PutObjectChildrenArg {
					id: message.id.clone(),
					children: message.children.iter().cloned().collect(),
				})
				.collect::<Vec<_>>(),
		};
		self.put_object_children_batch_with_transaction(
			&mut transaction,
			object_children_batch_arg,
		)?;

		// Compute the incomplete children counts, keep track of those with incomplete_children = 0.
		let incomplete_children_counts = self
			.try_get_object_incomplete_children_count_batch_with_transaction(
				&transaction,
				messages
					.iter()
					.map(|message| message.id.clone())
					.collect::<Vec<_>>()
					.as_slice(),
			)?;

		// Get the objects to insert.
		let objects: Vec<crate::index::PutObjectArg> = messages
			.iter()
			.zip(incomplete_children_counts)
			.map(
				|(message, incomplete_children)| crate::index::PutObjectArg {
					id: message.id.clone(),
					cache_reference: message.cache_reference.clone(),
					size: Some(message.size),
					touched_at: Some(message.touched_at),
					incomplete_children: Some(incomplete_children),
					complete: false,
					count: None,
					depth: None,
					weight: None,
				},
			)
			.collect();

		// Insert the objects.
		let arg = PutObjectBatchArg {
			objects: objects.clone(),
		};
		let inserted =
			self.insert_if_not_exists_object_batch_with_transaction(&mut transaction, arg)?;

		// Filter the objects that were just inserted and had incomplete_children = 0.
		let mut queue: Vec<tg::object::Id> = objects
			.iter()
			.zip(inserted.into_iter())
			.filter(|(object, inserted)| *inserted && object.incomplete_children.unwrap() == 0)
			.map(|(object, _)| object.id.clone())
			.collect();

		#[derive(Default)]
		struct Stats {
			count: u64,
			depth: u64,
			weight: u64,
		}

		// Process parent updates if needed.
		while !queue.is_empty() {
			let object_ids = queue
				.iter()
				.cloned()
				.collect::<FnvHashSet<_>>()
				.into_iter()
				.collect::<Vec<_>>();

			// For all objects whose incomplete_children count is 0, find all of its children, grab their count, depth, and weight and add them up to get the count, depth, and weight of the parent.
			let children_batch =
				self.try_get_object_children_batch_with_transaction(&transaction, &object_ids)?;

			let mut objects_to_update: Vec<crate::index::PutObjectArg> = Vec::new();
			for (id, children) in object_ids.iter().zip(children_batch.iter()) {
				let children_objects =
					self.try_get_object_batch_with_transaction(&transaction, children)?;
				let object = self
					.try_get_object_batch_with_transaction(&transaction, &[id.clone()])?
					.first()
					.cloned()
					.unwrap()
					.unwrap();
				let stats =
					children_objects
						.iter()
						.try_fold(Stats::default(), |mut stats, child| {
							let child = child.as_ref().ok_or_else(|| {
								tg::error!(?child, "failed to get child object in indexer task")
							})?;
							stats.count += child.count.ok_or_else(|| {
								tg::error!("expected a count for the child object")
							})?;
							stats.depth = stats.depth.max(child.depth.ok_or_else(|| {
								tg::error!("expected a depth for the child object")
							})?);
							stats.weight += child.weight.ok_or_else(|| {
								tg::error!("expected a weight for the child object")
							})?;
							Ok::<_, tg::error::Error>(stats)
						})?;
				objects_to_update.push(crate::index::PutObjectArg {
					id: object.id.clone(),
					cache_reference: None,
					incomplete_children: Some(0),
					size: None,
					touched_at: None,
					complete: true,
					count: Some(1 + stats.count),
					depth: Some(1 + stats.depth),
					weight: Some(object.size.unwrap() + stats.weight),
				});
			}

			// Update the count, depth, weight of the objects.
			self.update_object_batch_with_transaction(
				&mut transaction,
				PutObjectBatchArg {
					objects: objects_to_update,
				},
			)?;

			// For all objects who are now complete, find their parents and decrement their incomplete children count. All items whose incomplete_children count is 0 are now eligible for update themselves.
			let parents =
				self.try_get_object_parents_batch_with_transaction(&transaction, &object_ids)?;

			let parents: Vec<tg::object::Id> = parents.into_iter().flatten().collect();

			let incomplete_children_counts = self
				.decrement_object_incomplete_children_count_batch_with_transaction(
					&mut transaction,
					&parents,
				)?;

			queue = parents
				.iter()
				.zip(incomplete_children_counts.iter())
				.filter_map(|(parent, incomplete_children)| {
					if *incomplete_children == 0 {
						Some(parent.clone())
					} else {
						None
					}
				})
				.collect();
		}
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn try_get_object_batch_with_transaction<'a>(
		&self,
		transaction: &'a lmdb::RwTxn<'a>,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<super::Object>>> {
		let mut output = Vec::with_capacity(ids.len());
		for id in ids {
			let complete_key = (0, id.to_bytes(), 0);
			let count_key = (0, id.to_bytes(), 1);
			let depth_key = (0, id.to_bytes(), 2);
			let incomplete_children_key = (0, id.to_bytes(), 3);
			let size_key = (0, id.to_bytes(), 4);
			let weight_key = (0, id.to_bytes(), 5);

			let complete_bytes = self
				.db
				.get(transaction, &complete_key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to get the value"))?;
			let complete = if let Some(complete) = complete_bytes {
				// Convert the byte slice to a bool
				if complete == [1] {
					true
				} else if complete == [0] {
					false
				} else {
					panic!("Invalid boolean value stored in LMDB");
				}
			} else {
				output.push(None);
				continue;
			};

			let count = self
				.db
				.get(transaction, &count_key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to get the value"))?;
			let count = match count {
				Some(count) => {
					let count = u64::from_le_bytes(count.try_into().unwrap());
					Some(count)
				},
				None => None,
			};
			let depth = self
				.db
				.get(transaction, &depth_key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to get the value"))?;
			let depth = match depth {
				Some(depth) => {
					let depth = u64::from_le_bytes(depth.try_into().unwrap());
					Some(depth)
				},
				None => None,
			};
			let incomplete_children = self
				.db
				.get(transaction, &incomplete_children_key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to get the value"))?;
			let incomplete_children = match incomplete_children {
				Some(incomplete_children) => {
					let incomplete_children =
						u64::from_le_bytes(incomplete_children.try_into().unwrap());
					Some(incomplete_children)
				},
				None => None,
			};
			let size = self
				.db
				.get(transaction, &size_key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to get the value"))?;
			let size = match size {
				Some(size) => {
					let size = u64::from_le_bytes(size.try_into().unwrap());
					Some(size)
				},
				None => None,
			};
			let weight = self
				.db
				.get(transaction, &weight_key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to get the value"))?;
			let weight = match weight {
				Some(weight) => {
					let weight = u64::from_le_bytes(weight.try_into().unwrap());
					Some(weight)
				},
				None => None,
			};

			let object = super::Object {
				id: id.clone(),
				complete,
				count,
				depth,
				incomplete_children,
				size,
				weight,
			};
			output.push(Some(object));
		}
		Ok(output)
	}

	pub fn try_get_object_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<super::Object>>> {
		let transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let output = self.try_get_object_batch_with_transaction(&transaction, ids)?;
		Ok(output)
	}

	pub fn try_get_object_complete_batch_with_transaction(
		&self,
		transaction: &lmdb::RwTxn,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
		let mut output = Vec::with_capacity(ids.len());
		for id in ids {
			let complete_key = (0, id.to_bytes(), 0);
			let complete = self
				.db
				.get(transaction, &complete_key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to get the value"))?;
			let complete = if let Some(complete) = complete {
				// Convert the byte slice to a bool
				let complete = if complete == [1] {
					true
				} else if complete == [0] {
					false
				} else {
					panic!("Invalid boolean value stored in LMDB");
				};
				Some(complete)
			} else {
				output.push(None);
				continue;
			};
			output.push(complete);
		}
		Ok(output)
	}

	pub fn try_get_object_complete_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
		let transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let output = self.try_get_object_complete_batch_with_transaction(&transaction, ids)?;
		Ok(output)
	}

	pub fn try_get_object_parents_batch_with_transaction(
		&self,
		transaction: &lmdb::RwTxn,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Vec<tg::object::Id>>> {
		let mut output = Vec::with_capacity(ids.len());
		for id in ids {
			let mut parents = Vec::new();
			let key = (2, id.to_bytes());
			let dup = self
				.db
				.get_duplicates(&transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to get the value"))?;
			if let Some(dup) = dup {
				for parent in dup {
					let parent =
						parent.map_err(|source| tg::error!(!source, "failed to get the value"))?;
					let parent_id = tg::object::Id::from_slice(parent.1)
						.map_err(|source| tg::error!(!source, "failed to deserialize parent id"))?;
					parents.push(parent_id);
				}
			}
			output.push(parents);
		}
		Ok(output)
	}

	pub fn try_get_object_parents_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Vec<tg::object::Id>>> {
		let transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let output = self.try_get_object_parents_batch_with_transaction(&transaction, ids)?;
		Ok(output)
	}

	pub fn try_get_object_children_batch_with_transaction(
		&self,
		transaction: &lmdb::RwTxn,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Vec<tg::object::Id>>> {
		let mut output = Vec::with_capacity(ids.len());
		for id in ids {
			let mut children = Vec::new();
			let key = (1, id.to_bytes());
			let dup = self
				.db
				.get_duplicates(transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to get the value"))?;
			if let Some(dup) = dup {
				for child in dup {
					let child =
						child.map_err(|source| tg::error!(!source, "failed to get the value"))?;
					let child_id = tg::object::Id::from_slice(child.1)
						.map_err(|source| tg::error!(!source, "failed to deserialize child id"))?;
					children.push(child_id);
				}
			}
			output.push(children);
		}
		Ok(output)
	}

	pub fn try_get_object_children_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Vec<tg::object::Id>>> {
		let transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let output = self.try_get_object_children_batch_with_transaction(&transaction, ids)?;
		Ok(output)
	}

	pub fn try_get_object_incomplete_children_count_batch_with_transaction(
		&self,
		transaction: &lmdb::RwTxn,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<u64>> {
		let children_batch =
			self.try_get_object_children_batch_with_transaction(&transaction, ids)?;
		let mut output = Vec::with_capacity(ids.len());
		for children in children_batch {
			let children_objects =
				self.try_get_object_batch_with_transaction(&transaction, &children)?;
			let mut incomplete_children_count = 0;
			for object in children_objects {
				if let Some(object) = object {
					if !object.complete {
						incomplete_children_count += 1;
					}
				} else {
					incomplete_children_count += 1;
				}
			}
			output.push(incomplete_children_count);
		}
		Ok(output)
	}

	pub fn try_get_object_incomplete_children_count_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<u64>> {
		let transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let output = self
			.try_get_object_incomplete_children_count_batch_with_transaction(&transaction, ids)?;
		Ok(output)
	}

	pub fn insert_if_not_exists_object_batch_with_transaction(
		&self,
		transaction: &mut lmdb::RwTxn,
		arg: super::PutObjectBatchArg,
	) -> tg::Result<Vec<bool>> {
		let mut output = Vec::with_capacity(arg.objects.len());
		for object in arg.objects {
			let id = object.id.clone();
			let complete_key = (0, id.to_bytes(), 0);
			let count_key = (0, id.to_bytes(), 1);
			let depth_key = (0, id.to_bytes(), 2);
			let incomplete_children_key = (0, id.to_bytes(), 3);
			let size_key = (0, id.to_bytes(), 4);
			let weight_key = (0, id.to_bytes(), 5);

			// Write the complete key.
			if self
				.db
				.get(transaction, &complete_key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to get the key"))?
				.is_some()
			{
				output.push(false);
				continue;
			}
			let bytes = if object.complete { [1] } else { [0] };
			self.db
				.delete(transaction, &complete_key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to delete the value"))?;
			self.db
				.put(transaction, &complete_key.pack_to_vec(), &bytes)
				.map_err(|source| tg::error!(!source, "failed to put the value"))?;

			if let Some(count) = object.count {
				let bytes = count.to_le_bytes().to_vec();
				self.db
					.put(transaction, &count_key.pack_to_vec(), &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
			if let Some(depth) = object.depth {
				let bytes = depth.to_le_bytes().to_vec();
				self.db
					.put(transaction, &depth_key.pack_to_vec(), &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
			if let Some(incomplete_children) = object.incomplete_children {
				let bytes = incomplete_children.to_le_bytes().to_vec();
				self.db
					.put(transaction, &incomplete_children_key.pack_to_vec(), &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
			if let Some(size) = object.size {
				let bytes = size.to_le_bytes().to_vec();
				self.db
					.put(transaction, &size_key.pack_to_vec(), &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
			if let Some(weight) = object.weight {
				let bytes = weight.to_le_bytes().to_vec();
				self.db
					.put(transaction, &weight_key.pack_to_vec(), &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
		}
		return Ok(output);
	}

	pub fn update_object_batch_with_transaction(
		&self,
		transaction: &mut lmdb::RwTxn,
		arg: super::PutObjectBatchArg,
	) -> tg::Result<()> {
		for object in arg.objects {
			let id = object.id.clone();
			let complete_key = (0, id.to_bytes(), 0);
			let count_key = (0, id.to_bytes(), 1);
			let depth_key = (0, id.to_bytes(), 2);
			let incomplete_children_key = (0, id.to_bytes(), 3);
			let size_key = (0, id.to_bytes(), 4);
			let weight_key = (0, id.to_bytes(), 5);

			// Write the complete key.
			let bytes = if object.complete { [1] } else { [0] };
			self.db
				.delete(transaction, &complete_key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to delete the value"))?;
			self.db
				.put(transaction, &complete_key.pack_to_vec(), &bytes)
				.map_err(|source| tg::error!(!source, "failed to put the value"))?;

			if let Some(count) = object.count {
				let bytes = count.to_le_bytes().to_vec();
				self.db
					.put(transaction, &count_key.pack_to_vec(), &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
			if let Some(depth) = object.depth {
				let bytes = depth.to_le_bytes().to_vec();
				self.db
					.put(transaction, &depth_key.pack_to_vec(), &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
			if let Some(incomplete_children) = object.incomplete_children {
				let bytes = incomplete_children.to_le_bytes().to_vec();
				self.db
					.put(transaction, &incomplete_children_key.pack_to_vec(), &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
			if let Some(size) = object.size {
				let bytes = size.to_le_bytes().to_vec();
				self.db
					.put(transaction, &size_key.pack_to_vec(), &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
			if let Some(weight) = object.weight {
				let bytes = weight.to_le_bytes().to_vec();
				self.db
					.put(transaction, &weight_key.pack_to_vec(), &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
		}
		Ok(())
	}

	pub fn put_object_children_batch_with_transaction(
		&self,
		transaction: &mut lmdb::RwTxn,
		arg: super::PutObjectChildrenBatchArg,
	) -> tg::Result<()> {
		for item in arg.items {
			for child in item.children {
				// Insert the object, child key
				let key = (1, item.id.to_bytes());
				self.db
					.put(transaction, &key.pack_to_vec(), &child.to_bytes())
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
				// Insert the child, parent key
				let key = (2, child.to_bytes());
				self.db
					.put(transaction, &key.pack_to_vec(), &item.id.to_bytes())
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
		}
		Ok(())
	}

	pub fn decrement_object_incomplete_children_count_batch_with_transaction(
		&self,
		transaction: &mut lmdb::RwTxn,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<u64>> {
		let mut output = Vec::with_capacity(ids.len());
		for id in ids {
			let incomplete_children_key = (0, id.to_bytes(), 3);
			let incomplete_children = self
				.db
				.get(transaction, &incomplete_children_key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to get the value"))?;
			let incomplete_children = incomplete_children.ok_or_else(|| {
				tg::error!("expected an incomplete_children value for the object")
			})?;
			let mut incomplete_children =
				u64::from_le_bytes(incomplete_children.try_into().unwrap());
			if incomplete_children == 0 {
				return Err(tg::error!(
					"incomplete_children count is already 0 for the object"
				));
			}
			incomplete_children -= 1;
			let bytes = incomplete_children.to_le_bytes().to_vec();
			self.db
				.delete(transaction, &incomplete_children_key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to delete the value"))?;
			self.db
				.put(transaction, &incomplete_children_key.pack_to_vec(), &bytes)
				.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			output.push(incomplete_children);
		}
		Ok::<_, tg::Error>(output)
	}
}

// objects children
// (object_id, child_id) -> key
// need to also keep track of parents for each child.
// (child_id, parent_id)-> key
// Need to do range queries to find the parents of a child and the children of a parent.

//objects
// (object_id, 0) complete -> key
// (object_id, 1) count -> key
// (object_id, 2) depth -> key
// (object_id, 3) size -> key
// (object_id, 4) weight
// (object_id, 5) incomplete_children

// Step 1. Insert children into the object_children table.
// Step 2: Compute the incomplete children count for an object.
// For all of the object's children, see which ones are not present in the objects table or whose complete field is false. Set the incomplete children count, and keep track of all ids whose incomplete_children count is 0. => initial condition.
// Step 3. Insert objects into the objects table with their initial values.

// Begin loop
// Step 4: Update the count, depth, weight
// For all objects whose incomplete_children count is 0, find all of its children, grab their count, depth, and weight and add them up to get the count, depth, and weight of the parent.

// Step 5. For all objects who are now complete, find their parents and decrement their incomplete children count. All items whose incomplete_children count is 0 are now eligible for update themselves.
