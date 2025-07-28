use super::{
	DeleteTagMessage, PutCacheEntryMessage, PutObjectMessage, PutProcessMessage, PutTagMessage,
	TouchObjectMessage, TouchProcessMessage,
};
use crate::Server;
use fnv::FnvHashSet;
use foundationdb_tuple::TuplePack as _;
use heed::{self as lmdb, DatabaseFlags};
use im::HashMap;
use std::collections::BTreeSet;
use tangram_client::{self as tg};

pub struct Lmdb {
	db: lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>,
	env: lmdb::Env,
}

#[derive(Clone, Debug)]
struct PutObjectArg {
	id: tg::object::Id,
	cache_reference: Option<tg::artifact::Id>,
	size: Option<u64>,
	touched_at: Option<i64>,
	incomplete_children: Option<u64>,
	complete: bool,
	count: Option<u64>,
	depth: Option<u64>,
	weight: Option<u64>,
}

#[derive(Clone, Debug)]
struct PutObjectBatchArg {
	objects: Vec<PutObjectArg>,
}

#[derive(Clone, Debug)]
struct PutObjectChildrenArg {
	id: tg::object::Id,
	children: BTreeSet<tg::object::Id>,
}

#[derive(Clone, Debug)]
struct PutObjectChildrenBatchArg {
	items: Vec<PutObjectChildrenArg>,
}

#[derive(Clone, Debug)]
pub struct Object {
	pub id: tg::object::Id,
	pub complete: bool,
	pub count: Option<u64>,
	pub depth: Option<u64>,
	pub incomplete_children: Option<u64>,
	pub size: Option<u64>,
	pub weight: Option<u64>,
}

impl Server {
	pub(super) async fn indexer_task_handle_messages_lmdb(
		&self,
		index: &Lmdb,
		_put_cache_entry_messages: Vec<PutCacheEntryMessage>,
		put_object_messages: Vec<PutObjectMessage>,
		_touch_object_messages: Vec<TouchObjectMessage>,
		_put_process_messages: Vec<PutProcessMessage>,
		_touch_process_messages: Vec<TouchProcessMessage>,
		_put_tag_messages: Vec<PutTagMessage>,
		_delete_tag_messages: Vec<DeleteTagMessage>,
	) -> tg::Result<()> {
		// Handle the messages.
		index.put_objects(&put_object_messages)?;
		Ok(())
	}
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

	fn put_objects(&self, messages: &[PutObjectMessage]) -> tg::Result<()> {
		// Get the unique messages.
		let unique_messages: HashMap<&tg::object::Id, &PutObjectMessage, fnv::FnvBuildHasher> =
			messages
				.iter()
				.map(|message| (&message.id, message))
				.collect();
		let messages = unique_messages
			.into_iter()
			.map(|(_, message)| message)
			.collect::<Vec<_>>();

		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Insert the children
		let object_children_batch_arg = PutObjectChildrenBatchArg {
			items: messages
				.iter()
				.map(|message| PutObjectChildrenArg {
					id: message.id.clone(),
					children: message.children.iter().cloned().collect(),
				})
				.collect::<Vec<_>>(),
		};
		self.put_object_children_batch_with_transaction(
			&mut transaction,
			object_children_batch_arg,
		)?;

		// Insert the objects.
		let objects: Vec<PutObjectArg> = messages
			.iter()
			.map(|message| PutObjectArg {
				id: message.id.clone(),
				cache_reference: message.cache_reference.clone(),
				size: Some(message.size),
				touched_at: Some(message.touched_at),
				incomplete_children: None,
				complete: false,
				count: None,
				depth: None,
				weight: None,
			})
			.collect();
		let arg = PutObjectBatchArg { objects };
		self.insert_if_not_exists_object_batch_with_transaction(&mut transaction, arg)?;

		// Update the incomplete children counts.
		let ids = self.update_incomplete_children_counts_batch_with_transaction(
			&mut transaction,
			messages
				.iter()
				.map(|message| message.id.clone())
				.collect::<Vec<_>>()
				.as_slice(),
		)?;

		let mut queue = ids;

		#[derive(Default)]
		struct Stats {
			count: u64,
			depth: u64,
			weight: u64,
		}

		// Process parent updates if needed.
		while !queue.is_empty() {
			// For all objects whose incomplete_children count is 0, find all of its children, grab their count, depth, and weight and add them up to get the count, depth, and weight of the parent.
			let children_batch =
				self.try_get_object_children_batch_with_transaction(&transaction, &queue)?;

			let mut objects_to_update: Vec<PutObjectArg> = Vec::new();
			for (id, children) in queue.iter().zip(children_batch.iter()) {
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
				objects_to_update.push(PutObjectArg {
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
				self.try_get_object_parents_batch_with_transaction(&transaction, &queue)?;

			let parents: Vec<tg::object::Id> = parents.into_iter().flatten().collect();

			let incomplete_children_counts = self
				.decrement_object_incomplete_children_count_batch_with_transaction(
					&mut transaction,
					&parents,
				)?;
			let parents_to_update: Vec<tg::object::Id> = parents
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

			let unique_parents: FnvHashSet<_> = parents_to_update.iter().cloned().collect();

			queue = unique_parents.into_iter().collect::<Vec<_>>();
		}
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	fn try_get_object_batch_with_transaction<'a>(
		&self,
		transaction: &'a lmdb::RwTxn<'a>,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<Object>>> {
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

			let object = Object {
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

	pub fn try_get_object_batch(&self, ids: &[tg::object::Id]) -> tg::Result<Vec<Option<Object>>> {
		let transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let output = self.try_get_object_batch_with_transaction(&transaction, ids)?;
		Ok(output)
	}

	fn try_get_object_complete_batch_with_transaction(
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
			let complete = match complete {
				Some([1]) => Some(true),
				Some([0]) => Some(false),
				Some(_) => panic!("Invalid boolean value stored in LMDB"),
				None => {
					output.push(None);
					continue;
				},
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

	fn try_get_object_parents_batch_with_transaction(
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
				.get_duplicates(transaction, &key.pack_to_vec())
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

	fn try_get_object_children_batch_with_transaction(
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

	fn update_incomplete_children_counts_batch_with_transaction(
		&self,
		transaction: &mut lmdb::RwTxn,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<tg::object::Id>> {
		let children_batch =
			self.try_get_object_children_batch_with_transaction(transaction, ids)?;
		let mut output = Vec::with_capacity(ids.len());
		for (id, children) in ids.iter().zip(children_batch) {
			let children_objects =
				self.try_get_object_batch_with_transaction(transaction, &children)?;
			let mut incomplete_children_count: u64 = 0;
			for object in children_objects {
				if let Some(object) = object {
					if !object.complete {
						incomplete_children_count += 1;
					}
				} else {
					incomplete_children_count += 1;
				}
			}
			// Update the incomplete children count for the object.
			let incomplete_children_key = (0, id.to_bytes(), 3);
			let bytes = incomplete_children_count.to_le_bytes().to_vec();
			self.db
				.put(transaction, &incomplete_children_key.pack_to_vec(), &bytes)
				.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			let complete_key = (0, id.to_bytes(), 0);
			let complete_bytes = self
				.db
				.get(transaction, &complete_key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to get the value"))?;
			let complete = match complete_bytes {
				Some([1]) => true,
				Some([0]) => false,
				Some(_) => panic!("Invalid boolean value stored in LMDB"),
				None => return Err(tg::error!("expected a complete value for the object")),
			};
			if !complete && incomplete_children_count == 0 {
				output.push(id.clone());
			}
		}
		Ok(output)
	}

	fn insert_if_not_exists_object_batch_with_transaction(
		&self,
		transaction: &mut lmdb::RwTxn,
		arg: PutObjectBatchArg,
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
		Ok(output)
	}

	fn update_object_batch_with_transaction(
		&self,
		transaction: &mut lmdb::RwTxn,
		arg: PutObjectBatchArg,
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
				let count_key = count_key.pack_to_vec();
				let bytes = count.to_le_bytes().to_vec();
				self.db
					.delete(transaction, &count_key)
					.map_err(|source| tg::error!(!source, "failed to delete the value"))?;
				self.db
					.put(transaction, &count_key, &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
			if let Some(depth) = object.depth {
				let depth_key = depth_key.pack_to_vec();
				let bytes = depth.to_le_bytes().to_vec();
				self.db
					.delete(transaction, &depth_key)
					.map_err(|source| tg::error!(!source, "failed to delete the value"))?;
				self.db
					.put(transaction, &depth_key, &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
			if let Some(incomplete_children) = object.incomplete_children {
				let incomplete_children_key = incomplete_children_key.pack_to_vec();
				let bytes = incomplete_children.to_le_bytes().to_vec();
				self.db
					.delete(transaction, &incomplete_children_key)
					.map_err(|source| tg::error!(!source, "failed to delete the value"))?;
				self.db
					.put(transaction, &incomplete_children_key, &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
			if let Some(size) = object.size {
				let size_key = size_key.pack_to_vec();
				let bytes = size.to_le_bytes().to_vec();
				self.db
					.delete(transaction, &size_key)
					.map_err(|source| tg::error!(!source, "failed to delete the value"))?;
				self.db
					.put(transaction, &size_key, &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
			if let Some(weight) = object.weight {
				let weight_key = weight_key.pack_to_vec();
				let bytes = weight.to_le_bytes().to_vec();
				self.db
					.delete(transaction, &weight_key)
					.map_err(|source| tg::error!(!source, "failed to delete the value"))?;
				self.db
					.put(transaction, &weight_key, &bytes)
					.map_err(|source| tg::error!(!source, "failed to put the value"))?;
			}
		}
		Ok(())
	}

	fn put_object_children_batch_with_transaction(
		&self,
		transaction: &mut lmdb::RwTxn,
		arg: PutObjectChildrenBatchArg,
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

	fn decrement_object_incomplete_children_count_batch_with_transaction(
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
