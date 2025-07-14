use bytes::Bytes;
use foundationdb_tuple::TuplePack as _;
use heed::{self as lmdb, DatabaseFlags};
use http::header::CACHE_CONTROL;
use num::ToPrimitive as _;
use serde::de;
use tangram_client as tg;

use crate::{index::PutObjectBatchArg, process::complete};

pub struct Lmdb {
	db: lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>,
	env: lmdb::Env,
	sender: tokio::sync::mpsc::Sender<Message>,
	task: tokio::task::JoinHandle<()>,
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

		// Create the task.
		let (sender, receiver) = tokio::sync::mpsc::channel::<Message>(256);
		let task = tokio::spawn({
			let env = env.clone();
			async move { Self::task(env, db, receiver).await }
		});

		Ok(Self {
			db,
			env,
			sender,
			task,
		})
	}

	pub async fn try_get_object_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<super::Object>>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
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
				.get(&transaction, &complete_key.pack_to_vec())
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
				.get(&transaction, &count_key.pack_to_vec())
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
				.get(&transaction, &depth_key.pack_to_vec())
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
				.get(&transaction, &incomplete_children_key.pack_to_vec())
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
				.get(&transaction, &size_key.pack_to_vec())
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
				.get(&transaction, &weight_key.pack_to_vec())
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

	pub async fn try_get_object_complete_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let mut output = Vec::with_capacity(ids.len());
		for id in ids {
			let complete_key = (0, id.to_bytes(), 0);
			let complete = self
				.db
				.get(&transaction, &complete_key.pack_to_vec())
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

	pub async fn try_get_object_parents_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Vec<tg::object::Id>>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
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

	pub async fn try_get_object_children_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Vec<tg::object::Id>>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let mut output = Vec::with_capacity(ids.len());
		for id in ids {
			let mut children = Vec::new();
			let key = (1, id.to_bytes());
			let dup = self
				.db
				.get_duplicates(&transaction, &key.pack_to_vec())
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

	pub async fn try_get_object_incomplete_children_count_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<u64>> {
		let children_batch = self.try_get_object_children_batch(ids).await?;
		let mut output = Vec::with_capacity(ids.len());
		for children in children_batch {
			let children_objects = self.try_get_object_batch(&children).await?;
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

	pub async fn put_object_batch(&self, arg: super::PutObjectBatchArg) -> tg::Result<()> {
		if arg.objects.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = Message::PutObjects(PutObjectBatchMessage {
			objects: arg.objects.clone(),
			response_sender: sender,
		});
		self.sender
			.send(message)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the message"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub async fn put_object_children_batch(
		&self,
		arg: super::PutObjectChildrenBatchArg,
	) -> tg::Result<()> {
		if arg.items.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = Message::PutObjectChildren(PutObjectChildrenBatchMessage {
			items: arg.items.clone(),
			response_sender: sender,
		});
		self.sender
			.send(message)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the message"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub async fn decrement_object_incomplete_children_count_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<u64>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = Message::DecrementObjectIncompleteChildrenCountBatch(
			DecrementObjectIncompleteChildrenBatchArg {
				ids: ids.to_vec(),
				response_sender: sender,
			},
		);
		self.sender
			.send(message)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the message"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(response)
	}

	async fn task(
		env: lmdb::Env,
		db: lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>,
		mut receiver: tokio::sync::mpsc::Receiver<Message>,
	) {
		while let Some(message) = receiver.recv().await {
			match message {
				Message::PutObjects(message) => {
					let result = async {
						let mut transaction = env.write_txn().map_err(|source| {
							tg::error!(!source, "failed to begin a transaction")
						})?;
						for object in message.objects {
							let id = object.id.clone();
							let complete_key = (0, id.to_bytes(), 0);
							let count_key = (0, id.to_bytes(), 1);
							let depth_key = (0, id.to_bytes(), 2);
							let incomplete_children_key = (0, id.to_bytes(), 3);
							let size_key = (0, id.to_bytes(), 4);
							let weight_key = (0, id.to_bytes(), 5);

							// Write the complete key.
							let bytes = if object.complete { [1] } else { [0] };
							db.delete(&mut transaction, &complete_key.pack_to_vec())
								.map_err(|source| {
									tg::error!(!source, "failed to delete the value")
								})?;
							db.put(&mut transaction, &complete_key.pack_to_vec(), &bytes)
								.map_err(|source| tg::error!(!source, "failed to put the value"))?;

							if let Some(count) = object.count {
								let bytes = count.to_le_bytes().to_vec();
								db.put(&mut transaction, &count_key.pack_to_vec(), &bytes)
									.map_err(|source| {
										tg::error!(!source, "failed to put the value")
									})?;
							}
							if let Some(depth) = object.depth {
								let bytes = depth.to_le_bytes().to_vec();
								db.put(&mut transaction, &depth_key.pack_to_vec(), &bytes)
									.map_err(|source| {
										tg::error!(!source, "failed to put the value")
									})?;
							}
							if let Some(incomplete_children) = object.incomplete_children {
								let bytes = incomplete_children.to_le_bytes().to_vec();
								db.put(
									&mut transaction,
									&incomplete_children_key.pack_to_vec(),
									&bytes,
								)
								.map_err(|source| tg::error!(!source, "failed to put the value"))?;
							}
							if let Some(size) = object.size {
								let bytes = size.to_le_bytes().to_vec();
								db.put(&mut transaction, &size_key.pack_to_vec(), &bytes)
									.map_err(|source| {
										tg::error!(!source, "failed to put the value")
									})?;
							}
							if let Some(weight) = object.weight {
								let bytes = weight.to_le_bytes().to_vec();
								db.put(&mut transaction, &weight_key.pack_to_vec(), &bytes)
									.map_err(|source| {
										tg::error!(!source, "failed to put the value")
									})?;
							}
						}
						transaction.commit().map_err(|source| {
							tg::error!(!source, "failed to commit the transaction")
						})?;
						Ok::<_, tg::Error>(())
					}
					.await;
					message.response_sender.send(result).ok();
				},
				Message::PutObjectChildren(message) => {
					let result = async {
						let mut transaction = env.write_txn().map_err(|source| {
							tg::error!(!source, "failed to begin a transaction")
						})?;
						for item in message.items {
							for child in item.children {
								// Insert the object, child key
								let key = (1, item.id.to_bytes());
								db.put(&mut transaction, &key.pack_to_vec(), &child.to_bytes())
									.map_err(|source| {
										tg::error!(!source, "failed to put the value")
									})?;
								// Insert the child, parent key
								let key = (2, child.to_bytes());
								db.put(&mut transaction, &key.pack_to_vec(), &item.id.to_bytes())
									.map_err(|source| {
										tg::error!(!source, "failed to put the value")
									})?;
							}
						}
						transaction.commit().map_err(|source| {
							tg::error!(!source, "failed to commit the transaction")
						})?;
						Ok::<_, tg::Error>(())
					}
					.await;
					message.response_sender.send(result).ok();
				},
				Message::DecrementObjectIncompleteChildrenCountBatch(
					decrement_object_incomplete_children_batch_arg,
				) => {
					let result = async {
						let mut transaction = env.write_txn().map_err(|source| {
							tg::error!(!source, "failed to begin a transaction")
						})?;
						let mut output = Vec::with_capacity(
							decrement_object_incomplete_children_batch_arg.ids.len(),
						);
						for id in &decrement_object_incomplete_children_batch_arg.ids {
							let incomplete_children_key = (0, id.to_bytes(), 3);
							let incomplete_children = db
								.get(&transaction, &incomplete_children_key.pack_to_vec())
								.map_err(|source| tg::error!(!source, "failed to get the value"))?;
							if let Some(incomplete_children) = incomplete_children {
								let mut incomplete_children =
									u64::from_le_bytes(incomplete_children.try_into().unwrap());
								if incomplete_children > 0 {
									incomplete_children -= 1;
									let bytes = incomplete_children.to_le_bytes().to_vec();
									let flags = lmdb::PutFlags::NO_OVERWRITE;
									let result = db.put_with_flags(
										&mut transaction,
										flags,
										&incomplete_children_key.pack_to_vec(),
										&bytes,
									);
									match result {
										Ok(())
										| Err(lmdb::Error::Mdb(lmdb::MdbError::KeyExist)) => (),
										Err(error) => {
											return Err(tg::error!(
												!error,
												"failed to put the value"
											));
										},
									}
								}
								output.push(incomplete_children);
							} else {
								output.push(0);
							}
						}
						transaction.commit().map_err(|source| {
							tg::error!(!source, "failed to commit the transaction")
						})?;
						Ok::<_, tg::Error>(output)
					}
					.await;
					decrement_object_incomplete_children_batch_arg
						.response_sender
						.send(result)
						.ok();
				},
			}
		}
	}
}

impl Drop for Lmdb {
	fn drop(&mut self) {
		self.task.abort();
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
