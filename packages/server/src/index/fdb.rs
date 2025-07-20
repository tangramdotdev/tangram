use fnv::FnvHashSet;
use foundationdb::{self as fdb, FdbBindingError};
use foundationdb_tuple::TuplePack as _;
use futures::TryStreamExt as _;
use im::HashMap;
use std::pin::pin;
use tangram_client::{self as tg};

use crate::index::PutObjectBatchArg;

pub struct Fdb {
	database: fdb::Database,
}

impl Fdb {
	pub fn new(config: &crate::config::FdbIndex) -> tg::Result<Self> {
		let path = config
			.path
			.as_ref()
			.map(|path| path.as_os_str().to_str().unwrap());
		let database = fdb::Database::new(path)
			.map_err(|source| tg::error!(!source, "failed to open the database"))?;
		Ok(Self { database })
	}

	pub async fn put_objects(&self, messages: &[super::PutObjectMessage]) -> tg::Result<()> {
		// Get the unique messages.
		let unique_messages: HashMap<
			&tg::object::Id,
			&super::PutObjectMessage,
			fnv::FnvBuildHasher,
		> = messages
			.iter()
			.map(|message| (&message.id, message))
			.collect();
		let messages = unique_messages
			.into_iter()
			.map(|(_, message)| message)
			.collect::<Vec<_>>();

		self.database
			.run(|transaction, _| {
				let messages = messages.clone();
				async move {
					// Insert the children
					let items = messages
						.iter()
						.map(|message| crate::index::PutObjectChildrenArg {
							id: message.id.clone(),
							children: message.children.iter().cloned().collect(),
						})
						.collect::<Vec<_>>();
					let object_children_batch_arg =
						crate::index::PutObjectChildrenBatchArg { items };
					Self::put_object_children_batch_with_transaction(
						&transaction,
						object_children_batch_arg,
					)
					.await?;

					// Insert the objects.
					let objects: Vec<crate::index::PutObjectArg> = messages
						.iter()
						.map(|message| crate::index::PutObjectArg {
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
					Self::insert_if_not_exists_object_batch_with_transaction(&transaction, arg)
						.await?;

					// // Update the incomplete children counts.
					// let ids = Self::update_incomplete_children_counts_batch_with_transaction(
					// 	&transaction,
					// 	&messages
					// 		.iter()
					// 		.map(|message| message.id.clone())
					// 		.collect::<Vec<_>>(),
					// )
					// .await?;

					// let mut queue = ids;

					// #[derive(Default)]
					// struct Stats {
					// 	count: u64,
					// 	depth: u64,
					// 	weight: u64,
					// }

					// // Process parent updates if needed.
					// while !queue.is_empty() {
					// 	// For all objects whose incomplete_children count is 0, find all of its children, grab their count, depth, and weight and add them up to get the count, depth, and weight of the parent.
					// 	let children_batch = Self::try_get_object_children_batch_with_transaction(
					// 		&transaction,
					// 		&queue,
					// 	)
					// 	.await?;

					// 	let mut objects_to_update: Vec<crate::index::PutObjectArg> = Vec::new();
					// 	for (id, children) in queue.iter().zip(children_batch.iter()) {
					// 		let children_objects =
					// 			Self::try_get_object_batch_with_transaction(&transaction, children)
					// 				.await?;
					// 		let object = Self::try_get_object_batch_with_transaction(
					// 			&transaction,
					// 			&[id.clone()],
					// 		)
					// 		.await?
					// 		.first()
					// 		.cloned()
					// 		.unwrap()
					// 		.unwrap();
					// 		let stats = children_objects.iter().try_fold(
					// 			Stats::default(),
					// 			|mut stats, child| {
					// 				let child = child.as_ref().ok_or_else(|| {
					// 					FdbBindingError::new_custom_error(
					// 						"failed to get child object in indexer task".into(),
					// 					)
					// 				})?;
					// 				stats.count += child.count.ok_or_else(|| {
					// 					FdbBindingError::new_custom_error(
					// 						"expected a count for the child object".into(),
					// 					)
					// 				})?;
					// 				stats.depth =
					// 					stats.depth.max(child.depth.ok_or_else(|| {
					// 						FdbBindingError::new_custom_error(
					// 							"expected a depth for the child object".into(),
					// 						)
					// 					})?);
					// 				stats.weight += child.weight.ok_or_else(|| {
					// 					FdbBindingError::new_custom_error(
					// 						"expected a weight for the child object".into(),
					// 					)
					// 				})?;
					// 				Ok::<_, FdbBindingError>(stats)
					// 			},
					// 		)?;
					// 		objects_to_update.push(crate::index::PutObjectArg {
					// 			id: object.id.clone(),
					// 			cache_reference: None,
					// 			incomplete_children: Some(0),
					// 			size: None,
					// 			touched_at: None,
					// 			complete: true,
					// 			count: Some(1 + stats.count),
					// 			depth: Some(1 + stats.depth),
					// 			weight: Some(object.size.unwrap() + stats.weight),
					// 		});
					// 	}

					// 	// Update the count, depth, weight of the objects.
					// 	Self::update_object_batch_with_transaction(
					// 		&transaction,
					// 		PutObjectBatchArg {
					// 			objects: objects_to_update,
					// 		},
					// 	)
					// 	.await?;

					// 	// For all objects who are now complete, find their parents and decrement their incomplete children count. All items whose incomplete_children count is 0 are now eligible for update themselves.
					// 	let parents = Self::try_get_object_parents_batch_with_transaction(
					// 		&transaction,
					// 		&queue,
					// 	)
					// 	.await?;

					// 	let parents: Vec<tg::object::Id> = parents.into_iter().flatten().collect();

					// 	let incomplete_children_counts =
					// 	Self::decrement_object_incomplete_children_count_batch_with_transaction(
					// 		&transaction,
					// 		&parents,
					// 	)
					// 	.await?;
					// 	let parents_to_update: Vec<tg::object::Id> = parents
					// 		.iter()
					// 		.zip(incomplete_children_counts.iter())
					// 		.filter_map(|(parent, incomplete_children)| {
					// 			if *incomplete_children == 0 {
					// 				Some(parent.clone())
					// 			} else {
					// 				None
					// 			}
					// 		})
					// 		.collect();

					// 	let unique_parents: FnvHashSet<_> =
					// 		parents_to_update.iter().cloned().collect();

					// 	queue = unique_parents.into_iter().collect::<Vec<_>>();
					// }
					// Ok(())
					Ok(())
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the transaction failed"))?;
		Ok(())
	}

	pub async fn try_get_object_batch_with_transaction(
		transaction: &fdb::Transaction,
		ids: &[tg::object::Id],
	) -> Result<Vec<Option<super::Object>>, FdbBindingError> {
		let mut output = Vec::with_capacity(ids.len());
		for id in ids {
			let complete_key = (0, id.to_bytes(), 0);
			let count_key = (0, id.to_bytes(), 1);
			let depth_key = (0, id.to_bytes(), 2);
			let incomplete_children_key = (0, id.to_bytes(), 3);
			let size_key = (0, id.to_bytes(), 4);
			let weight_key = (0, id.to_bytes(), 5);

			let complete_bytes = transaction.get(&complete_key.pack_to_vec(), false).await?;
			let complete = match complete_bytes {
				Some(complete) if complete.as_ref() == [1] => true,
				Some(complete) if complete.as_ref() == [0] => false,
				Some(_) => {
					return Err(FdbBindingError::new_custom_error(
						"Invalid boolean value stored in FDB".into(),
					));
				},
				None => {
					output.push(None);
					continue;
				},
			};
			let count = transaction.get(&count_key.pack_to_vec(), false).await?;
			let count = match count {
				Some(count) => {
					let count = u64::from_le_bytes(count.as_ref().try_into().map_err(|_| {
						FdbBindingError::new_custom_error("invalid count bytes".into())
					})?);
					Some(count)
				},
				None => None,
			};
			let depth = transaction.get(&depth_key.pack_to_vec(), false).await?;
			let depth = match depth {
				Some(depth) => {
					let depth = u64::from_le_bytes(depth.as_ref().try_into().map_err(|_| {
						FdbBindingError::new_custom_error("invalid depth bytes".into())
					})?);
					Some(depth)
				},
				None => None,
			};
			let incomplete_children = transaction
				.get(&incomplete_children_key.pack_to_vec(), false)
				.await?;
			let incomplete_children = match incomplete_children {
				Some(incomplete_children) => {
					let incomplete_children = u64::from_le_bytes(
						incomplete_children.as_ref().try_into().map_err(|_| {
							FdbBindingError::new_custom_error(
								"invalid incomplete_children bytes".into(),
							)
						})?,
					);
					Some(incomplete_children)
				},
				None => None,
			};
			let size = transaction.get(&size_key.pack_to_vec(), false).await?;
			let size = match size {
				Some(size) => {
					let size = u64::from_le_bytes(size.as_ref().try_into().map_err(|_| {
						FdbBindingError::new_custom_error("invalid size bytes".into())
					})?);
					Some(size)
				},
				None => None,
			};
			let weight = transaction.get(&weight_key.pack_to_vec(), false).await?;
			let weight = match weight {
				Some(weight) => {
					let weight = u64::from_le_bytes(weight.as_ref().try_into().map_err(|_| {
						FdbBindingError::new_custom_error("invalid weight bytes".into())
					})?);
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

	pub async fn try_get_object_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<super::Object>>> {
		let output = self
			.database
			.run(|transaction, _| async move {
				Self::try_get_object_batch_with_transaction(&transaction, ids).await
			})
			.await
			.map_err(|source| tg::error!(!source, "the transaction failed"))?;
		Ok(output)
	}

	pub async fn try_get_object_complete_batch_with_transaction(
		transaction: &fdb::Transaction,
		ids: &[tg::object::Id],
	) -> Result<Vec<Option<bool>>, FdbBindingError> {
		let mut output = Vec::with_capacity(ids.len());
		for id in ids {
			let complete_key = (0, id.to_bytes(), 0);
			let complete = transaction.get(&complete_key.pack_to_vec(), false).await?;
			let complete = match complete {
				Some(complete) if complete.as_ref() == [1] => Some(true),
				Some(complete) if complete.as_ref() == [0] => Some(false),
				Some(_) => {
					return Err(FdbBindingError::new_custom_error(
						"Invalid boolean value stored in FDB".into(),
					));
				},
				None => {
					output.push(None);
					continue;
				},
			};
			output.push(complete);
		}
		Ok(output)
	}

	pub async fn try_get_object_complete_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
		let output = self
			.database
			.run(|transaction, _| async move {
				Self::try_get_object_complete_batch_with_transaction(&transaction, ids).await
			})
			.await
			.map_err(|source| tg::error!(!source, "the transaction failed"))?;
		Ok(output)
	}

	pub async fn try_get_object_parents_batch_with_transaction(
		transaction: &fdb::Transaction,
		ids: &[tg::object::Id],
	) -> Result<Vec<Vec<tg::object::Id>>, FdbBindingError> {
		let mut output = Vec::with_capacity(ids.len());
		for id in ids {
			let mut parents = Vec::new();
			let subspace = fdb::tuple::Subspace::all().subspace(&(2, id.to_bytes()));
			let mut range = fdb::RangeOption::from(subspace.range());
			range.mode = fdb::options::StreamingMode::WantAll;
			let stream = transaction.get_ranges(range, false);
			let mut stream = pin!(stream);
			while let Some(entries) = stream.try_next().await? {
				for entry in entries {
					// Extract parent ID from the key since value is now empty
					let key_tuple: Vec<foundationdb_tuple::Element> =
						foundationdb_tuple::unpack(entry.key()).map_err(|_| {
							FdbBindingError::new_custom_error("failed to unpack key".into())
						})?;
					if let Some(foundationdb_tuple::Element::Bytes(parent_bytes)) = key_tuple.get(2)
					{
						let parent_id =
							tg::object::Id::from_slice(parent_bytes.as_ref()).map_err(|_| {
								FdbBindingError::new_custom_error(
									"failed to deserialize parent id".into(),
								)
							})?;
						parents.push(parent_id);
					}
				}
			}
			output.push(parents);
		}
		Ok(output)
	}

	pub async fn try_get_object_children_batch_with_transaction(
		transaction: &fdb::Transaction,
		ids: &[tg::object::Id],
	) -> Result<Vec<Vec<tg::object::Id>>, FdbBindingError> {
		let mut output = Vec::with_capacity(ids.len());
		for id in ids {
			let mut children = Vec::new();
			let subspace = fdb::tuple::Subspace::all().subspace(&(1, id.to_bytes()));
			let mut range = fdb::RangeOption::from(subspace.range());
			range.mode = fdb::options::StreamingMode::WantAll;
			let stream = transaction.get_ranges(range, false);
			let mut stream = pin!(stream);
			while let Some(entries) = stream.try_next().await? {
				for entry in entries {
					// Extract child ID from the key since value is now empty
					let key_tuple: Vec<foundationdb_tuple::Element> =
						foundationdb_tuple::unpack(entry.key()).map_err(|_| {
							FdbBindingError::new_custom_error("failed to unpack key".into())
						})?;
					if let Some(foundationdb_tuple::Element::Bytes(child_bytes)) = key_tuple.get(2)
					{
						let child_id =
							tg::object::Id::from_slice(child_bytes.as_ref()).map_err(|_| {
								FdbBindingError::new_custom_error(
									"failed to deserialize child id".into(),
								)
							})?;
						children.push(child_id);
					}
				}
			}
			output.push(children);
		}
		Ok(output)
	}

	pub async fn update_incomplete_children_counts_batch_with_transaction(
		transaction: &fdb::Transaction,
		ids: &[tg::object::Id],
	) -> Result<Vec<tg::object::Id>, FdbBindingError> {
		let children_batch =
			Self::try_get_object_children_batch_with_transaction(transaction, ids).await?;
		let mut output = Vec::with_capacity(ids.len());
		for (id, children) in ids.iter().zip(children_batch) {
			let children_objects =
				Self::try_get_object_batch_with_transaction(transaction, &children).await?;
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
			transaction.set(&incomplete_children_key.pack_to_vec(), &bytes);
			let complete_key = (0, id.to_bytes(), 0);
			let complete_bytes = transaction.get(&complete_key.pack_to_vec(), false).await?;
			let complete = match complete_bytes {
				Some(complete) if complete.as_ref() == [1] => true,
				Some(complete) if complete.as_ref() == [0] => false,
				Some(_) => {
					return Err(FdbBindingError::new_custom_error(
						"Invalid boolean value stored in FDB".into(),
					));
				},
				None => {
					return Err(FdbBindingError::new_custom_error(
						"expected a complete value for the object".into(),
					));
				},
			};
			if !complete && incomplete_children_count == 0 {
				output.push(id.clone());
			}
		}
		Ok(output)
	}

	pub async fn insert_if_not_exists_object_batch_with_transaction(
		transaction: &fdb::Transaction,
		arg: super::PutObjectBatchArg,
	) -> Result<Vec<bool>, FdbBindingError> {
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
			if transaction
				.get(&complete_key.pack_to_vec(), false)
				.await?
				.is_some()
			{
				output.push(false);
				continue;
			}
			let bytes = if object.complete { [1] } else { [0] };
			transaction.set(&complete_key.pack_to_vec(), &bytes);

			if let Some(count) = object.count {
				let bytes = count.to_le_bytes().to_vec();
				transaction.set(&count_key.pack_to_vec(), &bytes);
			}
			if let Some(depth) = object.depth {
				let bytes = depth.to_le_bytes().to_vec();
				transaction.set(&depth_key.pack_to_vec(), &bytes);
			}
			if let Some(incomplete_children) = object.incomplete_children {
				let bytes = incomplete_children.to_le_bytes().to_vec();
				transaction.set(&incomplete_children_key.pack_to_vec(), &bytes);
			}
			if let Some(size) = object.size {
				let bytes = size.to_le_bytes().to_vec();
				transaction.set(&size_key.pack_to_vec(), &bytes);
			}
			if let Some(weight) = object.weight {
				let bytes = weight.to_le_bytes().to_vec();
				transaction.set(&weight_key.pack_to_vec(), &bytes);
			}
			output.push(true);
		}
		Ok(output)
	}

	pub async fn update_object_batch_with_transaction(
		transaction: &fdb::Transaction,
		arg: super::PutObjectBatchArg,
	) -> Result<(), FdbBindingError> {
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
			transaction.set(&complete_key.pack_to_vec(), &bytes);

			if let Some(count) = object.count {
				let bytes = count.to_le_bytes().to_vec();
				transaction.set(&count_key.pack_to_vec(), &bytes);
			}
			if let Some(depth) = object.depth {
				let bytes = depth.to_le_bytes().to_vec();
				transaction.set(&depth_key.pack_to_vec(), &bytes);
			}
			if let Some(incomplete_children) = object.incomplete_children {
				let bytes = incomplete_children.to_le_bytes().to_vec();
				transaction.set(&incomplete_children_key.pack_to_vec(), &bytes);
			}
			if let Some(size) = object.size {
				let bytes = size.to_le_bytes().to_vec();
				transaction.set(&size_key.pack_to_vec(), &bytes);
			}
			if let Some(weight) = object.weight {
				let bytes = weight.to_le_bytes().to_vec();
				transaction.set(&weight_key.pack_to_vec(), &bytes);
			}
		}
		Ok(())
	}

	pub async fn put_object_children_batch_with_transaction(
		transaction: &fdb::Transaction,
		arg: super::PutObjectChildrenBatchArg,
	) -> Result<(), FdbBindingError> {
		for item in arg.items {
			for child in item.children {
				// Insert the object, child key - include child ID to make key unique
				let key = (1, item.id.to_bytes(), child.to_bytes());
				transaction.set(&key.pack_to_vec(), &[]);
				// Insert the child, parent key - include parent ID to make key unique
				let key = (2, child.to_bytes(), item.id.to_bytes());
				transaction.set(&key.pack_to_vec(), &[]);
			}
		}
		Ok(())
	}

	pub async fn decrement_object_incomplete_children_count_batch_with_transaction(
		transaction: &fdb::Transaction,
		ids: &[tg::object::Id],
	) -> Result<Vec<u64>, FdbBindingError> {
		let mut output = Vec::with_capacity(ids.len());
		for id in ids {
			let incomplete_children_key = (0, id.to_bytes(), 3);
			let incomplete_children = transaction
				.get(&incomplete_children_key.pack_to_vec(), false)
				.await?;
			let incomplete_children = incomplete_children.ok_or_else(|| {
				FdbBindingError::new_custom_error(
					"expected an incomplete_children value for the object".into(),
				)
			})?;
			let mut incomplete_children =
				u64::from_le_bytes(incomplete_children.as_ref().try_into().map_err(|_| {
					FdbBindingError::new_custom_error("invalid incomplete_children bytes".into())
				})?);
			if incomplete_children == 0 {
				return Err(FdbBindingError::new_custom_error(
					"incomplete_children count is already 0 for the object".into(),
				));
			}
			incomplete_children -= 1;
			let bytes = incomplete_children.to_le_bytes().to_vec();
			transaction.set(&incomplete_children_key.pack_to_vec(), &bytes);
			output.push(incomplete_children);
		}
		Ok(output)
	}
}
