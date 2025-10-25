use {
	super::state::{State, Variant},
	crate::Server,
	bytes::Bytes,
	std::collections::BTreeSet,
	tangram_client as tg,
	tangram_either::Either,
	tangram_messenger::Messenger as _,
};

impl Server {
	pub(super) async fn checkin_index(&self, state: &State, touched_at: i64) -> tg::Result<()> {
		let mut dirty_objects = std::collections::HashSet::new();
		for node in state.graph.nodes.values() {
			if node.dirty {
				if let Some(object_id) = &node.object_id {
					dirty_objects.insert(object_id.clone());
					if let Some(object) = state.objects.as_ref().unwrap().get(object_id)
						&& let Some(data) = &object.data
						&& let Some(graph_id) = match data {
							tg::object::Data::Directory(tg::directory::Data::Reference(
								reference,
							))
							| tg::object::Data::File(tg::file::Data::Reference(reference))
							| tg::object::Data::Symlink(tg::symlink::Data::Reference(reference)) => {
								reference.graph.as_ref()
							},
							_ => None,
						} {
						dirty_objects.insert(graph_id.clone().into());
					}
				}
				if let Variant::File(file) = &node.variant
					&& let Some(Either::Left(blob)) = &file.contents
				{
					dirty_objects.insert(blob.id.clone().into());
				}
			}
		}

		let mut messages: Vec<Bytes> = Vec::new();

		// Create put cache entry messages.
		if state.arg.options.destructive {
			if state.graph.nodes.get(&0).unwrap().dirty {
				let id = state
					.graph
					.nodes
					.get(&0)
					.unwrap()
					.object_id
					.as_ref()
					.unwrap()
					.clone()
					.try_into()
					.unwrap();
				let message =
					crate::index::Message::PutCacheEntry(crate::index::message::PutCacheEntry {
						id,
						touched_at,
					});
				let message = message.serialize()?;
				let _published = self
					.messenger
					.stream_publish("index".to_owned(), message)
					.await
					.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
			}
		} else {
			for node in state.graph.nodes.values() {
				if !node.dirty {
					continue;
				}
				let Variant::File(file) = &node.variant else {
					continue;
				};
				let Some(Either::Left(_)) = &file.contents else {
					continue;
				};
				let message =
					crate::index::Message::PutCacheEntry(crate::index::message::PutCacheEntry {
						id: node.object_id.as_ref().unwrap().clone().try_into().unwrap(),
						touched_at,
					});
				let message = message.serialize()?;
				messages.push(message);
			}
		}

		// Create the put object messages.
		for object in state.objects.as_ref().unwrap().values() {
			if !dirty_objects.contains(&object.id) {
				continue;
			}
			let cache_entry = object
				.cache_reference
				.as_ref()
				.map(|cache_reference| cache_reference.artifact.clone());
			let mut children = BTreeSet::new();
			if let Some(data) = &object.data {
				data.children(&mut children);
			}
			let complete = object.complete;
			let metadata = object.metadata.clone().unwrap_or_default();
			let message = crate::index::Message::PutObject(crate::index::message::PutObject {
				cache_entry,
				children,
				complete,
				id: object.id.clone(),
				metadata,
				size: object.size,
				touched_at,
			});
			let message = message.serialize()?;
			messages.push(message);
		}

		self.messenger
			.stream_batch_publish("index".to_owned(), messages.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the messages"))?
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the messages"))?;

		Ok(())
	}
}
