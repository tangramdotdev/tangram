use {
	super::state::{State, Variant},
	crate::Server,
	bytes::Bytes,
	std::{collections::BTreeSet, sync::Arc},
	tangram_client as tg,
	tangram_messenger::Messenger as _,
};

impl Server {
	pub(super) async fn checkin_index(
		&self,
		state: &Arc<State>,
		touched_at: i64,
	) -> tg::Result<()> {
		let mut messages: Vec<Bytes> = Vec::new();

		// Create put cache entry messages.
		if state.arg.options.destructive {
			let id = state
				.graph
				.nodes
				.get(&0)
				.unwrap()
				.id
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
		} else {
			for (_, node) in &state.graph.nodes {
				let Variant::File(file) = &node.variant else {
					continue;
				};
				let Some(blob_id) = &file.contents else {
					continue;
				};
				let blob_object_id: tg::object::Id = blob_id.clone().into();
				let blob_object = state.objects.get(&blob_object_id);
				if blob_object.is_none() || blob_object.unwrap().cache_reference_range.is_none() {
					continue;
				}
				let message =
					crate::index::Message::PutCacheEntry(crate::index::message::PutCacheEntry {
						id: node.id.as_ref().unwrap().clone().try_into().unwrap(),
						touched_at,
					});
				let message = message.serialize()?;
				messages.push(message);
			}
		}

		// Create put object messages.
		for object in state.objects.values() {
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
