use {
	crate::{
		Server,
		checkin::{Graph, IndexCacheEntryMessages, IndexObjectMessages},
	},
	bytes::Bytes,
	std::path::Path,
	tangram_client as tg,
	tangram_messenger::Messenger as _,
};

impl Server {
	pub(super) async fn checkin_index(
		&self,
		arg: &tg::checkin::Arg,
		graph: &Graph,
		object_messages: IndexObjectMessages,
		cache_entry_messages: IndexCacheEntryMessages,
		root: &Path,
		touched_at: i64,
	) -> tg::Result<()> {
		let mut messages: Vec<Bytes> = Vec::new();

		// Create put cache entry messages.
		if arg.options.destructive {
			let index = graph.paths.get(root).unwrap();
			let id = graph
				.nodes
				.get(index)
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
			// Serialize and add cache entry messages.
			for message in cache_entry_messages {
				let message = crate::index::Message::PutCacheEntry(message);
				let message = message.serialize()?;
				messages.push(message);
			}
		}

		// Serialize and add put object messages.
		for (_, message) in object_messages {
			let message = crate::index::Message::PutObject(message);
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
