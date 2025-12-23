use {
	crate::{
		Server,
		checkin::{Graph, IndexCacheEntryMessages, IndexObjectMessages},
	},
	std::path::Path,
	tangram_client::prelude::*,
	tangram_messenger::prelude::*,
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
		let mut messages: Vec<crate::index::Message> = Vec::new();

		// Create put cache entry messages.
		if arg.options.cache_references {
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
				messages.push(message);
			} else {
				// Add cache entry messages.
				for message in cache_entry_messages {
					let message = crate::index::Message::PutCacheEntry(message);
					messages.push(message);
				}
			}
		}

		// Add put object messages.
		for (_, message) in object_messages {
			let message = crate::index::Message::PutObject(message);
			messages.push(message);
		}

		let message = crate::index::message::Messages(messages);
		self.messenger
			.stream_publish("index".to_owned(), message)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}
}
