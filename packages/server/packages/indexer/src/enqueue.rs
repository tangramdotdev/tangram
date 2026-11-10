use {
	crate::{
		Component,
		request::{EnqueueRequestArg, RequestArg},
	},
	futures::future,
	tangram_client::prelude::*,
	tangram_server_core::{Server, indexer::EnqueueArg},
};

#[derive(Default)]
struct Request {
	archive: tangram_server_core::archive::batch::Arg,
	index: tangram_index::write::Arg,
	size: usize,
}

enum Item {
	Archive(tangram_server_core::archive::batch::Item),
	Index(tangram_index::Item),
}

impl Component {
	#[tracing::instrument(name = "indexer.enqueue_items", level = "debug", skip_all, fields(archive = arg.archive.items.len(), index = arg.index.items.len()), err(level = "debug"))]
	pub(crate) async fn enqueue(&self, server: &Server, arg: EnqueueArg) -> tg::Result<()> {
		// Pack the items into requests.
		let requests = pack(arg, server.config.indexer.enqueue.request_size)?;
		if requests.is_empty() {
			return Ok(());
		}

		// Send every request to one indexer until it answers, and move a request to another indexer only if the indexer is unavailable.
		let start = tangram_server_core::service::start();
		let requests = requests
			.into_iter()
			.map(|arg| self.enqueue_request(server, start, arg));
		future::try_join_all(requests).await?;

		Ok(())
	}

	async fn enqueue_request(
		&self,
		server: &Server,
		start: u64,
		arg: EnqueueRequestArg,
	) -> tg::Result<()> {
		let retry = server.config.indexer.enqueue.retry.clone().into();
		let (_, output) =
			tangram_server_core::service::retry(&self.indexers, server, &retry, start, |indexer| {
				let arg = RequestArg::Enqueue(arg.clone());
				async move {
					let response = self
						.send_indexer_request(server, &indexer, arg)
						.await
						.map_err(|source| {
							tg::error!(!source, "failed to send an indexer request")
						})?;
					Ok(response)
				}
			})
			.await?;
		output
			.map_err(|source| tg::error!(!source, "the indexer failed to enqueue the items"))?
			.try_unwrap_enqueue()
			.map_err(|_| tg::error!("expected an enqueue response"))?;

		Ok(())
	}
}

impl Request {
	fn is_empty(&self) -> bool {
		self.archive.is_empty() && self.index.is_empty()
	}
}

fn pack(arg: EnqueueArg, request_size: usize) -> tg::Result<Vec<EnqueueRequestArg>> {
	// Add whole items to each request up to the request size, so that an item larger than the request size is in a request by itself.
	let mut requests = Vec::new();
	let mut request = Request::default();
	let mut buffer = Vec::new();
	let items = arg
		.archive
		.items
		.into_iter()
		.map(Item::Archive)
		.chain(arg.index.items.into_iter().map(Item::Index));
	for item in items {
		buffer.clear();
		match &item {
			Item::Archive(item) => tangram_serialize::to_writer(&mut buffer, item),
			Item::Index(item) => tangram_serialize::to_writer(&mut buffer, item),
		}
		.map_err(|error| tg::error!(!error, "failed to serialize an item"))?;
		let size = buffer.len();
		if !request.is_empty() && request.size + size > request_size {
			requests.push(std::mem::take(&mut request));
		}
		request.size += size;
		match item {
			Item::Archive(item) => request.archive.items.push(item),
			Item::Index(item) => request.index.items.push(item),
		}
	}
	if !request.is_empty() {
		requests.push(request);
	}

	// Serialize each request's archive items and index items.
	let requests = requests
		.into_iter()
		.map(|request| {
			let archive = if request.archive.is_empty() {
				None
			} else {
				Some(bytes::Bytes::from(request.archive.serialize()?))
			};
			let index = if request.index.is_empty() {
				None
			} else {
				Some(bytes::Bytes::from(request.index.serialize()?))
			};
			let request = EnqueueRequestArg { archive, index };
			Ok(request)
		})
		.collect::<tg::Result<_>>()?;

	Ok(requests)
}

#[cfg(test)]
mod tests {
	use {super::pack, tangram_client::prelude::*, tangram_server_core::indexer::EnqueueArg};

	fn index_item() -> tangram_index::Item {
		tangram_index::Item::DeleteCheckout(Box::new(tangram_index::checkout::delete::Arg {
			id: tg::tag::Id::new().into(),
		}))
	}

	fn size(item: &tangram_index::Item) -> usize {
		tangram_serialize::to_vec(item).unwrap().len()
	}

	fn counts(arg: EnqueueArg, request_size: usize) -> Vec<(usize, usize)> {
		pack(arg, request_size)
			.unwrap()
			.into_iter()
			.map(|request| {
				let archive = request.archive.map_or(0, |bytes| {
					tangram_server_core::archive::batch::Arg::deserialize(&bytes)
						.unwrap()
						.items
						.len()
				});
				let index = request.index.map_or(0, |bytes| {
					tangram_index::write::Arg::deserialize(&bytes)
						.unwrap()
						.items
						.len()
				});
				(archive, index)
			})
			.collect()
	}

	#[test]
	fn packs_whole_items_into_requests_up_to_the_request_size() {
		let items = (0..5).map(|_| index_item()).collect::<Vec<_>>();
		assert!(items.iter().all(|item| size(item) == size(&items[0])));
		let request_size = 2 * size(&items[0]);
		let index = tangram_index::write::Arg { items };
		let arg = EnqueueArg {
			archive: tangram_server_core::archive::batch::Arg::default(),
			index,
		};
		assert_eq!(counts(arg, request_size), [(0, 2), (0, 2), (0, 1)]);
	}

	#[test]
	fn packs_archive_and_index_items_into_one_request() {
		let item = tangram_server_core::archive::batch::Item {
			object: tg::blob::Id::new(b"blob").into(),
			put: [0; 16],
		};
		let archive = tangram_server_core::archive::batch::Arg { items: vec![item] };
		let index = tangram_index::write::Arg {
			items: vec![index_item()],
		};
		let arg = EnqueueArg { archive, index };
		assert_eq!(counts(arg, usize::MAX), [(1, 1)]);
	}

	#[test]
	fn puts_an_oversized_item_in_a_request_by_itself() {
		let items = (0..3).map(|_| index_item()).collect::<Vec<_>>();
		let request_size = size(&items[0]) - 1;
		let index = tangram_index::write::Arg { items };
		let arg = EnqueueArg {
			archive: tangram_server_core::archive::batch::Arg::default(),
			index,
		};
		assert_eq!(counts(arg, request_size), [(0, 1), (0, 1), (0, 1)]);
	}

	#[test]
	fn sends_no_requests_without_items() {
		let arg = EnqueueArg {
			archive: tangram_server_core::archive::batch::Arg::default(),
			index: tangram_index::write::Arg::default(),
		};
		assert!(pack(arg, 1).unwrap().is_empty());
	}
}
