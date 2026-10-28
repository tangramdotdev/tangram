use {
	crate::{
		Component, queue,
		request::{self, EnqueueRequestArg, RequestArg},
	},
	futures::future,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_server_core::{Server, indexer::EnqueueArg},
};

impl Component {
	#[tracing::instrument(name = "indexer.enqueue_batches", level = "debug", skip_all, fields(archive = arg.archive.items.len(), index = arg.index.items.len()), err(level = "debug"))]
	pub(crate) async fn enqueue(&self, server: &Server, arg: EnqueueArg) -> tg::Result<()> {
		// Serialize the batches.
		let archive = if arg.archive.is_empty() {
			None
		} else {
			Some(bytes::Bytes::from(arg.archive.serialize()?))
		};
		let index = if arg.index.is_empty() {
			None
		} else {
			Some(bytes::Bytes::from(arg.index.serialize()?))
		};
		if archive.is_none() && index.is_none() {
			return Ok(());
		}

		// Send every request to one indexer, and retry all of them with another indexer on failure.
		let start = rand::random::<u64>();
		let mut attempt = 0u64;
		let retry =
			tangram_futures::retry::Options::from(server.config.indexer.enqueue.retry.clone());
		tangram_futures::retry(&retry, || {
			let selection = start.wrapping_add(attempt);
			let refresh = attempt > 0;
			attempt = attempt.wrapping_add(1);

			// Fragment the batches with new batch IDs, so that a retry is not rejected by an indexer on which a previous attempt's batch timed out.
			let archive = archive.as_ref().map_or_else(Vec::new, |bytes| {
				fragment(bytes, server.config.object.archive_queue.fragment_size)
			});
			let index = index.as_ref().map_or_else(Vec::new, |bytes| {
				fragment(bytes, server.config.object.index_queue.fragment_size)
			});
			let requests = group(archive, index, server.config.indexer.enqueue.request_size);

			async move {
				let indexer = match self.select_indexer(server, selection, refresh).await {
					Ok(indexer) => indexer,
					Err(error) => return Ok(ControlFlow::Continue(error)),
				};
				match self.enqueue_with_indexer(server, &indexer, requests).await {
					Ok(()) => Ok(ControlFlow::Break(())),
					Err(error) => Ok(ControlFlow::Continue(error)),
				}
			}
		})
		.await?;

		Ok(())
	}

	async fn enqueue_with_indexer(
		&self,
		server: &Server,
		indexer: &tg::indexer::Id,
		requests: Vec<EnqueueRequestArg>,
	) -> tg::Result<()> {
		let requests = requests.into_iter().map(|arg| {
			let arg = RequestArg::Enqueue(arg);
			async move {
				let output = self
					.send_indexer_request(server, Some(indexer), arg)
					.await
					.map_err(|source| tg::error!(!source, "failed to send an indexer request"))?
					.map_err(|source| {
						tg::error!(!source, "the indexer failed to enqueue the fragments")
					})?;
				output
					.try_unwrap_enqueue()
					.map_err(|_| tg::error!("expected an enqueue response"))?;

				Ok::<_, tg::Error>(())
			}
		});
		let request = future::try_join_all(requests);
		tokio::time::timeout(server.config.indexer.enqueue.timeout, request)
			.await
			.map_err(|source| tg::error!(!source, "timed out enqueueing the batches"))??;

		Ok(())
	}
}

fn fragment(bytes: &bytes::Bytes, fragment_size: usize) -> Vec<request::Fragment> {
	let batch = uuid::Uuid::now_v7().into_bytes();
	let fragments = u64::try_from(bytes.len().div_ceil(fragment_size)).unwrap();
	bytes
		.chunks(fragment_size)
		.enumerate()
		.map(|(fragment, payload)| request::Fragment {
			batch,
			fragment: u64::try_from(fragment).unwrap(),
			fragments,
			payload: bytes.slice_ref(payload),
		})
		.collect()
}

fn group(
	archive: Vec<request::Fragment>,
	index: Vec<request::Fragment>,
	request_size: usize,
) -> Vec<EnqueueRequestArg> {
	// Fill each request with fragments up to the request size, always including at least one fragment.
	let mut requests = Vec::new();
	let mut request = EnqueueRequestArg::default();
	let mut size = 0;
	let fragments = archive
		.into_iter()
		.map(|fragment| (queue::Kind::Archive, fragment))
		.chain(
			index
				.into_iter()
				.map(|fragment| (queue::Kind::Index, fragment)),
		);
	for (kind, fragment) in fragments {
		let len = fragment.payload.len();
		let empty = request.archive.is_empty() && request.index.is_empty();
		if !empty && size + len > request_size {
			requests.push(std::mem::take(&mut request));
			size = 0;
		}
		size += len;
		match kind {
			queue::Kind::Archive => request.archive.push(fragment),
			queue::Kind::Index => request.index.push(fragment),
		}
	}
	if !request.archive.is_empty() || !request.index.is_empty() {
		requests.push(request);
	}

	requests
}

#[cfg(test)]
mod tests {
	use super::{fragment, group};

	#[test]
	fn groups_fragments_into_requests_up_to_the_request_size() {
		let archive = fragment(&bytes::Bytes::from(vec![0; 10]), 4);
		assert_eq!(archive.len(), 3);
		assert!(archive.iter().all(|fragment| fragment.fragments == 3));
		let index = fragment(&bytes::Bytes::from(vec![1; 8]), 4);
		let requests = group(archive, index, 8);
		let sizes = requests
			.iter()
			.map(|request| (request.archive.len(), request.index.len()))
			.collect::<Vec<_>>();
		assert_eq!(sizes, [(2, 0), (1, 1), (0, 1)]);
	}

	#[test]
	fn assigns_a_new_batch_id_each_time_a_batch_is_fragmented() {
		let bytes = bytes::Bytes::from(vec![0; 10]);
		let first = fragment(&bytes, 4);
		let second = fragment(&bytes, 4);
		assert!(
			first
				.iter()
				.all(|fragment| fragment.batch == first[0].batch)
		);
		assert_ne!(first[0].batch, second[0].batch);
	}

	#[test]
	fn includes_an_oversized_fragment_in_its_own_request() {
		let index = fragment(&bytes::Bytes::from(vec![0; 10]), 10);
		let requests = group(Vec::new(), index, 4);
		assert_eq!(requests.len(), 1);
		assert_eq!(requests[0].index.len(), 1);
	}
}
