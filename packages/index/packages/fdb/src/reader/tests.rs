use {super::Index, std::time::Duration, tangram_index::read::Request};

#[tokio::test]
async fn batches_drain_after_the_request_channel_closes() {
	let (sender, receiver) = tokio::sync::mpsc::channel(5);
	let mut responses = Vec::new();
	for _ in 0..5 {
		let (response, receiver) = tokio::sync::oneshot::channel();
		sender
			.send((Request::GetTransactionId, response))
			.await
			.unwrap();
		responses.push(receiver);
	}
	drop(sender);

	let (sender, batches) = async_channel::bounded(1);
	let producer = Index::reader_batch_task(receiver, sender, 2);
	let consumer = async move {
		let mut sizes = Vec::new();
		while let Ok(batch) = batches.recv().await {
			sizes.push(batch.len());
			for (_, sender) in batch {
				assert!(
					sender
						.send(Ok(tangram_index::read::Response::GetTransactionId(42)))
						.is_ok()
				);
			}
		}
		assert_eq!(sizes, [2, 2, 1]);
	};
	tokio::time::timeout(Duration::from_secs(5), async {
		futures::future::join(producer, consumer).await;
		for response in responses {
			assert!(matches!(
				response.await.unwrap().unwrap(),
				tangram_index::read::Response::GetTransactionId(42)
			));
		}
	})
	.await
	.unwrap();
}

#[tokio::test]
async fn closing_the_batch_receivers_releases_the_requests() {
	let (sender, receiver) = tokio::sync::mpsc::channel(2);
	let mut responses = Vec::new();
	for _ in 0..2 {
		let (response, receiver) = tokio::sync::oneshot::channel();
		sender
			.send((Request::GetTransactionId, response))
			.await
			.unwrap();
		responses.push(receiver);
	}
	let (batches, receiver_batches) = async_channel::bounded(1);
	drop(receiver_batches);

	tokio::time::timeout(Duration::from_secs(5), async {
		Index::reader_batch_task(receiver, batches, 1).await;
		assert!(sender.is_closed());
		for response in responses {
			assert!(response.await.is_err());
		}
	})
	.await
	.unwrap();
}
