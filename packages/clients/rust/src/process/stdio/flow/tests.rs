use {super::*, futures::FutureExt as _};

#[test]
fn windows_bound_bytes_and_tiny_chunk_metadata() {
	for length in [1, CHUNK_SIZE] {
		let mut sender = Sender::default();
		for _ in 0..MAX_CHUNKS {
			sender.send(length).unwrap();
		}
		assert!(!sender.available(length));
		let progress = read::Progress {
			consumed: (length * (MAX_CHUNKS / 2)) as u64,
		};
		sender.update(progress).unwrap();
		for _ in 0..MAX_CHUNKS / 2 {
			sender.send(length).unwrap();
		}
		assert!(!sender.available(length));
		assert!(sender.update(read::Progress { consumed: 0 }).is_err());
		assert!(
			sender
				.update(read::Progress { consumed: u64::MAX })
				.is_err()
		);
	}
}

#[test]
fn consumption_is_cumulative_and_batched() {
	for length in [1, CHUNK_SIZE] {
		let mut receiver = Receiver::default();
		for batch in 1..=3 {
			for _ in 0..MAX_CHUNKS / 2 - 1 {
				assert!(receiver.consume(length).unwrap().is_none());
			}
			let progress = receiver.consume(length).unwrap().unwrap();
			assert_eq!(progress.consumed, (batch * MAX_CHUNKS / 2 * length) as u64);
		}
	}
}

#[tokio::test]
async fn a_read_streams_a_window_before_progress_and_completes_after_its_chunks() {
	let chunk = tg::process::stdio::Chunk {
		bytes: bytes::Bytes::from(vec![0; CHUNK_SIZE]),
		combined_position: 0,
		stream: tg::process::stdio::Stream::Stdout,
		stream_position: 0,
		timestamp: None,
	};
	let messages = std::iter::repeat_n(
		read::ServerMessage::Notification(read::Event::Chunk(chunk)),
		MAX_CHUNKS + 1,
	)
	.chain([read::ServerMessage::Response(read::Output::End)]);
	let (sender, receiver) = async_channel::bounded(4);
	let mut output = read(receiver.boxed(), stream::iter(messages.map(Ok)).boxed());
	for _ in 0..MAX_CHUNKS {
		assert!(matches!(
			output.try_next().now_or_never().unwrap().unwrap(),
			Some(read::ServerMessage::Notification(read::Event::Chunk(_)))
		));
	}
	assert!(output.try_next().now_or_never().is_none());
	let progress = read::Progress {
		consumed: WINDOW / 2,
	};
	sender
		.send(Ok(read::ClientMessage::Notification(progress)))
		.await
		.unwrap();
	assert!(matches!(
		output.try_next().await.unwrap(),
		Some(read::ServerMessage::Notification(read::Event::Chunk(_)))
	));
	assert!(matches!(
		output.try_next().await.unwrap(),
		Some(read::ServerMessage::Response(read::Output::End))
	));
	assert!(output.try_next().now_or_never().is_none());
	sender.send(Ok(read::ClientMessage::Ack)).await.unwrap();
	assert!(output.try_next().await.unwrap().is_none());
}

#[tokio::test]
async fn closing_an_idle_read_cancels_its_source() {
	let (sender, receiver) = async_channel::bounded(4);
	let mut output = read(receiver.boxed(), stream::pending().boxed());
	assert!(output.try_next().now_or_never().is_none());
	drop(sender);
	assert!(output.try_next().now_or_never().unwrap().is_err());
}
