use {
	super::*,
	std::sync::{Arc, Mutex},
};

#[derive(Clone, Default)]
struct TestMessenger {
	state: Arc<Mutex<TestState>>,
}

#[derive(Default)]
struct TestState {
	count: u64,
	drop_every: Option<u64>,
	duplicate_every: Option<u64>,
	held: Option<(String, Bytes)>,
	reorder_every: Option<u64>,
	subscribers: Vec<(String, mpsc::UnboundedSender<Bytes>)>,
}

impl TestMessenger {
	fn with_faults(drop_every: u64, duplicate_every: u64, reorder_every: u64) -> Self {
		let state = TestState {
			drop_every: Some(drop_every),
			duplicate_every: Some(duplicate_every),
			reorder_every: Some(reorder_every),
			..Default::default()
		};
		Self {
			state: Arc::new(Mutex::new(state)),
		}
	}

	fn deliver(state: &mut TestState, subject: &str, bytes: &Bytes) {
		state.subscribers.retain(|(_, sender)| !sender.is_closed());
		for (name, sender) in &state.subscribers {
			if name == subject {
				sender.send(bytes.clone()).ok();
			}
		}
	}
}

impl crate::Messenger for TestMessenger {
	async fn publish<T>(&self, subject: String, payload: T) -> Result<(), crate::Error>
	where
		T: Payload,
	{
		let bytes = payload.serialize()?;
		let mut state = self.state.lock().unwrap();
		state.count += 1;
		let count = state.count;
		if state
			.drop_every
			.is_some_and(|every| count.is_multiple_of(every))
		{
			return Ok(());
		}
		if state
			.reorder_every
			.is_some_and(|every| count.is_multiple_of(every))
			&& state.held.is_none()
		{
			state.held = Some((subject, bytes));
			return Ok(());
		}
		Self::deliver(&mut state, &subject, &bytes);
		if state
			.duplicate_every
			.is_some_and(|every| count.is_multiple_of(every))
		{
			Self::deliver(&mut state, &subject, &bytes);
		}
		if let Some((subject, bytes)) = state.held.take() {
			Self::deliver(&mut state, &subject, &bytes);
		}
		Ok(())
	}

	async fn subscribe<T>(
		&self,
		subject: String,
	) -> Result<
		impl futures::Stream<Item = Result<crate::Message<T>, crate::Error>> + Send + 'static,
		crate::Error,
	>
	where
		T: Payload,
	{
		let (sender, mut receiver) = mpsc::unbounded_channel();
		self.state
			.lock()
			.unwrap()
			.subscribers
			.push((subject.clone(), sender));
		let stream = futures::stream::poll_fn(move |cx| receiver.poll_recv(cx)).map(move |bytes| {
			let payload = T::deserialize(bytes)?;
			let message = crate::Message {
				payload,
				subject: subject.clone(),
			};
			Ok(message)
		});
		Ok(stream)
	}

	async fn queue_subscribe<T>(
		&self,
		subject: String,
		_queue_group: String,
	) -> Result<
		impl futures::Stream<Item = Result<crate::Message<T>, crate::Error>> + Send + 'static,
		crate::Error,
	>
	where
		T: Payload,
	{
		self.subscribe(subject).await
	}
}

fn options() -> Options {
	Options {
		ack_delay: Duration::from_millis(2),
		ack_threshold: 128,
		max_retransmits: 10,
		message_overhead: 8,
		timeout: Duration::from_millis(20),
		window: 256,
	}
}

fn items(count: usize, size: usize) -> Vec<Bytes> {
	(0..count)
		.map(|index| Bytes::from(vec![u8::try_from(index % 251).unwrap(); size]))
		.collect()
}

type Half = (Sender<Bytes>, Receiver<Bytes>);

async fn pair(messenger: &TestMessenger) -> (Half, Half) {
	let second = open(messenger, "relay.b", "relay.a", options())
		.await
		.unwrap();
	let first = open(messenger, "relay.a", "relay.b", options())
		.await
		.unwrap();
	(first, second)
}

async fn drain(receiver: Receiver<Bytes>) -> Vec<Bytes> {
	receiver.map(Result::unwrap).collect().await
}

async fn finished<T>(task: tokio::task::JoinHandle<Result<T, Error>>) -> T {
	tokio::time::timeout(Duration::from_secs(10), task)
		.await
		.unwrap()
		.unwrap()
		.unwrap()
}

#[tokio::test]
async fn delivers_in_order_including_items_larger_than_the_window() {
	let messenger = TestMessenger::default();
	let ((sender, first_receiver), (second_sender, receiver)) = pair(&messenger).await;
	let mut expected = items(50, 40);
	expected.extend(items(3, 1024));
	let input = futures::stream::iter(expected.clone().into_iter().map(Ok::<_, Bytes>));
	let sender = tokio::spawn(sender.send_all(input));
	let second_sender = tokio::spawn(second_sender.finish());
	let first_receiver = tokio::spawn(drain(first_receiver));
	assert_eq!(drain(receiver).await, expected);
	finished(sender).await;
	finished(second_sender).await;
	assert_eq!(first_receiver.await.unwrap(), Vec::<Bytes>::new());
}

#[tokio::test]
async fn both_directions_deliver_in_order_under_loss_duplication_and_reordering() {
	let messenger = TestMessenger::with_faults(7, 5, 11);
	let ((first_sender, first_receiver), (second_sender, second_receiver)) = pair(&messenger).await;
	let forward = items(150, 30);
	let backward = items(100, 20);
	let first_sender = tokio::spawn(
		first_sender.send_all(futures::stream::iter(forward.clone().into_iter().map(Ok))),
	);
	let second_sender = tokio::spawn(
		second_sender.send_all(futures::stream::iter(backward.clone().into_iter().map(Ok))),
	);
	let (received_forward, received_backward) = tokio::time::timeout(
		Duration::from_secs(20),
		futures::future::join(drain(second_receiver), drain(first_receiver)),
	)
	.await
	.unwrap();
	assert_eq!(received_forward, forward);
	assert_eq!(received_backward, backward);
	finished(first_sender).await;
	finished(second_sender).await;
}

#[tokio::test]
async fn the_sender_finishes_once_the_stream_is_polled_to_its_end() {
	let messenger = TestMessenger::default();
	let ((sender, first_receiver), (second_sender, mut receiver)) = pair(&messenger).await;
	let input = futures::stream::iter(items(2, 4).into_iter().map(Ok::<_, Bytes>));
	let sender = tokio::spawn(sender.send_all(input));
	let second_sender = tokio::spawn(second_sender.finish());
	let first_receiver = tokio::spawn(drain(first_receiver));
	receiver.next().await.unwrap().unwrap();
	receiver.next().await.unwrap().unwrap();
	tokio::time::sleep(options().timeout * 2).await;
	assert!(!sender.is_finished());
	assert!(receiver.next().await.is_none());
	finished(sender).await;
	finished(second_sender).await;
	first_receiver.await.unwrap();
}

#[tokio::test]
async fn an_unpolled_receiver_holds_back_the_window_and_keeps_the_sender_alive() {
	let messenger = TestMessenger::default();
	let ((sender, first_receiver), (second_sender, mut receiver)) = pair(&messenger).await;
	let input = futures::stream::iter(items(20, 40).into_iter().map(Ok::<_, Bytes>));
	let sender = tokio::spawn(sender.send_all(input));
	let second_sender = tokio::spawn(second_sender.finish());
	let first_receiver = tokio::spawn(drain(first_receiver));

	// Poll one item and then stop, so that the window fills, and stall well past the peer timeout.
	let mut received = vec![receiver.next().await.unwrap().unwrap()];
	tokio::time::sleep(options().peer_timeout() + options().timeout * 10).await;
	assert!(!sender.is_finished());

	// Polling again lets the rest through.
	received.extend(drain(receiver).await);
	assert_eq!(received.len(), 20);
	finished(sender).await;
	finished(second_sender).await;
	first_receiver.await.unwrap();
}

#[tokio::test]
async fn abort_delivers_its_payload_with_and_without_loss() {
	let cases = [
		(true, None),
		(false, Some(3)),
		(true, Some(3)),
		(false, Some(4)),
		(true, Some(4)),
		(false, Some(5)),
		(true, Some(5)),
	];
	for (item, drop_every) in cases {
		let messenger = TestMessenger::default();
		messenger.state.lock().unwrap().drop_every = drop_every;
		let ((sender, first_receiver), (second_sender, mut receiver)) = pair(&messenger).await;
		let second_sender = tokio::spawn(second_sender.finish());
		let first_receiver = tokio::spawn(drain(first_receiver));
		let mut input = Vec::new();
		if item {
			input.push(Ok(Bytes::from_static(b"a")));
		}
		input.push(Err(Bytes::from_static(b"failed")));
		let sender = tokio::spawn(sender.send_all(futures::stream::iter(input)));
		if item {
			let item = receiver.next().await.unwrap().unwrap();
			assert_eq!(item, Bytes::from_static(b"a"));
		}
		let Some(Err(Error::Aborted(payload))) = receiver.next().await else {
			panic!("expected an abort");
		};
		assert_eq!(payload, Bytes::from_static(b"failed"));
		finished(sender).await;
		drop(receiver);
		finished(second_sender).await;
		first_receiver.await.unwrap();
	}
}

#[tokio::test]
async fn an_idle_sender_keeps_the_receiver_alive() {
	let messenger = TestMessenger::default();
	let ((mut sender, first_receiver), (second_sender, mut receiver)) = pair(&messenger).await;
	let second_sender = tokio::spawn(second_sender.finish());
	let first_receiver = tokio::spawn(drain(first_receiver));

	// Stay idle well past the peer timeout before sending anything.
	tokio::time::sleep(options().peer_timeout() + options().timeout * 10).await;
	sender.send(Bytes::from_static(b"a")).await.unwrap();
	let sender = tokio::spawn(sender.finish());
	let item = receiver.next().await.unwrap().unwrap();
	assert_eq!(item, Bytes::from_static(b"a"));
	assert!(receiver.next().await.is_none());
	finished(sender).await;
	finished(second_sender).await;
	first_receiver.await.unwrap();
}

#[tokio::test]
async fn dropping_a_sender_resets_the_connection() {
	let messenger = TestMessenger::default();
	let ((sender, _first_receiver), (second_sender, mut receiver)) = pair(&messenger).await;
	drop(sender);
	let result = tokio::time::timeout(Duration::from_secs(10), receiver.next())
		.await
		.unwrap();
	assert!(matches!(result, Some(Err(Error::Reset))));
	let result = tokio::time::timeout(Duration::from_secs(10), second_sender.finish())
		.await
		.unwrap();
	assert!(matches!(result, Err(Error::Reset)));
}

#[tokio::test]
async fn dropping_a_receiver_resets_the_connection_before_the_end_and_acknowledges_after_it() {
	// Drop a receiver before the end, which resets the sender.
	let messenger = TestMessenger::default();
	let ((mut sender, _first_receiver), (_second_sender, receiver)) = pair(&messenger).await;
	drop(receiver);
	let error = tokio::time::timeout(Duration::from_secs(10), async {
		loop {
			if let Err(error) = sender.send(Bytes::from_static(b"a")).await {
				break error;
			}
		}
	})
	.await
	.unwrap();
	assert!(matches!(error, Error::Reset));

	// Drop a receiver with unpolled items after the end arrives, which lets the sender finish.
	let messenger = TestMessenger::default();
	let ((sender, first_receiver), (second_sender, mut receiver)) = pair(&messenger).await;
	let input = futures::stream::iter(items(3, 4).into_iter().map(Ok::<_, Bytes>));
	let sender = tokio::spawn(sender.send_all(input));
	let second_sender = tokio::spawn(second_sender.finish());
	let first_receiver = tokio::spawn(drain(first_receiver));
	receiver.next().await.unwrap().unwrap();
	tokio::time::sleep(options().timeout * 2).await;
	drop(receiver);
	finished(sender).await;
	finished(second_sender).await;
	first_receiver.await.unwrap();
}

#[tokio::test]
async fn both_halves_time_out_without_a_peer() {
	let messenger = TestMessenger::default();
	let (sender, mut receiver) =
		open::<Bytes, Bytes, _>(&messenger, "relay.a", "relay.b", options())
			.await
			.unwrap();
	let input = futures::stream::iter([Ok::<_, Bytes>(Bytes::from_static(b"a"))]);
	let (sent, received) = tokio::time::timeout(
		Duration::from_secs(10),
		futures::future::join(sender.send_all(input), receiver.next()),
	)
	.await
	.unwrap();
	assert!(matches!(sent, Err(Error::Timeout)));
	assert!(matches!(received, Some(Err(Error::Timeout))));
}

#[tokio::test]
async fn invalid_options_are_rejected() {
	let invalid = [
		Options {
			timeout: Duration::ZERO,
			..options()
		},
		Options {
			window: 0,
			..options()
		},
		Options {
			ack_threshold: options().window + 1,
			..options()
		},
		Options {
			ack_delay: options().timeout,
			..options()
		},
	];
	for options in invalid {
		assert!(matches!(options.validate(), Err(Error::Options(_))));
		let result =
			open::<Bytes, Bytes, _>(&TestMessenger::default(), "relay.a", "relay.b", options).await;
		assert!(matches!(result, Err(Error::Options(_))));
	}
	options().validate().unwrap();
}

#[test]
fn messages_roundtrip() {
	let messages = [
		Message::Abort {
			payload: Bytes::from_static(b"error"),
			sequence: 6,
		},
		Message::Ack {
			received: 4,
			sequence: 3,
			window: 1024,
		},
		Message::Data {
			payload: Bytes::from_static(b"data"),
			sequence: 4,
		},
		Message::End { sequence: 5 },
		Message::Probe,
		Message::Reset,
	];
	for message in messages {
		let bytes = message.serialize().unwrap();
		assert_eq!(Message::deserialize(bytes).unwrap(), message);
	}
}
