use {super::*, futures::FutureExt as _, tokio_stream::wrappers::ReceiverStream};

#[derive(Clone, Debug, Eq, PartialEq)]
enum Message {
	Ack(String),
	Notification,
	Request { id: String, position: u64 },
	Response { id: String, length: u64 },
}

#[tokio::test]
async fn receipt_does_not_complete_and_reconnect_replays() {
	let (input, stream) = tokio::sync::mpsc::channel(8);
	let (output, mut received) = tokio::sync::mpsc::channel(8);
	let stream = ReceiverStream::new(stream).boxed();
	let mut options = stream_options();
	options.retry.backoff = Duration::from_millis(5);
	options.retry.max_delay = Duration::from_millis(5);
	let mut control = Stream::new_reconnecting(stream, output, options);
	let sender = control.sender();
	let request = Message::Request {
		id: "write".into(),
		position: 42,
	};
	let mut response = sender
		.request(request.clone(), Priority::Low)
		.await
		.unwrap();
	assert_eq!(received.recv().await, Some(request.clone()));
	input
		.send(Ok(tg::control::Event::Message(Message::Ack(
			"write".into(),
		))))
		.await
		.unwrap();
	control.recv_without_ack().await.unwrap();
	assert!(sender.outbox.contains_key("write"));
	assert!(
		tokio::time::timeout(Duration::from_millis(20), received.recv())
			.await
			.is_err()
	);
	assert!((&mut response).now_or_never().is_none());

	input.send(Ok(tg::control::Event::Reconnect)).await.unwrap();
	input
		.send(Ok(tg::control::Event::Message(Message::Notification)))
		.await
		.unwrap();
	assert_eq!(
		control.recv_with_ack().await.unwrap(),
		Some(Message::Notification)
	);
	assert_eq!(
		tokio::time::timeout(Duration::from_secs(1), received.recv())
			.await
			.unwrap(),
		Some(request)
	);

	let message = Message::Response {
		id: "write".into(),
		length: 7,
	};
	input
		.send(Ok(tg::control::Event::Message(message.clone())))
		.await
		.unwrap();
	input
		.send(Ok(tg::control::Event::Message(Message::Notification)))
		.await
		.unwrap();
	control.recv_with_ack().await.unwrap();
	assert_eq!(response.await.unwrap(), message);
	assert!(sender.outbox.is_empty());
	assert_eq!(received.recv().await, Some(Message::Ack("write".into())));
}

#[tokio::test]
async fn response_retries_until_receipt() {
	let (input, stream) = tokio::sync::mpsc::channel(8);
	let (output, _received) = tokio::sync::mpsc::channel(8);
	let mut control = Stream::new(
		ReceiverStream::new(stream).boxed(),
		output,
		stream_options(),
	);
	let sender = control.sender();
	let response = Message::Response {
		id: "write".into(),
		length: 7,
	};
	sender.send(response.clone()).await.unwrap();
	assert_eq!(sender.messages(Priority::High), vec![response]);
	input.send(Ok(Message::Ack("write".into()))).await.unwrap();
	control.recv_without_ack().await.unwrap();
	assert!(sender.outbox.is_empty());
}

#[tokio::test]
async fn cancellation_releases_pending_requests() {
	let (output, _received) = tokio::sync::mpsc::channel(8);
	let control = Stream::<Message, Message>::new(
		futures::stream::pending().boxed(),
		output,
		stream_options(),
	);
	let sender = control.sender();
	let request = Message::Request {
		id: "write".into(),
		position: 42,
	};
	let response = sender.request(request, Priority::Low).await.unwrap();
	drop(response);
	assert!(sender.outbox.is_empty());
	assert!(sender.responses.is_empty());
	sender.wait_for_empty().await;
}

#[tokio::test]
async fn stream_drop_ends_pending_responses() {
	let (output, _received) = tokio::sync::mpsc::channel(8);
	let control = Stream::<Message, Message>::new(
		futures::stream::pending().boxed(),
		output,
		stream_options(),
	);
	let sender = control.sender();
	let request = Message::Request {
		id: "write".into(),
		position: 42,
	};
	let response = sender.request(request, Priority::Low).await.unwrap();
	drop(control);
	assert!(response.await.is_err());
	assert!(sender.outbox.is_empty());
}

#[tokio::test]
async fn priority_stream() {
	let (sender_high, receiver_high) = tokio::sync::mpsc::channel(2);
	let (sender_low, receiver_low) = tokio::sync::mpsc::channel(2);
	sender_low.send(1).await.unwrap();
	sender_low.send(2).await.unwrap();
	sender_high.send(3).await.unwrap();
	drop(sender_high);
	drop(sender_low);
	let output = super::priority_stream(receiver_high, receiver_low)
		.collect::<Vec<_>>()
		.await;
	assert_eq!(output, vec![3, 1, 2]);
}

impl Input<Self> for Message {
	fn kind(&self) -> InputKind<'_> {
		match self {
			Self::Ack(id) => InputKind::Ack { id },
			Self::Notification => InputKind::Message { id: None },
			Self::Request { id, .. } => InputKind::Message { id: Some(id) },
			Self::Response { id, .. } => InputKind::Response { id },
		}
	}

	fn create_ack_message(id: String) -> Self {
		Self::Ack(id)
	}
}

impl Output for Message {
	fn id(&self) -> Option<&str> {
		match self {
			Self::Ack(_) | Self::Notification => None,
			Self::Request { id, .. } | Self::Response { id, .. } => Some(id),
		}
	}

	fn is_request(&self) -> bool {
		matches!(self, Self::Request { .. })
	}
}
