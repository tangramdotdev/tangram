use {
	crate::Payload,
	bytes::Bytes,
	futures::StreamExt as _,
	std::sync::{Arc, Mutex},
	tokio::sync::mpsc,
};

/// A messenger that delivers within one process and can drop, duplicate, and reorder messages.
#[derive(Clone, Default)]
pub(crate) struct TestMessenger {
	state: Arc<Mutex<TestState>>,
}

#[derive(Default)]
struct TestState {
	count: u64,
	drop_every: Option<u64>,
	duplicate_every: Option<u64>,
	held: Option<(String, Bytes)>,
	reorder_every: Option<u64>,
	subscribers: Vec<(String, mpsc::UnboundedSender<(String, Bytes)>)>,
}

impl TestMessenger {
	pub(crate) fn with_drop_every(drop_every: Option<u64>) -> Self {
		let state = TestState {
			drop_every,
			..Default::default()
		};
		Self {
			state: Arc::new(Mutex::new(state)),
		}
	}

	pub(crate) fn with_faults(drop_every: u64, duplicate_every: u64, reorder_every: u64) -> Self {
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

	/// Count the live subscriptions whose subjects start with a prefix.
	pub(crate) fn subscriptions(&self, prefix: &str) -> usize {
		let mut state = self.state.lock().unwrap();
		state.subscribers.retain(|(_, sender)| !sender.is_closed());
		state
			.subscribers
			.iter()
			.filter(|(name, _)| name.starts_with(prefix))
			.count()
	}

	fn deliver(state: &mut TestState, subject: &str, bytes: &Bytes) {
		state.subscribers.retain(|(_, sender)| !sender.is_closed());
		for (name, sender) in &state.subscribers {
			if matches(name, subject) {
				sender.send((subject.to_owned(), bytes.clone())).ok();
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
			.push((subject, sender));
		let stream =
			futures::stream::poll_fn(move |cx| receiver.poll_recv(cx)).map(|(subject, bytes)| {
				let payload = T::deserialize(bytes)?;
				let message = crate::Message { subject, payload };
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

/// Check whether a subscription's subject, whose tokens may be the token wildcard, matches a published subject.
fn matches(subscription: &str, subject: &str) -> bool {
	let subscription = subscription.split('.').collect::<Vec<_>>();
	let subject = subject.split('.').collect::<Vec<_>>();
	subscription.len() == subject.len()
		&& subscription
			.iter()
			.zip(&subject)
			.all(|(pattern, token)| *pattern == "*" || pattern == token)
}
