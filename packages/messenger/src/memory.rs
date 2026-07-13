use {
	crate::{Error, Message, Payload},
	bytes::Bytes,
	futures::Stream,
	std::{
		collections::HashMap,
		marker::PhantomData,
		pin::Pin,
		sync::{
			Arc, RwLock, Weak,
			atomic::{AtomicU64, Ordering},
		},
		task::{Context, Poll, ready},
	},
	tokio::sync::mpsc::{Receiver, Sender},
};

const SUBSCRIPTION_CAPACITY: usize = 65_536;

#[derive(Clone, Default)]
pub struct Messenger {
	state: Arc<State>,
}

#[derive(Default)]
struct State {
	next_subscription_id: AtomicU64,
	subscriptions: RwLock<SubscriptionTrie>,
}

pub struct Subscription<T> {
	receiver: Receiver<RawMessage>,
	_guard: SubscriptionGuard,
	marker: PhantomData<fn() -> T>,
}

struct SubscriptionGuard {
	queue_group: Option<String>,
	state: Weak<State>,
	subject: String,
	subscription_id: u64,
}

#[derive(Default)]
struct SubscriptionTrie {
	root: SubscriptionNode,
}

#[derive(Default)]
struct SubscriptionNode {
	full_wildcard: Subscriptions,
	literals: HashMap<String, SubscriptionNode>,
	terminal: Subscriptions,
	token_wildcard: Option<Box<SubscriptionNode>>,
}

#[derive(Default)]
struct Subscriptions {
	plain: HashMap<u64, Sender<RawMessage>>,
	queue_groups: HashMap<String, HashMap<u64, Sender<RawMessage>>>,
}

#[derive(Clone)]
struct RawMessage {
	payload: Bytes,
	subject: Arc<str>,
}

#[derive(Default)]
struct Matches {
	plain: Vec<Sender<RawMessage>>,
	queue_groups: HashMap<String, Vec<Sender<RawMessage>>>,
}

impl Messenger {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	fn subscribe_inner<T>(
		&self,
		subject: String,
		queue_group: Option<String>,
	) -> Result<Subscription<T>, Error>
	where
		T: Payload,
	{
		let tokens = subscription_tokens(&subject)?;
		if let Some(queue_group) = queue_group.as_deref() {
			validate_queue_group(queue_group)?;
		}
		let subscription_id = self
			.state
			.next_subscription_id
			.fetch_add(1, Ordering::Relaxed);
		let (sender, receiver) = tokio::sync::mpsc::channel(SUBSCRIPTION_CAPACITY);
		self.state.subscriptions.write().unwrap().insert(
			&tokens,
			subscription_id,
			queue_group.as_deref(),
			sender,
		);
		let guard = SubscriptionGuard {
			queue_group,
			state: Arc::downgrade(&self.state),
			subject,
			subscription_id,
		};
		Ok(Subscription {
			receiver,
			_guard: guard,
			marker: PhantomData,
		})
	}
}

impl<T> Stream for Subscription<T>
where
	T: Payload,
{
	type Item = Result<Message<T>, Error>;

	fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let this = self.get_mut();
		let Some(message) = ready!(this.receiver.poll_recv(context)) else {
			return Poll::Ready(None);
		};
		let subject = message.subject.to_string();
		let message = T::deserialize(message.payload)
			.map(|payload| Message { subject, payload })
			.map_err(Error::deserialization);
		Poll::Ready(Some(message))
	}
}

impl Drop for SubscriptionGuard {
	fn drop(&mut self) {
		let Some(state) = self.state.upgrade() else {
			return;
		};
		let Ok(tokens) = subscription_tokens(&self.subject) else {
			return;
		};
		let Ok(mut subscriptions) = state.subscriptions.write() else {
			return;
		};
		subscriptions.remove(&tokens, self.subscription_id, self.queue_group.as_deref());
	}
}

impl SubscriptionTrie {
	fn insert(
		&mut self,
		tokens: &[&str],
		subscription_id: u64,
		queue_group: Option<&str>,
		sender: Sender<RawMessage>,
	) {
		let mut node = &mut self.root;
		for token in tokens {
			match *token {
				">" => {
					node.full_wildcard
						.insert(subscription_id, queue_group, sender);
					return;
				},
				"*" => {
					node = node.token_wildcard.get_or_insert_with(Default::default);
				},
				literal => {
					node = node.literals.entry(literal.to_owned()).or_default();
				},
			}
		}
		node.terminal.insert(subscription_id, queue_group, sender);
	}

	fn matches(&self, tokens: &[&str]) -> Matches {
		let mut matches = Matches::default();
		self.root.collect_matches(tokens, &mut matches);
		matches
	}

	fn remove(&mut self, tokens: &[&str], subscription_id: u64, queue_group: Option<&str>) {
		self.root.remove(tokens, subscription_id, queue_group);
	}
}

impl SubscriptionNode {
	fn collect_matches(&self, tokens: &[&str], matches: &mut Matches) {
		let Some((token, remaining)) = tokens.split_first() else {
			matches.extend(&self.terminal);
			return;
		};
		matches.extend(&self.full_wildcard);
		if let Some(node) = self.literals.get(*token) {
			node.collect_matches(remaining, matches);
		}
		if let Some(node) = &self.token_wildcard {
			node.collect_matches(remaining, matches);
		}
	}

	fn is_empty(&self) -> bool {
		self.full_wildcard.is_empty()
			&& self.literals.is_empty()
			&& self.terminal.is_empty()
			&& self.token_wildcard.is_none()
	}

	fn remove(&mut self, tokens: &[&str], subscription_id: u64, queue_group: Option<&str>) {
		let Some((token, remaining)) = tokens.split_first() else {
			self.terminal.remove(subscription_id, queue_group);
			return;
		};
		match *token {
			">" => self.full_wildcard.remove(subscription_id, queue_group),
			"*" => {
				if let Some(node) = &mut self.token_wildcard {
					node.remove(remaining, subscription_id, queue_group);
					if node.is_empty() {
						self.token_wildcard = None;
					}
				}
			},
			literal => {
				let remove = self.literals.get_mut(literal).is_some_and(|node| {
					node.remove(remaining, subscription_id, queue_group);
					node.is_empty()
				});
				if remove {
					self.literals.remove(literal);
				}
			},
		}
	}
}

impl Subscriptions {
	fn insert(
		&mut self,
		subscription_id: u64,
		queue_group: Option<&str>,
		sender: Sender<RawMessage>,
	) {
		if let Some(queue_group) = queue_group {
			self.queue_groups
				.entry(queue_group.to_owned())
				.or_default()
				.insert(subscription_id, sender);
		} else {
			self.plain.insert(subscription_id, sender);
		}
	}

	fn is_empty(&self) -> bool {
		self.plain.is_empty() && self.queue_groups.is_empty()
	}

	fn remove(&mut self, subscription_id: u64, queue_group: Option<&str>) {
		if let Some(queue_group) = queue_group {
			let remove = self.queue_groups.get_mut(queue_group).is_some_and(|group| {
				group.remove(&subscription_id);
				group.is_empty()
			});
			if remove {
				self.queue_groups.remove(queue_group);
			}
		} else {
			self.plain.remove(&subscription_id);
		}
	}
}

impl Matches {
	fn extend(&mut self, subscriptions: &Subscriptions) {
		self.plain.extend(subscriptions.plain.values().cloned());
		for (queue_group, subscriptions) in &subscriptions.queue_groups {
			self.queue_groups
				.entry(queue_group.clone())
				.or_default()
				.extend(subscriptions.values().cloned());
		}
	}
}

impl crate::Messenger for Messenger {
	async fn publish<T>(&self, subject: String, payload: T) -> Result<(), Error>
	where
		T: Payload,
	{
		let tokens = publish_tokens(&subject)?;
		let payload = payload.serialize()?;
		let matches = self.state.subscriptions.read().unwrap().matches(&tokens);
		let message = RawMessage {
			payload,
			subject: subject.into(),
		};
		for sender in matches.plain {
			sender.try_send(message.clone()).ok();
		}
		for senders in matches.queue_groups.into_values() {
			let index = rand::random_range(0..senders.len());
			senders[index].try_send(message.clone()).ok();
		}
		Ok(())
	}

	async fn subscribe<T>(
		&self,
		subject: String,
	) -> Result<impl Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload,
	{
		self.subscribe_inner(subject, None)
	}

	async fn queue_subscribe<T>(
		&self,
		subject: String,
		queue_group: String,
	) -> Result<impl Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload,
	{
		self.subscribe_inner(subject, Some(queue_group))
	}
}

fn publish_tokens(subject: &str) -> Result<Vec<&str>, Error> {
	let tokens = subject_tokens(subject)?;
	if tokens.iter().any(|token| token.contains(['*', '>'])) {
		return Err(invalid_input(
			"wildcard tokens are not allowed in published subjects",
		));
	}
	Ok(tokens)
}

fn subscription_tokens(subject: &str) -> Result<Vec<&str>, Error> {
	let tokens = subject_tokens(subject)?;
	for (index, token) in tokens.iter().enumerate() {
		if token.contains('*') && *token != "*" {
			return Err(invalid_input("the token wildcard must occupy a full token"));
		}
		if token.contains('>') && (*token != ">" || index + 1 != tokens.len()) {
			return Err(invalid_input(
				"the full wildcard must occupy the final token",
			));
		}
	}
	Ok(tokens)
}

fn subject_tokens(subject: &str) -> Result<Vec<&str>, Error> {
	if subject.is_empty() {
		return Err(invalid_input("the subject must not be empty"));
	}
	if subject.chars().any(char::is_whitespace) || subject.contains('\0') {
		return Err(invalid_input(
			"the subject must not contain whitespace or null characters",
		));
	}
	let tokens = subject.split('.').collect::<Vec<_>>();
	if tokens.iter().any(|token| token.is_empty()) {
		return Err(invalid_input("the subject must not contain empty tokens"));
	}
	Ok(tokens)
}

fn validate_queue_group(queue_group: &str) -> Result<(), Error> {
	if queue_group.is_empty() {
		return Err(invalid_input("the queue group must not be empty"));
	}
	if queue_group.chars().any(char::is_whitespace) || queue_group.contains('\0') {
		return Err(invalid_input(
			"the queue group must not contain whitespace or null characters",
		));
	}
	Ok(())
}

fn invalid_input(message: &'static str) -> Error {
	Error::other(std::io::Error::new(
		std::io::ErrorKind::InvalidInput,
		message,
	))
}

#[cfg(test)]
mod tests {
	use {super::Messenger, crate::Messenger as _, bytes::Bytes, futures::StreamExt as _};

	#[tokio::test]
	async fn exact_subscription() {
		let messenger = Messenger::new();
		let mut subscription = messenger
			.subscribe::<Bytes>("foo.bar".into())
			.await
			.unwrap();
		messenger
			.publish("foo.bar".into(), Bytes::from_static(b"payload"))
			.await
			.unwrap();
		let message = subscription.next().await.unwrap().unwrap();
		assert_eq!(message.subject, "foo.bar");
		assert_eq!(message.payload, Bytes::from_static(b"payload"));
	}

	#[tokio::test]
	async fn wildcard_subscriptions() {
		let messenger = Messenger::new();
		let mut token = messenger.subscribe::<()>("foo.*.baz".into()).await.unwrap();
		let mut full = messenger.subscribe::<()>("foo.>".into()).await.unwrap();
		messenger.publish("foo.bar.baz".into(), ()).await.unwrap();
		assert_eq!(token.next().await.unwrap().unwrap().subject, "foo.bar.baz");
		assert_eq!(full.next().await.unwrap().unwrap().subject, "foo.bar.baz");
	}

	#[tokio::test]
	async fn full_wildcard_matches_one_or_more_tokens() {
		let messenger = Messenger::new();
		let mut subscription = messenger
			.subscribe_inner::<()>("foo.>".into(), None)
			.unwrap();
		messenger.publish("foo".into(), ()).await.unwrap();
		assert!(subscription.receiver.try_recv().is_err());
		messenger.publish("foo.bar".into(), ()).await.unwrap();
		assert!(subscription.receiver.try_recv().is_ok());
	}

	#[tokio::test]
	async fn plain_subscriptions_fan_out() {
		let messenger = Messenger::new();
		let mut first = messenger
			.subscribe_inner::<()>("foo.*".into(), None)
			.unwrap();
		let mut second = messenger
			.subscribe_inner::<()>("foo.bar".into(), None)
			.unwrap();
		messenger.publish("foo.bar".into(), ()).await.unwrap();
		assert!(first.receiver.try_recv().is_ok());
		assert!(second.receiver.try_recv().is_ok());
	}

	#[tokio::test]
	async fn queue_group_delivers_to_one_subscription() {
		let messenger = Messenger::new();
		let mut first = messenger
			.subscribe_inner::<()>("foo.*".into(), Some("workers".into()))
			.unwrap();
		let mut second = messenger
			.subscribe_inner::<()>("foo.bar".into(), Some("workers".into()))
			.unwrap();
		messenger.publish("foo.bar".into(), ()).await.unwrap();
		let received = usize::from(first.receiver.try_recv().is_ok())
			+ usize::from(second.receiver.try_recv().is_ok());
		assert_eq!(received, 1);
	}

	#[tokio::test]
	async fn queue_groups_each_receive_once() {
		let messenger = Messenger::new();
		let mut first = messenger
			.subscribe_inner::<()>("foo".into(), Some("first".into()))
			.unwrap();
		let mut second = messenger
			.subscribe_inner::<()>("foo".into(), Some("second".into()))
			.unwrap();
		messenger.publish("foo".into(), ()).await.unwrap();
		assert!(first.receiver.try_recv().is_ok());
		assert!(second.receiver.try_recv().is_ok());
	}

	#[tokio::test]
	async fn dropped_subscriptions_are_removed() {
		let messenger = Messenger::new();
		let subscription = messenger.subscribe::<()>("foo.*.>".into()).await.unwrap();
		drop(subscription);
		assert!(
			messenger
				.state
				.subscriptions
				.read()
				.unwrap()
				.root
				.is_empty()
		);
	}

	#[tokio::test]
	async fn invalid_subjects_are_rejected() {
		let messenger = Messenger::new();
		for subject in ["", ".foo", "foo.", "foo..bar", "foo bar"] {
			assert!(messenger.subscribe::<()>(subject.into()).await.is_err());
			assert!(messenger.publish(subject.into(), ()).await.is_err());
		}
		for subject in ["foo*", "foo.>.bar", "foo.>.*"] {
			assert!(messenger.subscribe::<()>(subject.into()).await.is_err());
		}
		for subject in ["foo.*", "foo.>"] {
			assert!(messenger.publish(subject.into(), ()).await.is_err());
		}
		assert!(
			messenger
				.queue_subscribe::<()>("foo".into(), String::new())
				.await
				.is_err()
		);
	}
}
