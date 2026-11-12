use std::{
	collections::{HashMap, VecDeque},
	sync::Mutex,
	time::Duration,
};

#[cfg(test)]
mod tests;

/// The replies that a component recently sent to requests, by request ID, so that a duplicate of a request is answered without acting on it again. The memory keeps each reply for a ttl after it was last used, and at most a capacity of replies. A request that is neither acknowledged nor answered within the pending ttl is acted on again, because the component did not take it up.
pub struct Replies<R> {
	capacity: usize,
	pending_time_to_live: Duration,
	state: Mutex<State<R>>,
}

/// What a component does with a request, given the replies that it remembers.
#[derive(Debug, Eq, PartialEq)]
pub enum Begin<R> {
	/// The request is new, so act on it.
	New,

	/// The request is being acted on, so do not act on it again, because its acknowledgment and reply will reach the request's sender.
	Pending,

	/// The request was answered with this reply, so send it again.
	Replied(R),
}

struct State<R> {
	entries: HashMap<String, Entry<R>>,
	order: VecDeque<(tokio::time::Instant, String)>,
	time_to_live: Duration,
}

struct Entry<R> {
	acknowledged: bool,
	reply: Option<R>,
	started_at: tokio::time::Instant,
	used_at: tokio::time::Instant,
}

impl<R: Clone> Replies<R> {
	#[must_use]
	pub fn new(capacity: usize, time_to_live: Duration, pending_time_to_live: Duration) -> Self {
		let state = State {
			entries: HashMap::new(),
			order: VecDeque::new(),
			time_to_live,
		};
		Self {
			capacity,
			pending_time_to_live,
			state: Mutex::new(state),
		}
	}

	/// Record that the component acknowledged a request, so that a duplicate of it waits for its reply however long that takes.
	pub fn acknowledge(&self, id: &str) {
		let mut state = self.state.lock().unwrap();
		if let Some(entry) = state.entries.get_mut(id) {
			entry.acknowledged = true;
		}
	}

	/// Begin a request, and return whether it is new, is being acted on, or was answered.
	pub fn begin(&self, id: &str) -> Begin<R> {
		let now = tokio::time::Instant::now();
		let mut state = self.state.lock().unwrap();
		self.prune(&mut state, now);
		let pending_time_to_live = self.pending_time_to_live;
		let begin = if let Some(entry) = state.entries.get_mut(id) {
			entry.used_at = now;
			match &entry.reply {
				Some(reply) => Begin::Replied(reply.clone()),
				None if !entry.acknowledged
					&& now.saturating_duration_since(entry.started_at) >= pending_time_to_live =>
				{
					entry.started_at = now;
					Begin::New
				},
				None => Begin::Pending,
			}
		} else {
			let entry = Entry {
				acknowledged: false,
				reply: None,
				started_at: now,
				used_at: now,
			};
			state.entries.insert(id.to_owned(), entry);
			Begin::New
		};
		state.order.push_back((now, id.to_owned()));

		begin
	}

	/// Remember the reply to a request, so that a duplicate of it is answered with the reply.
	pub fn finish(&self, id: &str, reply: R) {
		let now = tokio::time::Instant::now();
		let mut state = self.state.lock().unwrap();
		let entry = Entry {
			acknowledged: true,
			reply: Some(reply),
			started_at: now,
			used_at: now,
		};
		state.entries.insert(id.to_owned(), entry);
		state.order.push_back((now, id.to_owned()));
		self.prune(&mut state, now);
	}

	/// Forget a request, so that the next copy of it is acted on again, such as after a reply that asks the sender to send the request again.
	pub fn forget(&self, id: &str) {
		let mut state = self.state.lock().unwrap();
		state.entries.remove(id);
	}

	/// Set the ttl of the replies.
	pub fn set_time_to_live(&self, time_to_live: Duration) {
		let mut state = self.state.lock().unwrap();
		state.time_to_live = time_to_live;
	}

	/// Forget the replies that were not used within the ttl, and the least recently used replies beyond the capacity.
	fn prune(&self, state: &mut State<R>, now: tokio::time::Instant) {
		while let Some((used_at, id)) = state.order.front() {
			let current = state
				.entries
				.get(id)
				.is_some_and(|entry| entry.used_at == *used_at);
			let expired = now.saturating_duration_since(*used_at) >= state.time_to_live;
			let full = state.entries.len() > self.capacity;
			if current && !expired && !full {
				break;
			}
			let (used_at, id) = state.order.pop_front().unwrap();
			if state
				.entries
				.get(&id)
				.is_some_and(|entry| entry.used_at == used_at)
			{
				state.entries.remove(&id);
			}
		}
	}
}
