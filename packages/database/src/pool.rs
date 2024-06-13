use std::{collections::BinaryHeap, sync::Arc};

pub struct Pool<T> {
	pending: Arc<std::sync::Mutex<BinaryHeap<Pending<T>>>>,
	sender: async_channel::Sender<T>,
	receiver: async_channel::Receiver<T>,
}

struct Pending<T> {
	priority: Priority,
	send: tokio::sync::oneshot::Sender<T>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
	Low,
	High,
}

pub struct Guard<T> {
	value: Option<T>,
	sender: async_channel::Sender<T>,
	pending: Arc<std::sync::Mutex<BinaryHeap<Pending<T>>>>,
}

impl<T> Pool<T> {
	#[must_use]
	pub fn new() -> Self {
		let pending = Arc::new(std::sync::Mutex::new(BinaryHeap::new()));
		let (sender, receiver) = async_channel::unbounded();
		Self {
			pending,
			sender,
			receiver,
		}
	}

	pub fn add(&self, value: T) {
		self.sender.try_send(value).unwrap();
	}

	pub async fn get(&self, priority: Priority) -> Guard<T> {
		// First, try and receive from the channel.
		if let Ok(value) = self.receiver.try_recv() {
			return Guard {
				value: Some(value),
				sender: self.sender.clone(),
				pending: self.pending.clone(),
			};
		}

		// Otherwise create a pending object with priority.
		let (send, recv) = tokio::sync::oneshot::channel();
		let pending = Pending { priority, send };
		self.pending.lock().unwrap().push(pending);
		let value = recv.await.unwrap();
		Guard {
			value: Some(value),
			sender: self.sender.clone(),
			pending: self.pending.clone(),
		}
	}
}

impl<T> Default for Pool<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T> std::ops::Deref for Guard<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		self.value.as_ref().unwrap()
	}
}

impl<T> std::ops::DerefMut for Guard<T> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.value.as_mut().unwrap()
	}
}

impl<T> AsRef<T> for Guard<T> {
	fn as_ref(&self) -> &T {
		self
	}
}

impl<T> AsMut<T> for Guard<T> {
	fn as_mut(&mut self) -> &mut T {
		self
	}
}

impl<T> Drop for Guard<T> {
	fn drop(&mut self) {
		let value = self.value.take().unwrap();
		if let Some(pending) = self.pending.lock().unwrap().pop() {
			// First, try and wake the highest priority item.
			if let Err(value) = pending.send.send(value) {
				// If the receiver dropped, make sure to emplace to the back of the queue so panics don't starve connections.
				self.sender.try_send(value).ok();
			}
		} else {
			// Otherwise send along the channel.
			self.sender.try_send(value).ok();
		}
	}
}

impl<T> PartialEq for Pending<T> {
	fn eq(&self, other: &Self) -> bool {
		self.priority == other.priority
	}
}

impl<T> Eq for Pending<T> {}

impl<T> PartialOrd for Pending<T> {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.priority.cmp(&other.priority))
	}
}

impl<T> Ord for Pending<T> {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.partial_cmp(other).unwrap()
	}
}
