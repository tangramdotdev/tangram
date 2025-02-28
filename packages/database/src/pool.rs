use futures::{FutureExt as _, future};
use std::{
	collections::BinaryHeap,
	ops::{Deref, DerefMut},
	sync::{Arc, Mutex},
};

pub struct Pool<T> {
	state: Arc<Mutex<State<T>>>,
}

struct State<T> {
	requests: BinaryHeap<Request<T>>,
	order: usize,
	values: Vec<T>,
}

struct Request<T> {
	priority: Priority,
	order: usize,
	sender: tokio::sync::oneshot::Sender<Guard<T>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub enum Priority {
	Low,
	#[default]
	Medium,
	High,
}

pub struct Guard<T> {
	value: Option<T>,
	pool: Pool<T>,
}

impl<T> Pool<T> {
	#[must_use]
	pub fn new() -> Self {
		let state = State {
			requests: BinaryHeap::new(),
			order: 0,
			values: Vec::new(),
		};
		let state = Arc::new(Mutex::new(state));
		Self { state }
	}

	pub fn add(&self, value: T) {
		let mut state = self.state.lock().unwrap();
		let mut guard = Guard {
			value: Some(value),
			pool: self.clone(),
		};
		while let Some(request) = state.requests.pop() {
			match request.sender.send(guard) {
				Ok(()) => return,
				Err(guard_) => {
					guard = guard_;
				},
			}
		}
		state.values.push(guard.value.take().unwrap());
	}

	pub fn get(&self, priority: Priority) -> impl Future<Output = Guard<T>> {
		let mut state = self.state.lock().unwrap();
		if let Some(value) = state.values.pop() {
			return future::ready(Guard {
				value: Some(value),
				pool: self.clone(),
			})
			.left_future();
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let order = state.order;
		let request = Request {
			priority,
			order,
			sender,
		};
		state.order += 1;
		state.requests.push(request);
		receiver.map(move |result| result.unwrap()).right_future()
	}

	#[must_use]
	pub fn available(&self) -> usize {
		self.state.lock().unwrap().values.len()
	}
}

impl<T> Default for Pool<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T> Clone for Pool<T> {
	fn clone(&self) -> Self {
		Self {
			state: self.state.clone(),
		}
	}
}

impl<T> Deref for Guard<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		self.value.as_ref().unwrap()
	}
}

impl<T> DerefMut for Guard<T> {
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
		if let Some(value) = self.value.take() {
			self.pool.add(value);
		}
	}
}

impl<T> PartialEq for Request<T> {
	fn eq(&self, other: &Self) -> bool {
		self.priority == other.priority && self.order == other.order
	}
}

impl<T> Eq for Request<T> {}

impl<T> PartialOrd for Request<T> {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl<T> Ord for Request<T> {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.priority
			.cmp(&other.priority)
			.then_with(|| self.order.cmp(&other.order).reverse())
	}
}
