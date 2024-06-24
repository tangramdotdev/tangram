use std::{
	collections::BinaryHeap,
	sync::{Arc, Mutex},
};

pub struct Pool<T> {
	state: Arc<Mutex<State<T>>>,
}

struct State<T> {
	requests: BinaryHeap<Request<T>>,
	values: Vec<T>,
}

struct Request<T> {
	priority: Priority,
	sender: tokio::sync::oneshot::Sender<T>,
}

#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub enum Priority {
	Low,
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
			values: Vec::new(),
		};
		let state = Arc::new(Mutex::new(state));
		Self { state }
	}

	pub fn add(&self, value: T) {
		let mut state = self.state.lock().unwrap();
		if let Some(request) = state.requests.pop() {
			let result = request.sender.send(value);
			if let Err(value) = result {
				state.values.push(value);
			}
		} else {
			state.values.push(value);
		}
	}

	pub async fn get(&self, priority: Priority) -> Guard<T> {
		let receiver = {
			let mut state = self.state.lock().unwrap();
			if let Some(value) = state.values.pop() {
				return Guard {
					value: Some(value),
					pool: self.clone(),
				};
			}
			let (sender, receiver) = tokio::sync::oneshot::channel();
			let request = Request { priority, sender };
			state.requests.push(request);
			receiver
		};
		let value = receiver.await.unwrap();
		Guard {
			value: Some(value),
			pool: self.clone(),
		}
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
		let mut state = self.pool.state.lock().unwrap();
		if let Some(request) = state.requests.pop() {
			let result = request.sender.send(value);
			if let Err(value) = result {
				state.values.push(value);
			}
		} else {
			state.values.push(value);
		}
	}
}

impl<T> PartialEq for Request<T> {
	fn eq(&self, other: &Self) -> bool {
		self.priority == other.priority
	}
}

impl<T> Eq for Request<T> {}

impl<T> PartialOrd for Request<T> {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.priority.cmp(&other.priority))
	}
}

impl<T> Ord for Request<T> {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.partial_cmp(other).unwrap()
	}
}
