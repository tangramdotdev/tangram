use std::sync::Arc;

/// An object pool backed by a bounded mpsc channel.
pub struct Pool<T> {
	// The channel's sender.
	sender: tokio::sync::mpsc::Sender<T>,

	// The channel's receiver, wrapped in Arc<Mutex<_>> to provide mpmc semantics.
	receiver: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<T>>>,
}

pub struct Guard<'a, T> {
	object: Option<T>,
	pool: &'a Pool<T>, // The borrow here forbids the guard from outliving the pool.
}

impl<T> Pool<T> {
	/// Initialize the pool with a vector of objects.
	#[must_use]
	pub fn new(objects: Vec<T>) -> Self {
		let capacity = objects.len();
		let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
		for object in objects {
			sender.try_send(object).unwrap();
		}
		let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
		Self { sender, receiver }
	}

	pub async fn get(&self) -> Guard<T> {
		// We rely on the fact that tokio::sync::mutex is fair to allow concurrent callers to get()to make progress. It is safe to call .unwrap() because the channel cannot be closed until self is dropped.
		let object = self.receiver.lock().await.recv().await.unwrap();
		Guard {
			object: Some(object),
			pool: self,
		}
	}
}

impl<'a, T> Guard<'a, T> {
	pub fn replace(&mut self, object: T) -> T {
		self.object.replace(object).unwrap()
	}
}

impl<'a, T> std::ops::Deref for Guard<'a, T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		self.object.as_ref().unwrap()
	}
}

impl<'a, T> std::ops::DerefMut for Guard<'a, T> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.object.as_mut().unwrap()
	}
}

impl<'a, T> Drop for Guard<'a, T> {
	fn drop(&mut self) {
		if let Some(object) = self.object.take() {
			// This unwrap is safe, since it's not possible for the underlying channel to be full until the object wrapped by this guard is dropped.
			self.pool.sender.try_send(object).unwrap();
		}
	}
}
