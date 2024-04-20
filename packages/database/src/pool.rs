pub struct Pool<T> {
	sender: async_channel::Sender<T>,
	receiver: async_channel::Receiver<T>,
}

pub struct Guard<T> {
	value: Option<T>,
	sender: async_channel::Sender<T>,
}

impl<T> Pool<T> {
	#[must_use]
	pub fn new() -> Self {
		let (sender, receiver) = async_channel::unbounded();
		Self { sender, receiver }
	}

	pub fn add(&self, value: T) {
		self.sender.try_send(value).unwrap();
	}

	pub async fn get(&self) -> Guard<T> {
		let value = self.receiver.recv().await.unwrap();
		let sender = self.sender.clone();
		Guard {
			value: Some(value),
			sender,
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
		self.sender.try_send(value).ok();
	}
}
