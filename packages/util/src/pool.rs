use std::sync::Arc;

pub struct Pool<T> {
	semaphore: Arc<tokio::sync::Semaphore>,
	objects: Arc<crossbeam::queue::ArrayQueue<T>>,
}

pub struct Guard<T> {
	permit: Option<tokio::sync::OwnedSemaphorePermit>,
	objects: Arc<crossbeam::queue::ArrayQueue<T>>,
	object: Option<T>,
}

impl<T> Pool<T>
where
	T: Send + 'static,
{
	#[allow(clippy::new_without_default)]
	#[must_use]
	pub fn new(objects: Vec<T>) -> Self {
		let semaphore = Arc::new(tokio::sync::Semaphore::new(objects.len()));
		let queue = crossbeam::queue::ArrayQueue::new(objects.len());
		for object in objects {
			queue.push(object).ok().unwrap();
		}
		let objects = Arc::new(queue);
		Self { semaphore, objects }
	}

	pub async fn get(&self) -> Guard<T> {
		let permit = self.semaphore.clone().acquire_owned().await.unwrap();
		let object = self.objects.pop().unwrap();
		Guard {
			permit: Some(permit),
			object: Some(object),
			objects: self.objects.clone(),
		}
	}
}

impl<T> Guard<T> {
	pub fn replace(&mut self, object: T) -> T {
		self.object.replace(object).unwrap()
	}

	pub fn take(mut self) -> T {
		self.permit.take().unwrap().forget();
		self.object.take().unwrap()
	}
}

impl<T> std::ops::Deref for Guard<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		self.object.as_ref().unwrap()
	}
}

impl<T> std::ops::DerefMut for Guard<T> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.object.as_mut().unwrap()
	}
}

impl<T> Drop for Guard<T> {
	fn drop(&mut self) {
		if let Some(object) = self.object.take() {
			self.objects.push(object).ok().unwrap();
		}
	}
}
