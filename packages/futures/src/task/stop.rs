use futures::prelude::*;

#[derive(Clone)]
pub struct Stop(tokio::sync::watch::Sender<bool>);

impl Stop {
	#[must_use]
	pub fn new() -> Self {
		let (sender, _) = tokio::sync::watch::channel(false);
		Self(sender)
	}

	pub fn stop(&self) {
		self.0.send_replace(true);
	}

	#[must_use]
	pub fn stopped(&self) -> bool {
		*self.0.subscribe().borrow()
	}

	pub fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
		let sender = self.0.clone();
		async move {
			sender
				.subscribe()
				.wait_for(|stop| *stop)
				.map_ok(|_| ())
				.await
				.unwrap();
		}
	}
}

impl Default for Stop {
	fn default() -> Self {
		Self::new()
	}
}
