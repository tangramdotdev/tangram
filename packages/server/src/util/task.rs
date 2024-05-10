use futures::TryFutureExt as _;

#[derive(Clone)]
pub struct Stop(tokio::sync::watch::Sender<bool>);

impl Stop {
	pub fn new() -> Self {
		let (sender, _) = tokio::sync::watch::channel(false);
		Self(sender)
	}

	pub fn stop(&self) {
		self.0.send_replace(true);
	}

	pub fn is_stopped(&self) -> bool {
		*self.0.subscribe().borrow()
	}

	pub async fn stopped(&self) {
		self.0
			.subscribe()
			.wait_for(|stop| *stop)
			.map_ok(|_| ())
			.await
			.unwrap();
	}
}
