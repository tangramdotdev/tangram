use crate::Error;
use futures::{FutureExt as _, future::BoxFuture};

pub struct Acker {
	ack: Option<BoxFuture<'static, Result<(), Error>>>,
	acked: bool,
	retry: Option<BoxFuture<'static, ()>>,
}

impl Acker {
	pub fn new(
		ack: impl Future<Output = Result<(), Error>> + Send + 'static,
		retry: impl Future<Output = ()> + Send + 'static,
	) -> Self {
		Self {
			ack: Some(ack.boxed()),
			acked: false,
			retry: Some(retry.boxed()),
		}
	}

	pub async fn ack(mut self) -> Result<(), Error> {
		if let Some(fut) = self.ack.take() {
			fut.await?;
		}
		self.acked = true;
		Ok(())
	}
}

impl Default for Acker {
	fn default() -> Self {
		Self {
			ack: None,
			acked: true,
			retry: None,
		}
	}
}

impl Drop for Acker {
	fn drop(&mut self) {
		drop(self.ack.take());
		if let (false, Some(retry)) = (self.acked, self.retry.take()) {
			tokio::spawn(retry);
		}
	}
}
