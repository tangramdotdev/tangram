use crate::Error;
use futures::{FutureExt as _, future::BoxFuture};

pub struct Acker {
	ack: Option<BoxFuture<'static, Result<(), Error>>>,
}

impl Acker {
	pub fn new(ack: impl Future<Output = Result<(), Error>> + Send + 'static) -> Self {
		Self {
			ack: Some(ack.boxed()),
		}
	}

	pub async fn ack(mut self) -> Result<(), Error> {
		if let Some(fut) = self.ack.take() {
			fut.await?;
		}
		Ok(())
	}
}

impl Default for Acker {
	fn default() -> Self {
		Self { ack: None }
	}
}

impl Drop for Acker {
	fn drop(&mut self) {
		drop(self.ack.take());
	}
}
