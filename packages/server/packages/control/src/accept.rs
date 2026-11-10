use {futures::future, std::pin::pin, tangram_client::prelude::*};

#[cfg(test)]
mod tests;

/// The acceptance of a request that is both written to a queue and handled, which succeeds when either succeeds and fails only when both fail.
#[derive(Debug, Default)]
pub struct Accept {
	decided: bool,
	handled: Option<tg::Result<()>>,
	queued: Option<tg::Result<()>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Path {
	Handled,
	Queued,
}

/// Accept a request when either its queue write or its handling succeeds, and fail only when both fail.
pub async fn accept(
	queued: impl Future<Output = tg::Result<()>>,
	handled: impl Future<Output = tg::Result<()>>,
) -> tg::Result<()> {
	let queued = pin!(queued);
	let handled = pin!(handled);
	let mut accept = Accept::default();
	let output = match future::select(queued, handled).await {
		future::Either::Left((result, handled)) => {
			if let Some(output) = accept.finish(Path::Queued, result) {
				return output;
			}
			accept.finish(Path::Handled, handled.await)
		},
		future::Either::Right((result, queued)) => {
			if let Some(output) = accept.finish(Path::Handled, result) {
				return output;
			}
			accept.finish(Path::Queued, queued.await)
		},
	};
	output.unwrap_or_else(|| Err(tg::error!("expected the acceptance to be decided")))
}

impl Accept {
	#[must_use]
	pub fn decided(&self) -> bool {
		self.decided
	}

	/// Record the result of a path, and get the result of the acceptance the first time that it is decided.
	pub fn finish(&mut self, path: Path, result: tg::Result<()>) -> Option<tg::Result<()>> {
		match path {
			Path::Handled => self.handled = Some(result),
			Path::Queued => self.queued = Some(result),
		}
		if self.decided {
			return None;
		}
		let output = match (&self.queued, &self.handled) {
			(Some(Ok(())), _) | (_, Some(Ok(()))) => Ok(()),
			(Some(Err(error)), Some(Err(_))) => Err(error.clone()),
			_ => return None,
		};
		self.decided = true;
		Some(output)
	}
}
