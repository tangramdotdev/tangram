use {
	crate::{SandboxPermit, Server},
	futures::{FutureExt as _, TryFutureExt as _, future},
	std::time::Duration,
	tangram_client::prelude::*,
};

mod process;
mod progress;
mod sandbox;

#[derive(Clone, Debug)]
pub struct Output {
	pub checksum: Option<tg::Checksum>,
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub value: Option<tg::Value>,
}

type ProcessTaskMap =
	tangram_futures::task::Map<tg::process::Id, tg::Result<()>, (), tg::id::BuildHasher>;

impl Server {
	pub(crate) async fn runner_task(&self) {
		loop {
			let permit = self
				.sandbox_semaphore
				.clone()
				.acquire_owned()
				.await
				.unwrap();
			let permit = SandboxPermit(tg::Either::Left(permit));

			let location = tg::location::Location::Local(tg::location::Local::default());
			let arg = tg::sandbox::queue::Arg {
				location: Some(location.clone()),
			};
			let futures = std::iter::once(
				self.dequeue_sandbox(arg)
					.map_ok(move |output| (output, location.clone()))
					.boxed(),
			)
			.chain(self.config.runner.iter().flat_map(|config| {
				config.remotes.iter().map(|name| {
					let server = self.clone();
					let location = tg::location::Location::Remote(tg::location::Remote {
						remote: name.to_owned(),
						regions: None,
					});
					async move {
						let arg = tg::sandbox::queue::Arg {
							location: Some(location.clone()),
						};
						let output = server.dequeue_sandbox(arg).await?;
						Ok::<_, tg::Error>((output, location))
					}
					.boxed()
				})
			}));

			let (output, location) = match future::select_ok(futures).await {
				Ok((output, _)) => output,
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to dequeue a sandbox");
					tokio::time::sleep(Duration::from_secs(1)).await;
					continue;
				},
			};

			self.spawn_sandbox_task(&output.sandbox, location, permit, output.process);
		}
	}
}
