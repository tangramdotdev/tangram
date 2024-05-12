use crate::{BuildPermit, Server};
use either::Either;
use futures::{future, FutureExt as _, TryFutureExt as _};
use tangram_client as tg;
use tg::Handle as _;

impl Server {
	pub(crate) async fn build_queue_task(&self) {
		loop {
			// Wait for a permit.
			let permit = self.build_semaphore.clone().acquire_owned().await.unwrap();
			let permit = BuildPermit(Either::Left(permit));

			// Try to dequeue a build locally or from one of the remotes.
			let arg = tg::build::dequeue::Arg::default();
			let futures = std::iter::once(
				self.dequeue_build(arg)
					.map_ok(|output| (output, None))
					.boxed(),
			)
			.chain(
				self.options
					.remotes
					.iter()
					.filter(|remote| remote.build)
					.map(|remote| {
						let arg = tg::build::dequeue::Arg::default();
						remote
							.client
							.dequeue_build(arg)
							.map_ok(|output| (output, Some(remote.client.clone())))
							.boxed()
					}),
			);
			let (output, remote) = match future::select_ok(futures).await {
				Ok(((output, remote), _)) => (output, remote),
				Err(error) => {
					tracing::error!(?error, "failed to dequeue a build");
					tokio::time::sleep(std::time::Duration::from_secs(1)).await;
					continue;
				},
			};

			// Spawn the build.
			let build = tg::Build::with_id(output.build);
			self.spawn_build(build.clone(), permit, remote).await.ok();
		}
	}
}
