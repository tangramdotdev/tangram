use crate::{BuildPermit, Server};
use either::Either;
use futures::FutureExt;
use tangram_client as tg;

impl Server {
	pub(crate) async fn run_build_queue(&self) {
		loop {
			// Wait for a permit.
			let permit = self.build_semaphore.clone().acquire_owned().await.unwrap();
			let permit = BuildPermit(Either::Left(permit));

			// Create an iterator over all remotes with build: true and the local queue.
			let queues = self
				.options
				.remotes
				.iter()
				.filter(|remote| remote.build)
				.map(|remote| {
					remote
						.client
						.try_dequeue_build(tg::build::dequeue::Arg::default())
						.boxed()
				})
				.chain(std::iter::once(
					self.try_dequeue_build(tg::build::dequeue::Arg::default())
						.boxed(),
				));

			// Try and dequeue a build locally or from one of the remotes.
			let output = match futures::future::select_all(queues).await {
				(Ok(Some(output)), _, _) => output,
				(Ok(None), _, _) => continue,
				(Err(error), _, _) => {
					tracing::error!(?error, "failed to dequeue a build");
					tokio::time::sleep(std::time::Duration::from_secs(1)).await;
					continue;
				},
			};

			let build = tg::Build::with_id(output.build);

			// Start the build.
			self.try_start_build_internal(build.clone(), permit)
				.await
				.ok();

			// Create the build's heartbeat.
			self.try_start_heartbeat_for_build(&build)
				.await
				.inspect_err(
					|error| tracing::error!(%error, "failed to start the build's heartbeat"),
				)
				.ok();
		}
	}
}
