use crate::{BuildPermit, Server};
use either::Either;
use tangram_client as tg;

impl Server {
	pub(crate) async fn run_build_queue(&self) {
		loop {
			// Wait for a permit.
			let permit = self.build_semaphore.clone().acquire_owned().await.unwrap();
			let permit = BuildPermit(Either::Left(permit));

			// Dequeue a build.
			let output = match self
				.try_dequeue_build(tg::build::dequeue::Arg::default())
				.await
			{
				Ok(Some(output)) => output,
				Ok(None) => continue,
				Err(error) => {
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
