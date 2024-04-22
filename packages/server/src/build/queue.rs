use crate::{Permit, Server};
use either::Either;
use tangram_client as tg;

impl Server {
	pub(crate) async fn run_build_queue(&self) {
		loop {
			// Wait for a permit.
			let permit = self.build_semaphore.clone().acquire_owned().await.unwrap();
			let permit = Permit(Either::Left(permit));

			// Dequeue a build.
			let output = match self
				.try_dequeue_build(tg::build::DequeueArg::default(), None)
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
			let build = tg::Build::with_id(output.id);

			// Start the build.
			self.try_start_build_internal(build, permit).await.ok();
		}
	}
}
