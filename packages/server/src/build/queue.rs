use crate::{Permit, Server};
use either::Either;
use tangram_client as tg;

impl Server {
	pub(crate) async fn run_build_queue(&self) -> tg::Result<()> {
		loop {
			// Wait for a permit.
			let Ok(permit) = self.build_semaphore.clone().acquire_owned().await else {
				return Ok(());
			};
			let permit = Permit(Either::Left(permit));

			// Dequeue a build.
			let Some(output) = self
				.try_dequeue_build(tg::build::DequeueArg::default(), None)
				.await?
			else {
				continue;
			};
			let build = tg::Build::with_id(output.id);

			// Start the build.
			self.start_build(build, permit).await?;
		}
	}
}
