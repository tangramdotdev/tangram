use crate::Server;
use futures::{FutureExt, StreamExt as _, future};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::stream::TryExt as _;

pub(crate) mod cache;
pub(crate) mod checkin;
pub(crate) mod checkout;

impl Server {
	pub(crate) async fn ensure_artifact_is_complete(
		&self,
		artifact: &tg::artifact::Id,
		progress: &crate::progress::Handle<tg::artifact::checkout::Output>,
	) -> tg::Result<()> {
		let pull = {
			let artifact = artifact.clone();
			let progress = progress.clone();
			let server = self.clone();
			async move {
				let stream = server
					.pull(tg::pull::Arg {
						items: vec![Either::Right(artifact.into())],
						recursive: true,
						..tg::pull::Arg::default()
					})
					.await?;
				progress.spinner("pull", "pulling...");
				let mut stream = std::pin::pin!(stream);
				while let Some(event) = stream.next().await {
					progress.forward(event);
				}
				Ok::<_, tg::Error>(())
			}
		}
		.boxed();
		let index = {
			let artifact = artifact.clone();
			let server = self.clone();
			async move {
				let stream = server.index().await?;
				let stream = std::pin::pin!(stream);
				stream.try_last().await?;
				let metadata = server
					.try_get_object_complete_metadata_local(&artifact.into())
					.await?
					.ok_or_else(|| tg::error!("expected an object"))?;
				if !metadata.complete {
					return Err(tg::error!("expected the object to be complete"));
				}
				Ok::<_, tg::Error>(())
			}
		}
		.boxed();
		future::select_ok([pull, index]).await?;
		progress.finish_all();
		Ok(())
	}
}
