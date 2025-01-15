use crate::Server;
use futures::{stream::FuturesUnordered, TryStreamExt};
use tangram_client as tg;

impl Server {
	pub(crate) async fn pull_tag(
		&self,
		pattern: tg::tag::Pattern,
		remote: Option<String>,
	) -> tg::Result<()> {
		let list = self
			.list_tags(tg::tag::list::Arg {
				length: None,
				remote: remote.clone(),
				pattern,
				reverse: false,
			})
			.await?
			.data;
		list.into_iter()
			.filter_map(|output| {
				// Skip objects that can't be packages.
				let directory = output.item.right()?.try_unwrap_directory().ok()?;
				let server = self.clone();
				let remote = remote.clone().unwrap_or_else(|| "default".to_owned());
				let future = async move {
					// Pull the object
					let arg = tg::object::pull::Arg { remote };
					let stream = server.pull_object(&directory.into(), arg).await?;

					// Drain the stream.
					let mut stream = std::pin::pin!(stream);
					while let Some(_event) = stream.try_next().await? {}
					Ok::<_, tg::Error>(())
				};
				Some(future)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await
	}
}
