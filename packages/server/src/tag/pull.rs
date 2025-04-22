use crate::Server;
use futures::{TryStreamExt as _, stream::FuturesUnordered};
use tangram_client as tg;
use tangram_either::Either;

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
				let directory = output.item.right()?.try_unwrap_directory().ok()?;
				let server = self.clone();
				let remote = remote.clone().unwrap_or_else(|| "default".to_owned());
				Some(async move {
					let arg = tg::pull::Arg {
						items: vec![Either::Right(directory.into())],
						remote: Some(remote),
						..Default::default()
					};
					let stream = server.pull(arg).await?;
					let mut stream = std::pin::pin!(stream);
					while stream.try_next().await?.is_some() {}
					Ok::<_, tg::Error>(())
				})
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await
	}
}
