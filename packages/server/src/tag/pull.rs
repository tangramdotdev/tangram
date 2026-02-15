use {
	crate::Server,
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	std::pin::pin,
	tangram_client::prelude::*,
};

impl Server {
	pub(crate) async fn pull_tag(
		&self,
		pattern: tg::tag::Pattern,
		remote: Option<String>,
	) -> tg::Result<()> {
		let list = self
			.list_tags(tg::tag::list::Arg {
				cached: false,
				length: None,
				local: None,
				pattern,
				recursive: false,
				remotes: remote.clone().map(|r| vec![r]),
				reverse: false,
				ttl: None,
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to list the tags"))?
			.data;
		list.into_iter()
			.filter_map(|output| {
				let directory = output.item?.left()?.try_unwrap_directory().ok()?;
				let server = self.clone();
				let remote = remote.clone().unwrap_or_else(|| "default".to_owned());
				Some(async move {
					let arg = tg::pull::Arg {
						items: vec![tg::Either::Left(directory.into())],
						remote: Some(remote),
						..Default::default()
					};
					let stream = server.pull(arg).await?;
					let mut stream = pin!(stream);
					while stream.try_next().await?.is_some() {}
					Ok::<_, tg::Error>(())
				})
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await
	}
}
