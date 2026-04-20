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
		location: Option<tg::location::Arg>,
	) -> tg::Result<()> {
		let list = self
			.list_tags(tg::tag::list::Arg {
				cached: false,
				length: None,
				location,
				pattern,
				recursive: false,
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
				let location = output.location?;
				Some(async move {
					let arg = tg::pull::Arg {
						source: Some(location),
						items: vec![tg::Either::Left(directory.into())],
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
