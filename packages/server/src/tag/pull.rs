use {
	crate::Session,
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	std::pin::pin,
	tangram_client::prelude::*,
};

impl Session {
	pub(crate) async fn pull_tag(
		&self,
		pattern: tg::specifier::Pattern,
		location: Option<tg::location::Arg>,
	) -> tg::Result<()> {
		let list = self
			.list(tg::list::Arg {
				cached: false,
				length: None,
				location,
				groups: false,
				pattern: pattern.clone(),
				recursive: false,
				reverse: false,
				tags: true,
				ttl: None,
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to list entries"))?
			.data;
		list.into_iter()
			.filter_map(|entry| {
				let tg::list::Entry::Tag { item, location, .. } = entry else {
					return None;
				};
				let directory = item.left()?.try_unwrap_directory().ok()?;
				let session = self.clone();
				let location = location?;
				Some(async move {
					let arg = tg::pull::Arg {
						source: Some(location),
						items: vec![tg::Referent::with_item(tg::Either::Left(directory.into()))],
						..Default::default()
					};
					let stream = session.pull(arg).await?;
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
