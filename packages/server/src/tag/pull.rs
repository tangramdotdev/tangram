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
			.match_(tg::match_::Arg {
				cached: false,
				groups: false,
				length: None,
				location,
				organizations: false,
				pattern: pattern.clone(),
				reverse: false,
				tags: true,
				ttl: tg::remote::cache::Ttl::default(),
				users: false,
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to list entries"))?
			.data;
		list.into_iter()
			.filter_map(|entry| {
				let tg::list::Entry::Tag {
					location, target, ..
				} = entry
				else {
					return None;
				};
				let tg::Referent { node, options } = target;
				let directory = node.left()?.try_unwrap_directory().ok()?;
				let session = self.clone();
				let location = options.location.clone().or(location)?;
				Some(async move {
					let arg = tg::pull::Arg {
						source: Some(location),
						nodes: vec![tg::Referent::new(directory.into(), options)],
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
