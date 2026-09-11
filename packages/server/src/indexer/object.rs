use {
	super::Indexer, futures::StreamExt as _, std::collections::BTreeSet, tangram_cache::Cache as _,
	tangram_client::prelude::*,
};

mod cache;

impl Indexer {
	pub(super) async fn wait_for_object_put_batch(
		&self,
		retry: &crate::config::Retry,
		mut missing: BTreeSet<(tg::object::Id, [u8; 16])>,
	) -> tg::Result<BTreeSet<(tg::object::Id, [u8; 16])>> {
		// A queue entry can become visible before its concurrent object put completes.
		let options = retry.clone().into();
		let attempts = tangram_futures::retry::stream(options);
		futures::pin_mut!(attempts);
		while attempts.next().await.is_some() {
			if missing.is_empty() {
				break;
			}
			let arg = crate::cache::object::get::batch::Arg {
				bytes: false,
				ids: missing.iter().map(|(id, _)| id.clone()).collect(),
			};
			let outputs = self
				.server
				.cache
				.try_get_object_batch(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the objects from the cache"))?;
			if outputs.len() != missing.len() {
				return Err(tg::error!("unexpected object get batch length"));
			}
			missing = missing
				.into_iter()
				.zip(outputs)
				.filter_map(|((id, put), output)| {
					output
						.object
						.is_none_or(|object| object.put < put)
						.then_some((id, put))
				})
				.collect();
			if missing.is_empty() {
				break;
			}
		}

		Ok(missing)
	}

	pub(super) async fn try_wait_for_object_put(
		&self,
		retry: &crate::config::Retry,
		id: &tg::object::Id,
		put: [u8; 16],
	) -> tg::Result<Option<crate::cache::object::Object<'static>>> {
		// A queue entry can become visible before its concurrent object put completes.
		let options = retry.clone().into();
		let attempts = tangram_futures::retry::stream(options);
		futures::pin_mut!(attempts);
		while attempts.next().await.is_some() {
			let arg = crate::cache::object::get::Arg {
				bytes: true,
				id: id.clone(),
				put: Some(put),
			};
			let output = self.server.cache.try_get_object(arg).await.map_err(
				|error| tg::error!(!error, %id, "failed to get an object from the cache"),
			)?;
			if output.object.is_some() {
				return Ok(output.object);
			}
		}

		Ok(None)
	}
}
