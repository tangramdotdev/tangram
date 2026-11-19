use {
	super::Indexer, futures::StreamExt as _, std::collections::BTreeSet, tangram_client::prelude::*,
};

mod cache;
#[cfg(test)]
mod tests;

/// The state of an object put in the cache.
#[derive(Clone)]
pub(super) enum ObjectPut {
	/// The cache does not have the put yet.
	Absent,

	/// The cache has the put.
	Present(tangram_server_core::cache::object::Object<'static>),

	/// The cache has a newer put of the object, which replaced the put.
	Replaced,
}

impl Indexer {
	pub(super) async fn wait_for_object_put_batch(
		&self,
		retry: &tangram_server_core::config::Retry,
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
			let arg = tangram_server_core::cache::object::get::batch::Arg {
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

	pub(super) async fn try_wait_for_object_put_batch(
		&self,
		retry: &tangram_server_core::config::Retry,
		puts: &[(tg::object::Id, [u8; 16])],
	) -> tg::Result<Vec<ObjectPut>> {
		try_wait_for_object_puts(retry, puts, |ids| async move {
			let arg = tangram_server_core::cache::object::get::batch::Arg { bytes: true, ids };
			self.server
				.cache
				.try_get_object_batch(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the objects from the cache"))
		})
		.await
	}
}

async fn try_wait_for_object_puts<F, Fut>(
	retry: &tangram_server_core::config::Retry,
	puts: &[(tg::object::Id, [u8; 16])],
	get: F,
) -> tg::Result<Vec<ObjectPut>>
where
	F: Fn(Vec<tg::object::Id>) -> Fut,
	Fut: Future<Output = tg::Result<Vec<tangram_server_core::cache::object::get::Output>>>,
{
	// Read the puts until the cache has each one or a newer put that replaced it, because a queue item can become visible before its concurrent object puts complete.
	let mut objects = vec![ObjectPut::Absent; puts.len()];
	let mut missing = (0..puts.len()).collect::<Vec<_>>();
	let options = retry.clone().into();
	let attempts = tangram_futures::retry::stream(options);
	futures::pin_mut!(attempts);
	while !missing.is_empty() && attempts.next().await.is_some() {
		let ids = missing.iter().map(|index| puts[*index].0.clone()).collect();
		let outputs = get(ids).await?;
		if outputs.len() != missing.len() {
			return Err(tg::error!("unexpected object get batch length"));
		}
		missing = missing
			.into_iter()
			.zip(outputs)
			.filter_map(|(index, output)| match output.object {
				Some(object) if object.put == puts[index].1 => {
					objects[index] = ObjectPut::Present(object);
					None
				},
				Some(object) if object.put > puts[index].1 => {
					objects[index] = ObjectPut::Replaced;
					None
				},
				_ => Some(index),
			})
			.collect();
	}

	Ok(objects)
}
