use {
	super::Indexer, futures::StreamExt as _, tangram_client::prelude::*, tangram_store::Store as _,
};

mod archive;
mod cache;
mod index;

pub(crate) use {archive::object_archive_outbox_subject, index::object_index_outbox_subject};

impl Indexer {
	async fn wait_for_object_put(
		&self,
		retry: &crate::config::Retry,
		id: &tg::object::Id,
		put: [u8; 16],
	) -> tg::Result<bool> {
		// An outbox entry can become visible before its concurrent object put completes.
		let options = retry.clone().into();
		let attempts = tangram_futures::retry::stream(options);
		futures::pin_mut!(attempts);
		while attempts.next().await.is_some() {
			let arg = crate::store::object::get::Arg {
				id: id.clone(),
				put: None,
			};
			let output = self.server.store.try_get_object(arg).await.map_err(
				|error| tg::error!(!error, %id, "failed to get an object from the store"),
			)?;
			if output.object.is_some_and(|object| object.put >= put) {
				return Ok(true);
			}
		}

		Ok(false)
	}

	async fn try_wait_for_object_put(
		&self,
		retry: &crate::config::Retry,
		id: &tg::object::Id,
		put: [u8; 16],
	) -> tg::Result<Option<crate::store::object::Object<'static>>> {
		// An outbox entry can become visible before its concurrent object put completes.
		let options = retry.clone().into();
		let attempts = tangram_futures::retry::stream(options);
		futures::pin_mut!(attempts);
		while attempts.next().await.is_some() {
			let arg = crate::store::object::get::Arg {
				id: id.clone(),
				put: Some(put),
			};
			let output = self.server.store.try_get_object(arg).await.map_err(
				|error| tg::error!(!error, %id, "failed to get an object from the store"),
			)?;
			if output.object.is_some() {
				return Ok(output.object);
			}
		}

		Ok(None)
	}
}
