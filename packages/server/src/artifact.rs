use crate::Server;
use futures::{FutureExt as _, StreamExt as _, future};
use indoc::indoc;
use rusqlite::{self as sqlite, fallible_streaming_iterator::FallibleStreamingIterator};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::stream::TryExt as _;

impl Server {
	pub(crate) async fn pull_incompete(
		&self,
		artifact: &tg::artifact::Id,
		progress: &crate::progress::Handle<tg::checkout::Output>,
	) -> tg::Result<()> {
		// Get the incomplete objects.
		let index = self
			.index
			.as_ref()
			.unwrap_left()
			.create_connection(true)
			.map_err(|source| tg::error!(!source, "failed to create an index connection"))?;
		let items = tokio::task::spawn_blocking({
			let server = self.clone();
			let items = vec![artifact.clone().into()];
			move || server.get_incomplete_objects_sync(items, &index)
		})
		.await
		.unwrap()?;

		// Create a future to pull the objects.
		let pull_future = {
			let progress = progress.clone();
			let server = self.clone();
			async move {
				if !items.is_empty() {
					let items = items.into_iter().map(Either::Right).collect();
					let stream = server
						.pull(tg::pull::Arg {
							items,
							remote: Some("default".to_owned()),
							..tg::pull::Arg::default()
						})
						.await?;
					progress.spinner("pull", "pull");
					let mut stream = std::pin::pin!(stream);
					while let Some(event) = stream.next().await {
						progress.forward(event);
					}
				}
				Ok::<_, tg::Error>(())
			}
		}
		.boxed();

		// Create a future to index then check if the objects are complete.
		let index_future = {
			let artifact = artifact.clone();
			let server = self.clone();
			async move {
				let stream = server.index().await?;
				let stream = std::pin::pin!(stream);
				stream.try_last().await?;
				let metadata = server
					.try_get_object_complete_metadata_local(&artifact.clone().into())
					.await?
					.ok_or_else(|| tg::error!(%artifact, "expected an object"))?;
				if !metadata.complete {
					return Err(tg::error!("expected the object to be complete"));
				}
				Ok::<_, tg::Error>(())
			}
		}
		.boxed();

		// Select the pull and index futures.
		future::select_ok([pull_future, index_future]).await?;

		progress.finish_all();

		Ok(())
	}

	pub fn get_incomplete_objects_sync(
		&self,
		items: Vec<tg::object::Id>,
		index: &sqlite::Connection,
	) -> tg::Result<Vec<tg::object::Id>> {
		// Prepare the complete statement.
		let statement = indoc!(
			"
				select complete
				from objects
				where id = ?1;
			",
		);
		let mut statement = index
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let mut objects = Vec::new();
		let mut stack: Vec<tg::object::Id> = items;
		while let Some(object) = stack.pop() {
			// Attempt to get the data or the cache reference. If the object is not stored, then add it to the objects and continue.
			#[allow(clippy::match_wildcard_for_single_variants)]
			let data = match &self.store {
				crate::store::Store::Lmdb(lmdb) => lmdb.try_get_object_data_sync(&object)?,
				crate::store::Store::Memory(memory) => memory.try_get_object_data(&object)?,
				_ => {
					return Err(tg::error!("invalid store"));
				},
			};
			let cache_reference = if data.is_none() && object.is_leaf() {
				#[allow(clippy::match_wildcard_for_single_variants)]
				match &self.store {
					crate::store::Store::Lmdb(lmdb) => {
						lmdb.try_get_cache_reference_sync(&object)?
					},
					crate::store::Store::Memory(memory) => memory.try_get_cache_reference(&object),
					_ => {
						return Err(tg::error!("invalid store"));
					},
				}
			} else {
				None
			};
			if data.is_none() && cache_reference.is_none() {
				objects.push(object);
				continue;
			}

			// Get the complete flag.
			let params = sqlite::params![object.to_string()];
			let mut rows = statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to perform the query"))?;
			rows.advance()
				.map_err(|source| tg::error!(!source, "failed to perform the query"))?;
			let row = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to perform the query"))?;
			let complete = row.map(|row| row.get_unwrap::<_, u64>(0) != 0);

			// If the object is not complete, then add its children to the stack.
			if !complete.is_some_and(|complete| complete) {
				if let Some(data) = data {
					stack.extend(data.children());
				}
			}
		}

		Ok(objects)
	}
}
