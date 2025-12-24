use {
	crate::{Context, Server, database::Database},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future,
		stream::{self, FuturesUnordered},
	},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

impl Server {
	pub async fn try_get_process_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> tg::Result<Option<tg::process::get::Output>> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(output) = self
				.try_get_process_local(id)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
		{
			return Ok(Some(output));
		}

		// Try remotes.
		let remotes = self
			.remotes(arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(output) = self.try_get_process_remote(id, &remotes).await.map_err(
			|source| tg::error!(!source, %id, "failed to get the process from the remote"),
		)? {
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_process_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::get::Output>> {
		self.try_get_process_batch(std::slice::from_ref(id))
			.await
			.map(|outputs| outputs.into_iter().next().unwrap())
	}

	pub async fn try_get_process_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		let outputs = self
			.try_get_process_batch_local(ids)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the processes locally"))?;
		let remotes = self
			.remotes(None)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		let outputs = std::iter::zip(ids, outputs)
			.map(|(id, output)| {
				let remotes = remotes.clone();
				async move {
					if let Some(output) = output {
						return Ok(Some(output));
					}
					let output = self.try_get_process_remote(id, &remotes).await?;
					Ok::<_, tg::Error>(output)
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		Ok(outputs)
	}

	pub async fn try_get_process_batch_local(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => self.try_get_process_batch_postgres(database, ids).await,
			Database::Sqlite(database) => self.try_get_process_batch_sqlite(database, ids).await,
		}
	}

	async fn try_get_process_remote(
		&self,
		id: &tg::process::Id,
		remotes: &[String],
	) -> tg::Result<Option<tg::process::get::Output>> {
		// Attempt to get the process from the remotes.
		if remotes.is_empty() {
			return Ok(None);
		}
		let futures = remotes.iter().map(|remote| {
			let remote = remote.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the remote client"),
				)?;
				client.get_process(id).await.map_err(
					|source| tg::error!(!source, %id, %remote, "failed to get the process"),
				)
			}
			.boxed()
		});
		let Ok((output, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};

		// Spawn a task to put the process if it is finished.
		if output.data.status.is_finished() {
			tokio::spawn({
				let server = self.clone();
				let id = id.clone();
				let mut data = output.data.clone();
				async move {
					let arg = tg::process::children::get::Arg::default();
					let children = server
						.try_get_process_children(&id, arg)
						.await?
						.ok_or_else(|| tg::error!("expected the process to exist"))?
						.map_ok(|chunk| stream::iter(chunk.data).map(Ok::<_, tg::Error>))
						.try_flatten()
						.try_collect()
						.await?;
					data.children = Some(children);
					let arg = tg::process::put::Arg {
						data,
						local: None,
						remotes: None,
					};
					server.put_process(&id, arg).await?;
					Ok::<_, tg::Error>(())
				}
			});
		}

		Ok(Some(output))
	}

	pub(crate) async fn handle_get_process_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();
		let Some(output) = self.try_get_process_with_context(context, &id, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
