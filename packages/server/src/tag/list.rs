use crate::{Database, Server};
use futures::{TryStreamExt, stream::FuturesUnordered};
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

impl Server {
	pub async fn list_tags(
		&self,
		mut arg: tg::tag::list::Arg,
	) -> tg::Result<tg::tag::list::Output> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let client = self.get_remote_client(remote.clone()).await?;
			let arg = tg::tag::list::Arg {
				remote: None,
				..arg
			};
			let mut output = client.list_tags(arg).await?;
			for output in &mut output.data {
				output.remote = Some(remote.clone());
			}
			return Ok(output);
		}

		// List the local tags.
		let mut output = self.list_tags_local(arg.clone()).await?;

		// List the remote tags.
		let remote = self
			.get_remote_clients()
			.await?
			.into_iter()
			.map(|(remote, client)| {
				let arg = arg.clone();
				async move {
					let mut output = client.list_tags(arg).await?;
					for output in &mut output.data {
						output.remote = Some(remote.clone());
					}
					Ok::<_, tg::Error>(output)
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		output
			.data
			.extend(remote.into_iter().flat_map(|output| output.data));

		Ok(output)
	}

	async fn list_tags_local(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => self.list_tags_postgres(database, arg).await,
			Database::Sqlite(database) => self.list_tags_sqlite(database, arg).await,
		}
	}

	pub(crate) async fn handle_list_tags_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = handle.list_tags(arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
