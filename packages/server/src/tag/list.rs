use {
	crate::{Context, Database, Server},
	futures::{TryStreamExt, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

impl Server {
	pub(crate) async fn list_tags_with_context(
		&self,
		context: &Context,
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

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
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

	pub(crate) async fn handle_list_tags_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = self.list_tags_with_context(context, arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
