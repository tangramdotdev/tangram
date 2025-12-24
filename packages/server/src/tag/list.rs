use {
	crate::{Context, Database, Server},
	futures::{TryStreamExt as _, stream::FuturesUnordered},
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
		arg: tg::tag::list::Arg,
	) -> tg::Result<tg::tag::list::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let mut output = tg::tag::list::Output { data: Vec::new() };

		// List the local tags if requested.
		if Self::local(arg.local, arg.remotes.as_ref()) {
			let local_output = self
				.list_tags_local(arg.clone())
				.await
				.map_err(|source| tg::error!(!source, "failed to list local tags"))?;
			output.data.extend(local_output.data);
		}

		// List the remote tags if requested.
		let remotes = self
			.remotes(arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		let remote_outputs = remotes
			.into_iter()
			.map(|remote| {
				let arg = tg::tag::list::Arg {
					local: None,
					remotes: None,
					..arg.clone()
				};
				async move {
					let client = self.get_remote_client(remote.clone()).await.map_err(
						|source| tg::error!(!source, %remote, "failed to get the remote client"),
					)?;
					let mut output = client.list_tags(arg).await.map_err(
						|source| tg::error!(!source, %remote, "failed to list remote tags"),
					)?;
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
			.extend(remote_outputs.into_iter().flat_map(|output| output.data));

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
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();
		let output = self.list_tags_with_context(context, arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
