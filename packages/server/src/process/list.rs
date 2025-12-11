use {
	crate::{Context, Server, database::Database},
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

impl Server {
	pub(crate) async fn list_processes_with_context(
		&self,
		context: &Context,
		arg: tg::process::list::Arg,
	) -> tg::Result<tg::process::list::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let mut output = tg::process::list::Output { data: Vec::new() };

		// List the local processes if requested.
		if Self::local(arg.local, arg.remotes.as_ref()) {
			let local_data = self.list_processes_local().await?;
			output.data.extend(local_data);
		}

		// List the remote processes if requested.
		let remotes = self.remotes(arg.remotes.clone()).await?;
		let remote_outputs = remotes
			.into_iter()
			.map(|remote| {
				let arg = tg::process::list::Arg {
					local: None,
					remotes: None,
				};
				async move {
					let client = self.get_remote_client(remote.clone()).await?;
					let output = client.list_processes(arg).await?;
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

	async fn list_processes_local(&self) -> tg::Result<Vec<tg::process::get::Output>> {
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => self.list_processes_postgres(database).await,
			Database::Sqlite(database) => self.list_processes_sqlite(database).await,
		}
	}

	pub(crate) async fn handle_list_processes_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = self.list_processes_with_context(context, arg).await?;
		let output = output.data;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
