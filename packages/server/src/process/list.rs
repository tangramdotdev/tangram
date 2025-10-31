use {
	crate::{Server, database::Database, handle::ServerOrProxy},
	tangram_client::{self as tg, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

impl Server {
	pub async fn list_processes(
		&self,
		_arg: tg::process::list::Arg,
	) -> tg::Result<tg::process::list::Output> {
		let data = self.list_processes_local().await?;
		Ok(tg::process::list::Output { data })
	}

	async fn list_processes_local(&self) -> tg::Result<Vec<tg::process::get::Output>> {
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => self.list_processes_postgres(database).await,
			Database::Sqlite(database) => self.list_processes_sqlite(database).await,
		}
	}

	pub(crate) async fn handle_list_processes_request(
		handle: &ServerOrProxy,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>> {
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = handle.list_processes(arg).await?;
		let output = output.data;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
