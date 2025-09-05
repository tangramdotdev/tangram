use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

impl Server {
	pub async fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		mut arg: tg::pty::read::Arg,
	) -> tg::Result<Option<tg::pty::Size>> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			return remote.get_pty_size(id, arg).await;
		}
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		#[derive(serde::Deserialize)]
		struct Row {
			size: db::value::Json<tg::pty::Size>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select size
				from ptys
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let Some(row) = connection
			.query_optional_into::<db::row::Serde<Row>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?
			.map(|row| row.0)
		else {
			return Ok(None);
		};
		Ok(Some(row.size.0))
	}

	pub(crate) async fn handle_get_pty_size_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to parse the body"))?;
		let output = handle.get_pty_size(&id, arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
