use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn get_pty_size_with_context(
		&self,
		_context: &Context,
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

	pub(crate) async fn handle_get_pty_size_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Parse the ID.
		let id = id.parse()?;
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to parse the body"))?;
		let output = self.get_pty_size_with_context(context, &id, arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
