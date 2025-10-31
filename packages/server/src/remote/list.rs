use {
	crate::{Server, handle::ServerOrProxy},
	indoc::indoc,
	tangram_client::{self as tg, prelude::*},
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_uri::Uri,
};

impl Server {
	pub async fn list_remotes(
		&self,
		_arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		#[derive(Debug, serde::Deserialize)]
		struct Row {
			name: String,
			url: Uri,
		}
		let statement = indoc!(
			"
				select name, url
				from remotes
				order by name;
			",
		);
		let params = db::params![];
		let rows = connection
			.query_all_into::<db::row::Serde<Row>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statemtent"))?
			.into_iter()
			.map(|row| row.0)
			.collect::<Vec<_>>();
		let data = rows
			.into_iter()
			.map(|row| tg::remote::get::Output {
				name: row.name,
				url: row.url,
			})
			.collect();
		let output = tg::remote::list::Output { data };
		Ok(output)
	}

	pub(crate) async fn handle_list_remotes_request(
		handle: &ServerOrProxy,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>> {
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = handle.list_remotes(arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
