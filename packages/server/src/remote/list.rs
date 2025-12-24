use {
	crate::{Context, Server},
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_uri::Uri,
};

impl Server {
	pub(crate) async fn list_remotes_with_context(
		&self,
		context: &Context,
		_arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		#[derive(db::row::Deserialize)]
		struct Row {
			name: String,
			#[tangram_database(as = "db::value::FromStr")]
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
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
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
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();
		let output = self
			.list_remotes_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to list the remotes"))?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
