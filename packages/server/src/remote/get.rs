use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, response::builder::Ext as _},
	tangram_uri::Uri,
};

impl Server {
	pub(crate) async fn try_get_remote_with_context(
		&self,
		context: &Context,
		name: &str,
	) -> tg::Result<Option<tg::remote::get::Output>> {
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
		let p = connection.p();
		let statement = formatdoc!(
			"
				select name, url
				from remotes
				where name = {p}1;
			",
		);
		let params = db::params![&name];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let output = row.map(|row| tg::remote::get::Output {
			name: row.name,
			url: row.url,
		});
		Ok(output)
	}

	pub(crate) async fn handle_get_remote_request(
		&self,
		_request: http::Request<Body>,
		context: &Context,
		name: &str,
	) -> tg::Result<http::Response<Body>> {
		let Some(output) = self
			.try_get_remote_with_context(context, name)
			.await
			.map_err(|source| tg::error!(!source, %name, "failed to get the remote"))?
		else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
