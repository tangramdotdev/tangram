use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{response::builder::Ext as _, Body};
use url::Url;

impl Server {
	pub async fn try_get_remote(&self, name: &str) -> tg::Result<Option<tg::remote::get::Output>> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;
		#[derive(Debug, serde::Deserialize)]
		struct Row {
			name: String,
			url: Url,
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
			.map_err(|source| tg::error!(!source, "failed to execute the statemtent"))?;
		let output = row.map(|row| tg::remote::get::Output {
			name: row.name,
			url: row.url,
		});
		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_get_remote_request<H>(
		handle: &H,
		_request: http::Request<Body>,
		name: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let Some(output) = handle.try_get_remote(name).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
