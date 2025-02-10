use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{request::Ext as _, response::builder::Ext as _, Body};
use url::Url;

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
			url: Url,
		}
		let statement = formatdoc!(
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
			.map_err(|source| tg::error!(!source, "failed to execute the statemtent"))?;
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
}

impl Server {
	pub(crate) async fn handle_list_remotes_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = handle.list_remotes(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
