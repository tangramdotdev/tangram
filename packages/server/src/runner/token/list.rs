use {
	crate::Session,
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, response::Ext as _},
};

#[derive(db::row::Deserialize)]
struct Row {
	created_at: i64,

	#[tangram_database(as = "db::value::FromStr")]
	id: tg::token::Id,
}

impl Session {
	pub(crate) async fn list_runner_tokens(
		&self,
		runner: &tg::runner::Id,
		_arg: tg::runner::token::list::Arg,
	) -> tg::Result<tg::runner::token::list::Output> {
		self.get_authorized_runner(runner).await?;
		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select created_at, id
				from runner_tokens
				where runner = {p}1
				order by id;
			"
		);
		let rows = connection
			.query_all_into::<Row>(statement.into(), db::params![runner.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let data = rows
			.into_iter()
			.map(|row| tg::runner::token::Data {
				created_at: row.created_at,
				id: row.id,
			})
			.collect();

		Ok(tg::runner::token::list::Output { data })
	}

	pub(crate) async fn list_runner_tokens_request(
		&self,
		_request: http::Request<BoxBody>,
		runner: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let runner = runner
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the runner ID"))?;
		let output = self
			.list_runner_tokens(&runner, tg::runner::token::list::Arg::default())
			.await?;
		let body = serde_json::to_vec(&output).unwrap();
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.body(BoxBody::with_bytes(body))
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}
