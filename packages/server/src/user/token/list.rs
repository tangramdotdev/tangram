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
	pub(crate) async fn list_user_tokens(
		&self,
		_arg: tg::user::token::list::Arg,
	) -> tg::Result<tg::user::token::list::Output> {
		let user = self.authenticated_user()?;
		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			r#"
				select created_at, id
				from user_tokens
				where "user" = {p}1
				order by id;
			"#
		);
		let rows = connection
			.query_all_into::<Row>(statement.into(), db::params![user.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let data = rows
			.into_iter()
			.map(|row| tg::user::token::Data {
				created_at: row.created_at,
				id: row.id,
			})
			.collect();

		Ok(tg::user::token::list::Output { data })
	}

	pub(crate) async fn list_user_tokens_request(
		&self,
		_request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let output = self
			.list_user_tokens(tg::user::token::list::Arg::default())
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
