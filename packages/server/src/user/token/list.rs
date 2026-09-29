use {
	crate::{
		Session,
		cursor::{DEFAULT_LIMIT, MAX_LIMIT},
	},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
};

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "version")]
enum Cursor {
	V0 { after: tg::token::Id },
}

#[derive(db::row::Deserialize)]
struct Row {
	created_at: i64,

	#[tangram_database(as = "db::value::FromStr")]
	id: tg::token::Id,
}

impl Session {
	pub(crate) async fn list_user_tokens(
		&self,
		arg: tg::user::token::list::Arg,
	) -> tg::Result<tg::user::token::list::Output> {
		// Authenticate the user and parse the arg.
		let user = self.authenticated_user()?;
		let limit = arg.limit.unwrap_or(DEFAULT_LIMIT);
		if !(1..=MAX_LIMIT).contains(&limit) {
			return Err(tg::error!(
				"the page limit must be between 1 and {MAX_LIMIT}"
			));
		}
		let cursor = arg
			.cursor
			.as_deref()
			.map(crate::cursor::deserialize::<Cursor>)
			.transpose()?;
		let after = cursor.map(|cursor| match cursor {
			Cursor::V0 { after } => after,
		});

		// Read an extra row to determine whether another page exists.
		let mut rows = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let after = after.clone();
				let user = user.clone();
				async move {
					Self::list_user_tokens_with_transaction(
						transaction,
						&user,
						after.as_ref(),
						limit + 1,
					)
					.await
				}
				.boxed()
			})
			.await?;

		// Create the continuation cursor.
		let cursor = if u64::try_from(rows.len()).unwrap() > limit {
			rows.pop();
			let cursor = Cursor::V0 {
				after: rows.last().unwrap().id.clone(),
			};
			Some(crate::cursor::serialize(&cursor)?)
		} else {
			None
		};

		// Create the output.
		let data = rows
			.into_iter()
			.map(|row| tg::user::token::Data {
				created_at: row.created_at,
				id: row.id,
				token: None,
			})
			.collect();
		let output = tg::user::token::list::Output { cursor, data };

		Ok(output)
	}

	async fn list_user_tokens_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		user: &tg::user::Id,
		after: Option<&tg::token::Id>,
		limit: u64,
	) -> tg::Result<ControlFlow<Vec<Row>, crate::database::Error>> {
		// The empty string sorts before every token ID for the first page.
		let after = after.map(ToString::to_string).unwrap_or_default();
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select created_at, id
				from user_tokens
				where "user" = {p}1
					and id > {p}2
				order by id
				limit {p}3;
			"#
		);
		let result = transaction
			.query_all_into::<Row>(
				statement.into(),
				db::params![user.to_string(), after, i64::try_from(limit).unwrap()],
			)
			.await;
		let rows = crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(rows))
	}

	pub(crate) async fn list_user_tokens_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let output = self.list_user_tokens(arg).await?;
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
