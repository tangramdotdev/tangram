use {
	crate::Component,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_server_core::Session,
};

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "version")]
enum Cursor {
	V0 { after: tg::remote::Id },
}

impl Component {
	pub(crate) async fn list_remotes(
		&self,
		session: &Session,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		// Parse the arg.
		let limit = session.parse_page_limit(arg.limit)?;
		let cursor = arg
			.cursor
			.as_deref()
			.map(tangram_server_core::cursor::deserialize::<Cursor>)
			.transpose()?;
		let after = cursor.map(|cursor| match cursor {
			Cursor::V0 { after } => after,
		});

		// List only the runner's configured remote for a caller restricted to it.
		if arg.principal.is_none() && Self::is_runner_remote_caller(session) {
			let data = self
				.try_get_runner_remote(session)
				.await?
				.filter(|row| after.as_ref().is_none_or(|after| row.id > *after))
				.map(|row| {
					let data = row.into();
					let output = tg::remote::get::Output { data };
					item(output, arg.expand)
				})
				.into_iter()
				.collect();
			let output = tg::remote::list::Output { cursor: None, data };
			return Ok(output);
		}

		// Resolve the principal.
		let principal = self
			.resolve_remote_arg_principal(session, arg.principal)
			.await?
			.map(|principal| principal.to_string());

		// Read an extra row to determine whether another page exists.
		let mut rows = session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let after = after.clone();
				let principal = principal.clone();
				async move {
					Self::list_remotes_with_transaction(
						transaction,
						principal.as_deref(),
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
			Some(tangram_server_core::cursor::serialize(&cursor)?)
		} else {
			None
		};

		// Create the output.
		let data = rows
			.into_iter()
			.map(|row| {
				let data = row.into();
				let output = tg::remote::get::Output { data };
				item(output, arg.expand)
			})
			.collect();
		let output = tg::remote::list::Output { cursor, data };

		Ok(output)
	}

	async fn list_remotes_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		principal: Option<&str>,
		after: Option<&tg::remote::Id>,
		limit: u64,
	) -> tg::Result<ControlFlow<Vec<crate::get::Row>, tangram_server_core::database::Error>> {
		let p = transaction.p();
		// The empty string sorts before every remote ID for the first page.
		let after = after.map(ToString::to_string).unwrap_or_default();
		let statement = formatdoc!(
			"
			select id, name, principal, cast(null as text) as token, trusted, url from remotes
			where coalesce(principal, '') = {p}1
				and id > {p}2
			order by id limit {p}3;
		"
		);
		let result = transaction
			.query_all_into::<crate::get::Row>(
				statement.into(),
				db::params![
					principal.unwrap_or_default(),
					after,
					i64::try_from(limit).unwrap()
				],
			)
			.await;
		let rows = tangram_server_core::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(rows))
	}

	pub(crate) async fn list_remotes_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		session.verify_request_from_host()?;

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the arg.
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// List the remotes.
		let output = self
			.list_remotes(session, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to list the remotes"))?;

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}

fn item(output: tg::remote::get::Output, expand: bool) -> tg::remote::list::Item {
	if expand {
		return tg::Either::Left(output);
	}
	let referent = tg::Referent::with_node(output.data.id);
	tg::Either::Right(referent)
}
