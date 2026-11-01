use {
	crate::Component,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
	tangram_server_core::Session,
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

impl Component {
	pub(crate) async fn list_runner_tokens(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		mut arg: tg::runner::token::list::Arg,
	) -> tg::Result<tg::runner::token::list::Output> {
		let location = session.server.runner_location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				let client = session.get_region_session(&region).await?;
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				client.list_runner_tokens(runner, arg).await
			},
			tg::Location::Local(_) => {
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				self.list_runner_tokens_local(session, runner, arg).await
			},
			tg::Location::Remote(remote) => {
				let client = session
					.server
					.components
					.remote
					.get_remote_session(session, &remote.name)
					.await?;
				arg.location = Some(
					tg::Location::Local(tg::location::Local {
						region: remote.region.clone(),
					})
					.into(),
				);
				let mut output = client.list_runner_tokens(runner, arg).await?;
				let location = tg::Location::Remote(tg::location::Remote {
					name: remote.name.clone(),
					region: remote.region,
				});
				let trusted = client.trusted();
				for item in &mut output.data {
					match item {
						tg::Either::Left(output) => {
							output.location = Some(location.clone());
						},
						tg::Either::Right(referent) => {
							session
								.server
								.components
								.authorization
								.update_tokens_and_location(
									session,
									&mut referent.options.tokens,
									Some(&mut referent.options.location),
									&location,
									trusted,
								)?;
						},
					}
				}
				Ok(output)
			},
		}
	}

	async fn list_runner_tokens_local(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		arg: tg::runner::token::list::Arg,
	) -> tg::Result<tg::runner::token::list::Output> {
		// Authenticate the runner and parse the arg.
		self.get_authorized_runner(session, runner).await?;
		let limit = session.parse_page_limit(arg.limit)?;
		let cursor = arg
			.cursor
			.as_deref()
			.map(tangram_server_core::cursor::deserialize::<Cursor>)
			.transpose()?;
		let after = cursor.map(|cursor| match cursor {
			Cursor::V0 { after } => after,
		});

		// Read an extra row to determine whether another page exists.
		let mut rows = session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let after = after.clone();
				let runner = runner.clone();
				async move {
					Self::list_runner_tokens_with_transaction(
						transaction,
						&runner,
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
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let data = rows
			.into_iter()
			.map(|row| {
				let data = tg::runner::token::Data {
					created_at: row.created_at,
					id: row.id,
					token: None,
				};
				let output = tg::runner::token::get::Output {
					data,
					location: Some(location.clone()),
				};
				item(output, arg.expand)
			})
			.collect();
		let output = tg::runner::token::list::Output { cursor, data };

		Ok(output)
	}

	async fn list_runner_tokens_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		runner: &tg::runner::Id,
		after: Option<&tg::token::Id>,
		limit: u64,
	) -> tg::Result<ControlFlow<Vec<Row>, tangram_server_core::database::Error>> {
		// The empty string sorts before every token ID for the first page.
		let after = after.map(ToString::to_string).unwrap_or_default();
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select created_at, id
				from runner_tokens
				where "runner" = {p}1
					and id > {p}2
				order by id
				limit {p}3;
			"#
		);
		let result = transaction
			.query_all_into::<Row>(
				statement.into(),
				db::params![runner.to_string(), after, i64::try_from(limit).unwrap()],
			)
			.await;
		let rows = tangram_server_core::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(rows))
	}

	pub(crate) async fn list_runner_tokens_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		runner: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let runner = runner
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the runner ID"))?;
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let output = self.list_runner_tokens(session, &runner, arg).await?;
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

fn item(output: tg::runner::token::get::Output, expand: bool) -> tg::runner::token::list::Item {
	if expand {
		return tg::Either::Left(output);
	}
	let options = tg::referent::Options {
		location: output.location,
		..Default::default()
	};
	let referent = tg::Referent::new(output.data.id, options);
	tg::Either::Right(referent)
}
