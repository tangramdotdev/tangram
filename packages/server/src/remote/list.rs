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
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_uri::Uri,
};

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "version")]
enum Cursor {
	V0 { after: String },
}

#[derive(db::row::Deserialize)]
struct Row {
	name: String,
	trusted: bool,
	#[tangram_database(as = "db::value::FromStr")]
	url: Uri,
}

impl Session {
	pub(crate) async fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		// Parse the arg.
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

		// Resolve the principal and any runner-specific restriction.
		let remote = self
			.server
			.config
			.roles
			.contains(&crate::config::Role::Runner)
			.then(|| self.server.config.runner.remote.as_deref())
			.flatten();
		let restricted = arg.principal.is_none()
			&& (matches!(self.context.principal, tg::Principal::Runner(_))
				|| (matches!(
					self.context.principal,
					tg::Principal::Process(_) | tg::Principal::Sandbox(_)
				) && remote.is_some()));
		let (principal, name) = if restricted {
			let Some(remote) = remote else {
				let output = tg::remote::list::Output {
					cursor: None,
					data: Vec::new(),
				};
				return Ok(output);
			};
			(None, Some(remote.to_owned()))
		} else {
			let principal = self.resolve_remote_arg_principal(arg.principal).await?;
			(principal.map(|principal| principal.to_string()), None)
		};

		// Read an extra row to determine whether another page exists.
		let mut rows = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let after = after.clone();
				let name = name.clone();
				let principal = principal.clone();
				async move {
					Self::list_remotes_with_transaction(
						transaction,
						principal.as_deref(),
						name.as_deref(),
						after.as_deref(),
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
				after: rows.last().unwrap().name.clone(),
			};
			Some(crate::cursor::serialize(&cursor)?)
		} else {
			None
		};

		// Create the output.
		let data = rows
			.into_iter()
			.map(|row| tg::remote::Data {
				name: row.name,
				token: None,
				trusted: row.trusted,
				url: row.url,
			})
			.collect();
		let output = tg::remote::list::Output { cursor, data };

		Ok(output)
	}

	async fn list_remotes_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		principal: Option<&str>,
		name: Option<&str>,
		after: Option<&str>,
		limit: u64,
	) -> tg::Result<ControlFlow<Vec<Row>, crate::database::Error>> {
		let p = transaction.p();
		let comparison = if after.is_some() { ">" } else { ">=" };
		let statement = formatdoc!(
			"
			select name, trusted, url from remotes
			where coalesce(principal, '') = {p}1
				and name {comparison} {p}2
				and (cast({p}4 as text) is null or name = {p}4)
			order by name limit {p}3;
		"
		);
		let result = transaction
			.query_all_into::<Row>(
				statement.into(),
				db::params![
					principal.unwrap_or_default(),
					after.unwrap_or_default(),
					i64::try_from(limit).unwrap(),
					name
				],
			)
			.await;
		let rows = crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(rows))
	}

	pub(crate) async fn list_remotes_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		self.verify_request_from_host()?;

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// List the remotes.
		let output = self
			.list_remotes(arg)
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
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
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
