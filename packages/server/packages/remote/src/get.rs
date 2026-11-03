use {
	crate::Component,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::Session,
	tangram_uri::Uri,
};

#[derive(db::row::Deserialize)]
pub(crate) struct Row {
	#[tangram_database(as = "db::value::FromStr")]
	pub id: tg::remote::Id,

	pub name: String,

	#[tangram_database(as = "Option<db::value::FromStr>")]
	pub principal: Option<tg::Principal>,

	pub token: Option<String>,

	pub trusted: bool,

	#[tangram_database(as = "db::value::FromStr")]
	pub url: Uri,
}

impl Component {
	pub(crate) async fn try_get_remote(
		&self,
		session: &Session,
		remote: &tg::remote::Selector,
		arg: tg::remote::get::Arg,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		let output = self
			.try_get_remote_inner(session, remote, arg.principal)
			.await?
			.map(|row| {
				let data = row.into();
				tg::remote::get::Output { data }
			});
		Ok(output)
	}

	pub(crate) async fn try_get_remote_inner(
		&self,
		session: &Session,
		remote: &tg::remote::Selector,
		principal: Option<tg::principal::Selector>,
	) -> tg::Result<Option<Row>> {
		// Get only the runner's remote for a caller restricted to it.
		if principal.is_none() && Self::is_runner_remote_caller(session) {
			let row = self
				.try_get_runner_remote(session)
				.await?
				.filter(|row| match remote {
					tg::remote::Selector::Id(id) => row.id == *id,
					tg::remote::Selector::Name(name) => row.name == *name,
				});
			return Ok(row);
		}

		// Resolve the principal and get the remote.
		let Some(principal) = self
			.try_resolve_remote_selector_principal(session, remote, principal)
			.await?
		else {
			return Ok(None);
		};
		let row = self.try_get_remote_row(session, remote, principal).await?;

		Ok(row)
	}

	pub(crate) fn is_runner_remote_caller(session: &Session) -> bool {
		// A runner, or a process or sandbox on a runner, may use only the runner's configured remote.
		match &session.context.principal {
			tg::Principal::Runner(_) => true,
			tg::Principal::Process(_) | tg::Principal::Sandbox(_) => {
				session
					.server
					.config
					.roles
					.contains(&tangram_server_core::config::Role::Runner)
					&& session.server.config.runner.remote.is_some()
			},
			_ => false,
		}
	}

	pub(crate) async fn try_get_runner_remote(&self, session: &Session) -> tg::Result<Option<Row>> {
		let runner = session
			.server
			.config
			.roles
			.contains(&tangram_server_core::config::Role::Runner);
		let Some(name) = runner
			.then(|| session.server.config.runner.remote.clone())
			.flatten()
		else {
			return Ok(None);
		};
		let remote = tg::remote::Selector::Name(name);
		let row = self.try_get_remote_row(session, &remote, None).await?;

		Ok(row)
	}

	async fn try_get_remote_row(
		&self,
		session: &Session,
		remote: &tg::remote::Selector,
		principal: Option<tg::Principal>,
	) -> tg::Result<Option<Row>> {
		let principal = principal.as_ref().map(ToString::to_string);
		let remote = remote.clone();
		session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let principal = principal.clone();
				let remote = remote.clone();
				async move {
					Self::try_get_remote_row_with_transaction(
						transaction,
						&remote,
						principal.as_deref(),
					)
					.await
				}
				.boxed()
			})
			.await
	}

	async fn try_get_remote_row_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		remote: &tg::remote::Selector,
		principal: Option<&str>,
	) -> tg::Result<ControlFlow<Option<Row>, tangram_server_core::database::Error>> {
		let p = transaction.p();
		let column = crate::selector_column(remote);
		let statement = formatdoc!(
			r"
				select id, name, principal, token, trusted, url
				from remotes
				where coalesce(principal, '') = {p}1 and {column} = {p}2;
			",
		);
		let params = db::params![principal.unwrap_or_default(), remote.to_string()];
		let result = transaction
			.query_optional_into::<Row>(statement.into(), params)
			.await;
		let row = tangram_server_core::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(row))
	}

	pub(crate) async fn try_get_remote_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		remote: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		session.verify_request_from_host()?;

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let remote = remote
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the remote"))?;

		// Get the remote.
		let Some(output) = self
			.try_get_remote(session, &remote, arg)
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote"))?
		else {
			let response = http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		};

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

impl From<Row> for tg::remote::Data {
	fn from(value: Row) -> Self {
		Self {
			id: value.id,
			name: value.name,
			principal: value.principal.unwrap_or_default(),
			trusted: value.trusted,
			url: value.url,
		}
	}
}
