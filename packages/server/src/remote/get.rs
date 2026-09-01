use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_uri::Uri,
};

#[derive(db::row::Deserialize)]
struct Row {
	name: String,
	token: Option<String>,
	trusted: bool,
	#[tangram_database(as = "db::value::FromStr")]
	url: Uri,
}

impl Session {
	pub(crate) async fn try_get_remote(
		&self,
		name: &str,
		arg: tg::remote::get::Arg,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		if arg.principal.is_none() && matches!(self.context.principal, tg::Principal::Runner(_)) {
			return self.try_get_remote_runner(name).await;
		}
		if arg.principal.is_none()
			&& matches!(
				self.context.principal,
				tg::Principal::Process(_) | tg::Principal::Sandbox(_)
			) && self
			.server
			.config
			.roles
			.contains(&crate::config::Role::Runner)
			.then(|| self.server.config.runner.remote.as_deref())
			.flatten()
			.is_some_and(|remote| remote == name)
		{
			return self.try_get_remote_runner(name).await;
		}
		let principal = self
			.resolve_remote_arg_principal(arg.principal.clone())
			.await?;
		self.try_get_remote_for_principal(name, principal.as_ref())
			.await
	}

	async fn try_get_remote_for_principal(
		&self,
		name: &str,
		principal: Option<&tg::Principal>,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		let name = name.to_owned();
		let principal = principal.map(ToString::to_string);
		let row = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let name = name.clone();
				let principal = principal.clone();
				async move {
					Self::try_get_remote_for_principal_with_transaction(
						transaction,
						&name,
						principal.as_deref(),
					)
					.await
				}
				.boxed()
			})
			.await?;
		let output = row.map(|row| {
			let data = tg::remote::Data {
				name: row.name,
				token: row.token,
				trusted: row.trusted,
				url: row.url,
			};
			tg::remote::get::Output { data }
		});
		Ok(output)
	}

	async fn try_get_remote_for_principal_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		name: &str,
		principal: Option<&str>,
	) -> tg::Result<ControlFlow<Option<Row>, crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select name, token, trusted, url
				from remotes
				where name = {p}1 and (
					(principal is null and cast({p}2 as text) is null) or
					principal = {p}2
				);
			",
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![name, principal])
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(row))
	}

	async fn try_get_remote_runner(
		&self,
		name: &str,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		let Some(remote) = self
			.server
			.config
			.roles
			.contains(&crate::config::Role::Runner)
			.then(|| self.server.config.runner.remote.as_deref())
			.flatten()
		else {
			return Ok(None);
		};
		let name = name.to_owned();
		let remote = remote.to_owned();
		let row = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let name = name.clone();
				let remote = remote.clone();
				async move {
					Self::try_get_remote_runner_with_transaction(transaction, &name, &remote).await
				}
				.boxed()
			})
			.await?;
		let output = row.map(|row| {
			let data = tg::remote::Data {
				name: row.name,
				token: row.token,
				trusted: row.trusted,
				url: row.url,
			};
			tg::remote::get::Output { data }
		});
		Ok(output)
	}

	async fn try_get_remote_runner_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		name: &str,
		remote: &str,
	) -> tg::Result<ControlFlow<Option<Row>, crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select name, token, trusted, url
				from remotes
				where name = {p}1 and name = {p}2 and principal is null;
			",
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![name, remote])
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(row))
	}

	pub(crate) async fn try_get_remote_request(
		&self,
		request: http::Request<BoxBody>,
		name: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		self.verify_request_from_host()?;

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the remote.
		let Some(output) = self
			.try_get_remote(name, arg)
			.await
			.map_err(|error| tg::error!(!error, %name, "failed to get the remote"))?
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
