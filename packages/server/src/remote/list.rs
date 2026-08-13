use {
	crate::Session,
	futures::FutureExt as _,
	indoc::{formatdoc, indoc},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_uri::Uri,
};

#[derive(db::row::Deserialize)]
struct Row {
	name: String,
	#[tangram_database(as = "db::value::FromStr")]
	url: Uri,
}

impl Session {
	pub(crate) async fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		if arg.principal.is_none() && matches!(self.context.principal, tg::Principal::Runner(_)) {
			return self.list_remotes_runner().await;
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
			.is_some()
		{
			return self.list_remotes_runner().await;
		}
		let principal = self
			.resolve_remote_arg_principal(arg.principal.clone())
			.await?;
		self.list_remotes_for_principal(principal.as_ref()).await
	}

	async fn list_remotes_for_principal(
		&self,
		principal: Option<&tg::Principal>,
	) -> tg::Result<tg::remote::list::Output> {
		let principal = principal.map(ToString::to_string);
		let rows = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let principal = principal.clone();
				async move {
					Self::list_remotes_for_principal_with_transaction(
						transaction,
						principal.as_deref(),
					)
					.await
				}
				.boxed()
			})
			.await?;
		let data = rows
			.into_iter()
			.map(|row| tg::remote::get::Output {
				name: row.name,
				token: None,
				url: row.url,
			})
			.collect();
		let output = tg::remote::list::Output { data };
		Ok(output)
	}

	async fn list_remotes_for_principal_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		principal: Option<&str>,
	) -> tg::Result<ControlFlow<Vec<Row>, crate::database::Error>> {
		let p = transaction.p();
		let statement = indoc!(
			r"
				select name, url
				from remotes
				where (principal is null and cast({p}1 as text) is null) or principal = {p}1
				order by name;
			",
		);
		let statement = statement.replace("{p}", p);
		let result = transaction
			.query_all_into::<Row>(statement.into(), db::params![principal])
			.await;
		let rows = crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(rows))
	}

	async fn list_remotes_runner(&self) -> tg::Result<tg::remote::list::Output> {
		let Some(remote) = self
			.server
			.config
			.roles
			.contains(&crate::config::Role::Runner)
			.then(|| self.server.config.runner.remote.as_deref())
			.flatten()
		else {
			return Ok(tg::remote::list::Output { data: Vec::new() });
		};
		let remote = remote.to_owned();
		let rows = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let remote = remote.clone();
				async move { Self::list_remotes_runner_with_transaction(transaction, &remote).await }
					.boxed()
			})
			.await?;
		let data = rows
			.into_iter()
			.map(|row| tg::remote::get::Output {
				name: row.name,
				token: None,
				url: row.url,
			})
			.collect();
		let output = tg::remote::list::Output { data };
		Ok(output)
	}

	async fn list_remotes_runner_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		remote: &str,
	) -> tg::Result<ControlFlow<Vec<Row>, crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select name, url
				from remotes
				where name = {p}1 and principal is null
				order by name;
			",
		);
		let result = transaction
			.query_all_into::<Row>(statement.into(), db::params![remote])
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
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();

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
