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
};

impl Component {
	pub(crate) async fn put_remote(
		&self,
		session: &Session,
		remote: &tg::remote::Selector,
		arg: tg::remote::put::Arg,
	) -> tg::Result<()> {
		if !session.server.is_primary_region() {
			return self.put_remote_primary_region(session, remote, arg).await;
		}
		Self::verify_request_can_mutate_remotes(session)?;
		if matches!(session.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthenticated"));
		}
		if arg.trusted && !matches!(session.context.principal, tg::Principal::Root) {
			return Err(tg::error!("unauthorized"));
		}
		if let tg::remote::Selector::Name(name) = remote {
			crate::validate_name(name)?;
		}
		let Some(principal) = self
			.try_resolve_remote_selector_principal(session, remote, arg.principal)
			.await?
		else {
			return Err(tg::error!(missing, %remote, "failed to find the remote"));
		};
		let remote = remote.clone();
		let trusted = arg.trusted;
		let url = arg.url.to_string();
		let principal = principal.as_ref().map(ToString::to_string);
		let name = session
			.server
			.database
			.run(|transaction| {
				let principal = principal.clone();
				let remote = remote.clone();
				let url = url.clone();
				async move {
					Self::put_remote_with_transaction(
						transaction,
						&remote,
						principal.as_deref(),
						trusted,
						&url,
					)
					.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote"))?
			.ok_or_else(|| tg::error!(missing, %remote, "failed to find the remote"))?;
		self.delete_remote_cache(session, &name).await?;

		Ok(())
	}

	async fn put_remote_primary_region(
		&self,
		session: &Session,
		remote: &tg::remote::Selector,
		arg: tg::remote::put::Arg,
	) -> tg::Result<()> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		client.put_remote(remote, arg).await.map_err(|error| {
			tg::error!(!error, "failed to put the remote in the primary region")
		})?;

		Ok(())
	}

	async fn put_remote_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		remote: &tg::remote::Selector,
		principal: Option<&str>,
		trusted: bool,
		url: &str,
	) -> tg::Result<ControlFlow<Option<String>, tangram_server_core::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			name: String,
		}

		// Update the remote.
		let p = transaction.p();
		let column = crate::selector_column(remote);
		let statement = formatdoc!(
			r"
				update remotes
				set trusted = {p}3, url = {p}4
				where coalesce(principal, '') = {p}1 and {column} = {p}2
				returning name;
			",
		);
		let params = db::params![
			principal.unwrap_or_default(),
			remote.to_string(),
			trusted,
			url
		];
		let result = transaction
			.query_optional_into::<Row>(statement.into(), params)
			.await;
		let row = tangram_server_core::database::retry!(result, "failed to execute the statement");
		if let Some(row) = row {
			return Ok(ControlFlow::Break(Some(row.name)));
		}

		// Create the remote when it is selected by a name that does not exist.
		let tg::remote::Selector::Name(name) = remote else {
			return Ok(ControlFlow::Break(None));
		};
		let id = tg::remote::Id::new();
		let statement = formatdoc!(
			r"
				insert into remotes (id, name, principal, trusted, url)
				values ({p}1, {p}2, {p}3, {p}4, {p}5);
			",
		);
		let params = db::params![id.to_string(), name, principal, trusted, url];
		let result = transaction.execute(statement.into(), params).await;
		tangram_server_core::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(Some(name.clone())))
	}

	pub(crate) async fn put_remote_request(
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

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Put the remote.
		let remote = remote
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the remote"))?;
		self.put_remote(session, &remote, arg)
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to put the remote"))?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().empty().unwrap().boxed_body();

		Ok(response)
	}
}
