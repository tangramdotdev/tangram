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
		name: &str,
		arg: tg::remote::put::Arg,
	) -> tg::Result<()> {
		if !session.server.is_primary_region() {
			return self.put_remote_primary_region(session, name, arg).await;
		}
		Self::verify_request_can_mutate_remotes(session)?;
		if matches!(session.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthenticated"));
		}
		if arg.trusted && !matches!(session.context.principal, tg::Principal::Root) {
			return Err(tg::error!("unauthorized"));
		}
		let name = name.to_owned();
		let principal = self
			.resolve_remote_arg_principal(session, arg.principal)
			.await?;
		let trusted = arg.trusted;
		let url = arg.url.to_string();
		let principal = principal.as_ref().map(ToString::to_string);
		session
			.server
			.database
			.run(|transaction| {
				let name = name.clone();
				let principal = principal.clone();
				let url = url.clone();
				async move {
					Self::put_remote_with_transaction(
						transaction,
						&name,
						principal.as_deref(),
						trusted,
						&url,
					)
					.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote"))?;
		self.delete_remote_cache(session, &name).await?;

		Ok(())
	}

	async fn put_remote_primary_region(
		&self,
		session: &Session,
		name: &str,
		arg: tg::remote::put::Arg,
	) -> tg::Result<()> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		client.put_remote(name, arg).await.map_err(|error| {
			tg::error!(!error, "failed to put the remote in the primary region")
		})?;

		Ok(())
	}

	async fn put_remote_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		name: &str,
		principal: Option<&str>,
		trusted: bool,
		url: &str,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				update remotes
				set trusted = {p}3, url = {p}4
				where name = {p}1 and (
					(principal is null and cast({p}2 as text) is null) or
					principal = {p}2
				);
			",
		);
		let params = db::params![name, principal, trusted, url];
		let result = transaction.execute(statement.into(), params).await;
		let n = tangram_server_core::database::retry!(result, "failed to execute the statement");
		if n == 0 {
			let statement = formatdoc!(
				r"
					insert into remotes (name, principal, trusted, url)
					values ({p}1, {p}2, {p}3, {p}4);
				",
			);
			let params = db::params![name, principal, trusted, url];
			let result = transaction.execute(statement.into(), params).await;
			tangram_server_core::database::retry!(result, "failed to execute the statement");
		}
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn put_remote_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		name: &str,
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
		self.put_remote(session, name, arg)
			.await
			.map_err(|error| tg::error!(!error, %name, "failed to put the remote"))?;

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
