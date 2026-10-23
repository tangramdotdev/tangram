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
	pub(crate) async fn try_delete_remote(
		&self,
		session: &Session,
		name: &str,
		arg: tg::remote::delete::Arg,
	) -> tg::Result<Option<()>> {
		if !session.server.is_primary_region() {
			return self.delete_remote_primary_region(session, name, arg).await;
		}
		Self::verify_request_can_mutate_remotes(session)?;
		if matches!(session.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthenticated"));
		}
		let name = name.to_owned();
		let principal = self
			.resolve_remote_arg_principal(session, arg.principal)
			.await?;
		let principal = principal.as_ref().map(ToString::to_string);
		let n = session
			.server
			.database
			.run(|transaction| {
				let name = name.clone();
				let principal = principal.clone();
				async move {
					Self::delete_remote_with_transaction(transaction, &name, principal.as_deref())
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the remote"))?;
		if n == 0 {
			return Ok(None);
		}
		self.delete_remote_cache(session, &name).await?;

		Ok(Some(()))
	}

	async fn delete_remote_primary_region(
		&self,
		session: &Session,
		name: &str,
		arg: tg::remote::delete::Arg,
	) -> tg::Result<Option<()>> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		let deleted = client.try_delete_remote(name, arg).await.map_err(|error| {
			tg::error!(!error, "failed to delete the remote in the primary region")
		})?;

		Ok(deleted)
	}

	async fn delete_remote_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		name: &str,
		principal: Option<&str>,
	) -> tg::Result<ControlFlow<u64, tangram_server_core::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				delete from remotes
				where name = {p}1 and (
					(principal is null and cast({p}2 as text) is null) or
					principal = {p}2
				);
			",
		);
		let params = db::params![name, principal];
		let result = transaction.execute(statement.into(), params).await;
		let n = tangram_server_core::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(n))
	}

	pub(crate) async fn delete_remote_request(
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
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// Delete the remote.
		let deleted = self
			.try_delete_remote(session, name, arg)
			.await
			.map_err(|error| tg::error!(!error, %name, "failed to delete the remote"))?;
		if deleted.is_none() {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		}

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
