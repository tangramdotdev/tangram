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
};

impl Session {
	pub(crate) async fn delete_remote(
		&self,
		name: &str,
		arg: tg::remote::delete::Arg,
	) -> tg::Result<()> {
		let deleted = self.delete_remote_inner(name, arg).await?;
		if !deleted {
			return Err(tg::error!("failed to find the remote"));
		}
		Ok(())
	}

	async fn delete_remote_inner(
		&self,
		name: &str,
		arg: tg::remote::delete::Arg,
	) -> tg::Result<bool> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthenticated"));
		}
		if matches!(
			self.context.principal,
			tg::Principal::Process(_) | tg::Principal::Sandbox(_)
		) {
			return Err(tg::error!("unauthorized"));
		}

		let name = name.to_owned();
		let principal = self.resolve_remote_arg_principal(arg.principal).await?;
		let principal = principal.as_ref().map(ToString::to_string);
		let n = self
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

		Ok(n != 0)
	}

	async fn delete_remote_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		name: &str,
		principal: Option<&str>,
	) -> tg::Result<ControlFlow<u64, crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				delete from remotes
				where name = {p}1 and (
					(principal is null and {p}2 is null) or
					principal = {p}2
				);
			",
		);
		let params = db::params![name, principal];
		let result = transaction.execute(statement.into(), params).await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(n))
	}

	pub(crate) async fn delete_remote_request(
		&self,
		request: http::Request<BoxBody>,
		name: &str,
	) -> tg::Result<http::Response<BoxBody>> {
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

		// Delete the remote.
		let deleted = self
			.delete_remote_inner(name, arg)
			.await
			.map_err(|error| tg::error!(!error, %name, "failed to delete the remote"))?;
		if !deleted {
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
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
