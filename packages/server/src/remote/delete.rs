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
	pub(crate) async fn try_delete_remote(&self, name: &str) -> tg::Result<Option<()>> {
		if matches!(self.context.principal, tg::Principal::Process(_)) {
			return Err(tg::error!("unauthorized"));
		}

		let principal = &self.context.principal;
		let user = match principal {
			tg::Principal::Root => None,
			tg::Principal::User(user) => Some(user),
			_ => {
				return Err(tg::error!("unauthorized"));
			},
		};

		let name = name.to_owned();
		let user = user.map(ToString::to_string);
		let n = self
			.server
			.database
			.run(|transaction| {
				let name = name.clone();
				let user = user.clone();
				async move {
					Self::try_delete_remote_with_transaction(transaction, &name, user.as_deref())
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the remote"))?;

		if n == 0 {
			return Ok(None);
		}

		Ok(Some(()))
	}

	async fn try_delete_remote_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		name: &str,
		user: Option<&str>,
	) -> tg::Result<ControlFlow<u64, crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from remotes
				where name = {p}1 and (
					("user" is null and {p}2 is null) or
					"user" = {p}2
				);
			"#,
		);
		let params = db::params![name, user];
		let result = transaction.execute(statement.into(), params).await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(n))
	}

	pub(crate) async fn try_delete_remote_request(
		&self,
		request: http::Request<BoxBody>,
		name: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Delete the remote.
		let Some(()) = self
			.try_delete_remote(name)
			.await
			.map_err(|error| tg::error!(!error, %name, "failed to delete the remote"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

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
