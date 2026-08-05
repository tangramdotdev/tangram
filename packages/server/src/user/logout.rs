use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, response::Ext as _, response::builder::Ext as _},
};

impl Session {
	pub(crate) async fn logout(&self) -> tg::Result<()> {
		let tg::Principal::User(user) = &self.context.principal else {
			return Err(tg::error!("not logged in"));
		};
		let token = self
			.context
			.token
			.as_deref()
			.ok_or_else(|| tg::error!("missing the session token"))?;
		let token = crate::token::hash(token);
		let user = user.to_string();
		self.server
			.database
			.run(|transaction| {
				let token = token.clone();
				let user = user.clone();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						r#"
							delete from user_tokens
							where
								token = {p}1
								and "user" = {p}2;
						"#
					);
					let n = transaction
						.execute(statement.into(), db::params![token, user])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
					if n != 1 {
						return Err(tg::error!("invalid session").into());
					}

					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await?;

		Ok(())
	}

	pub(crate) async fn logout_request(
		&self,
		_request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		self.logout().await?;
		let response = http::Response::builder()
			.status(http::StatusCode::NO_CONTENT)
			.empty()
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}
