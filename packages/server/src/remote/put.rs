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
	pub(crate) async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
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
		let url = arg.url.to_string();
		let user = user.map(ToString::to_string);
		self.server
			.database
			.run(|transaction| {
				let name = name.clone();
				let url = url.clone();
				let user = user.clone();
				async move {
					Self::put_remote_with_transaction(transaction, &name, user.as_deref(), &url)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote"))?;

		Ok(())
	}

	async fn put_remote_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		name: &str,
		user: Option<&str>,
		url: &str,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				update remotes
				set url = {p}3
				where name = {p}1 and (
					("user" is null and {p}2 is null) or
					"user" = {p}2
				);
			"#,
		);
		let params = db::params![name, user, url];
		let result = transaction.execute(statement.into(), params).await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n == 0 {
			let statement = formatdoc!(
				r#"
					insert into remotes (name, "user", url)
					values ({p}1, {p}2, {p}3);
				"#,
			);
			let params = db::params![name, user, url];
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to execute the statement");
		}
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn put_remote_request(
		&self,
		request: http::Request<BoxBody>,
		name: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Put the remote.
		self.put_remote(name, arg)
			.await
			.map_err(|error| tg::error!(!error, %name, "failed to put the remote"))?;

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
