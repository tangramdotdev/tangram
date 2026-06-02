use {
	crate::{Session, context::Authentication},
	futures::FutureExt as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn delete_tag_grant(
		&self,
		arg: tg::tag::grants::delete::Arg,
	) -> tg::Result<Option<()>> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.delete_tag_grant_local(arg).await,
			tg::Location::Remote(remote) => self.delete_tag_grant_remote(arg, remote).await,
		}
	}

	async fn delete_tag_grant_local(
		&self,
		arg: tg::tag::grants::delete::Arg,
	) -> tg::Result<Option<()>> {
		if matches!(arg.principal, tg::Principal::All) && arg.permission != tg::Permission::Read {
			return Err(tg::error!("all grants may only be read"));
		}

		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					session
						.delete_tag_grant_inner_with_transaction(transaction, &arg)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the tag grant"))
	}

	async fn delete_tag_grant_inner_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: &tg::tag::grants::delete::Arg,
	) -> tg::Result<ControlFlow<Option<()>, crate::database::Error>> {
		self.authorize_tag_with_transaction(transaction, &arg.tag, tg::Permission::Admin)
			.await?;
		let Some(namespace_id) =
			Self::try_get_tag_namespace_id_with_transaction(transaction, &arg.tag).await?
		else {
			return Ok(ControlFlow::Break(None));
		};
		match &arg.principal {
			tg::Principal::All | tg::Principal::Root => {},
			tg::Principal::Group(group) => {
				if Self::try_get_group_with_transaction(transaction, &group.to_string())
					.await?
					.is_none()
				{
					return Ok(ControlFlow::Break(None));
				}
			},
			tg::Principal::User(user) => {
				if Self::try_get_user_with_transaction(transaction, &user.to_string())
					.await?
					.is_none()
				{
					return Ok(ControlFlow::Break(None));
				}
			},
		}
		match Self::delete_tag_grant_with_transaction(
			transaction,
			namespace_id,
			&arg.tag.name,
			&arg.principal,
			arg.permission,
		)
		.await?
		{
			ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
			ControlFlow::Continue(error) => Ok(ControlFlow::Continue(error)),
		}
	}

	async fn delete_tag_grant_remote(
		&self,
		mut arg: tg::tag::grants::delete::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self
			.get_remote_session(&remote.name)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					remote = %remote.name,
					"failed to get the remote client"
				)
			})?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client.delete_tag_grant(arg).await.map_err(|error| {
			tg::error!(
				!error,
				remote = %remote.name,
				"failed to delete the tag grant"
			)
		})
	}

	pub(crate) async fn delete_tag_grant_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("expected query params"))?;
		if self
			.delete_tag_grant(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the tag grant"))?
			.is_none()
		{
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		}
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
