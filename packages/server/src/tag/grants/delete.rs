use {
	super::Grantee,
	crate::{Session, context::Authentication},
	tangram_client::prelude::*,
	tangram_database::prelude::*,
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
		let grantee = Self::tag_grantee(arg.user, arg.group, arg.all)?;
		if matches!(grantee, Grantee::All) && arg.permission != tg::Permission::Read {
			return Err(tg::error!("all grants may only be read"));
		}

		let mut connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		self.authorize_tag_with_transaction(&transaction, &arg.tag, tg::Permission::Admin)
			.await?;
		let Some(namespace_id) =
			Self::try_get_tag_namespace_id_with_transaction(&transaction, &arg.tag).await?
		else {
			return Ok(None);
		};
		let output = match grantee {
			Grantee::User(user) => {
				let Some(user) = Self::try_get_user_with_transaction(&transaction, &user).await?
				else {
					return Ok(None);
				};
				Self::delete_tag_grant_for_user_with_transaction(
					&transaction,
					namespace_id,
					&arg.tag.name,
					&user.id,
					arg.permission,
				)
				.await?
			},
			Grantee::Group(group) => {
				let Some(group) =
					Self::try_get_group_with_transaction(&transaction, &group).await?
				else {
					return Ok(None);
				};
				Self::delete_tag_grant_for_group_with_transaction(
					&transaction,
					namespace_id,
					&arg.tag.name,
					&group.id,
					arg.permission,
				)
				.await?
			},
			Grantee::All => {
				Self::delete_tag_grant_for_all_with_transaction(
					&transaction,
					namespace_id,
					&arg.tag.name,
				)
				.await?
			},
		};
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(output)
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
