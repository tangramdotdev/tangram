use {
	crate::{Session, context::Authentication},
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Session {
	pub(crate) async fn create_namespace(&self, namespace: &tg::Namespace) -> tg::Result<()> {
		let created_by = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_user_ref().ok())
			.map(|user| user.id.clone());
		match Self::parent_namespace(namespace) {
			Some(parent) => {
				if self
					.context
					.authentication
					.as_ref()
					.is_some_and(Authentication::is_process)
				{
					return Err(tg::error!("unauthorized"));
				}
				self.authorize_namespace(&parent, tg::Permission::Write)
					.await?;
			},
			None => match &self.context.authentication {
				Some(Authentication::Root | Authentication::User(_)) => {},
				_ => return Err(tg::error!("unauthorized")),
			},
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
		let namespace_id =
			Self::get_or_create_namespace_with_transaction(&transaction, namespace).await?;
		if let Some(user) = created_by.as_ref() {
			Self::create_namespace_grant_for_user_with_transaction(
				&transaction,
				namespace,
				namespace_id,
				user,
				tg::Permission::Admin,
				created_by.as_ref(),
			)
			.await?;
		}
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub(crate) async fn create_namespace_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg: tg::namespace::create::Arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		self.create_namespace(&arg.namespace).await.map_err(
			|error| tg::error!(!error, namespace = %arg.namespace, "failed to create the namespace"),
		)?;
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
