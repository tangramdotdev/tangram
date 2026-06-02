use {
	crate::{Database, Session, context::Authentication},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

impl Session {
	pub(crate) async fn try_delete_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<()>> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		self.authorize_namespace(namespace, tg::Permission::Admin)
			.await?;
		match &self.server.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				self.try_delete_namespace_postgres(database, namespace)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => self.try_delete_namespace_sqlite(database, namespace).await,
			#[cfg(feature = "turso")]
			Database::Turso(database) => self.try_delete_namespace_turso(database, namespace).await,
		}
	}

	pub(crate) async fn try_delete_namespace_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg: tg::namespace::delete::Arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("expected query params"))?;
		if self
			.try_delete_namespace(&arg.namespace)
			.await
			.map_err(
				|error| tg::error!(!error, namespace = %arg.namespace, "failed to delete the namespace"),
			)?
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
