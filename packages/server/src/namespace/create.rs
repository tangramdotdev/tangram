use {
	crate::{Database, Session},
	tangram_client::prelude::*,
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
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		match &self.server.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				self.create_namespace_postgres(database, namespace).await?;
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => {
				self.create_namespace_sqlite(database, namespace).await?;
			},
		}
		Ok(())
	}

	pub(crate) async fn create_namespace_request(
		&self,
		request: http::Request<BoxBody>,
		namespace: &[&str],
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let namespace: tg::Namespace = namespace
			.join("/")
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the namespace"))?;
		self.create_namespace(&namespace)
			.await
			.map_err(|error| tg::error!(!error, %namespace, "failed to create the namespace"))?;
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
