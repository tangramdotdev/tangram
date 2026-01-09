use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub async fn touch_object_with_context(
		&self,
		_context: &Context,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::object::touch::Arg {
				local: None,
				remotes: None,
			};
			client.touch_object(id, arg).await.map_err(
				|source| tg::error!(!source, %id, "failed to touch the object on the remote"),
			)?;
			return Ok(());
		}

		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.touch_object_postgres(database, id)
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to touch the object"))?;
			},
			#[cfg(feature = "sqlite")]
			crate::index::Index::Sqlite(database) => {
				self.touch_object_sqlite(database, id)
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to touch the object"))?;
			},
		}

		Ok(())
	}

	pub(crate) async fn handle_touch_object_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the object id"))?;
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		self.touch_object_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to touch the object"))?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
