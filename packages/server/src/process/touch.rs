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
	pub(crate) async fn touch_process_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::process::touch::Arg {
				local: None,
				remotes: None,
			};
			client.touch_process(id, arg).await.map_err(
				|source| tg::error!(!source, %id, "failed to touch the process on the remote"),
			)?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.touch_process_postgres(database, id)
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to touch the process"))?;
			},
			#[cfg(feature = "sqlite")]
			crate::index::Index::Sqlite(database) => {
				self.touch_process_sqlite(database, id)
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to touch the process"))?;
			},
		}

		Ok(())
	}

	pub(crate) async fn handle_touch_process_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		self.touch_process_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to touch the process"))?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
