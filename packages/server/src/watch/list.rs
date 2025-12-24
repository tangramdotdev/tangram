use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub(crate) async fn list_watches_with_context(
		&self,
		context: &Context,
		_arg: tg::watch::list::Arg,
	) -> tg::Result<tg::watch::list::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		let data = self
			.watches
			.iter()
			.map(|entry| tg::watch::list::Item {
				path: entry.key().clone(),
			})
			.collect();
		let output = tg::watch::list::Output { data };
		Ok(output)
	}

	pub(crate) async fn handle_list_watches_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();
		let output = self
			.list_watches_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to list the watches"))?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
