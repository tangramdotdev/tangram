use {
	crate::Server,
	tangram_client as tg,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn list_watches(
		&self,
		_arg: tg::watch::list::Arg,
	) -> tg::Result<tg::watch::list::Output> {
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

	pub(crate) async fn handle_list_watches_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = handle.list_watches(arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
