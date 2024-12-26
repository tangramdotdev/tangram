use crate::Server;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn list_remotes(
		&self,
		_arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		let items = self
			.remotes
			.iter()
			.map(|entry| {
				let name = entry.key().to_owned();
				let url = entry.value().url().clone();
				tg::remote::get::Output { name, url }
			})
			.collect();
		let output = tg::remote::list::Output { data: items };
		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_list_remotes_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = handle.list_remotes(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
