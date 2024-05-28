use crate::Server;
use futures::{stream, Stream, TryStreamExt as _};
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn pull_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::pull::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::build::Progress>> + Send + 'static> {
		Ok(stream::empty())
	}
}

impl Server {
	pub(crate) async fn handle_pull_build_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let stream = handle.pull_build(&id, arg).await?;
		let sse = stream.map_ok(|event| {
			let data = serde_json::to_string(&event).unwrap();
			tangram_http::sse::Event::with_data(data)
		});
		let body = Outgoing::sse(sse);
		let response = http::Response::builder().ok().body(body).unwrap();
		Ok(response)
	}
}
