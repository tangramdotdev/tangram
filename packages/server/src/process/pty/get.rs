use crate::Server;
use futures::{stream, Stream};
use tangram_client as tg;
use tangram_http::{Incoming, Outgoing};

impl Server {
	pub(crate) async fn try_get_process_pty_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::pty::get::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::process::pty::get::Event>> + Send + 'static>
	{
		Ok::<_, tg::Error>(stream::empty())
	}
}

impl Server {
	pub(crate) async fn handle_try_get_process_pty_stream_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>> {
		todo!()
	}
}
