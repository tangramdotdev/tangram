use crate::{util, Server};
use futures::Stream;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn pull_build(
		&self,
		build: &tg::build::Id,
		arg: tg::build::pull::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static> {
		let remote = self
			.remotes
			.get(&arg.remote)
			.ok_or_else(|| tg::error!("failed to find the remote"))?
			.clone();
		Self::push_or_pull_build(&remote, self, build, arg).await
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
		let arg = request.json().await?;
		let stream = handle.pull_build(&id, arg).await?;
		Ok(util::progress::sse(stream))
	}
}
