use crate::Server;
use tangram_client as tg;
use tangram_http::{incoming::RequestExt as _, Incoming, Outgoing};

impl Server {
	pub async fn archive_artifact(
		&self,
		_id: &tg::artifact::Id,
		_arg: tg::artifact::archive::Arg,
	) -> tg::Result<tg::artifact::archive::Output> {
		Err(tg::error!("unimplemented"))
	}
}

impl Server {
	pub(crate) async fn handle_archive_artifact_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.archive_artifact(&id, arg).await?;
		let response = http::Response::builder()
			.body(Outgoing::json(output))
			.unwrap();
		Ok(response)
	}
}
