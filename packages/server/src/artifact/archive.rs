use crate::{
	util::http::{bad_request, full, Incoming, Outgoing},
	Server,
};
use http_body_util::BodyExt as _;
use tangram_client as tg;

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
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["artifacts", id, "archive"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the body"))?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;

		// Archive the artifact.
		let output = handle.archive_artifact(&id, arg).await?;

		// Create the response.
		let body = serde_json::to_vec(&output)
			.map_err(|source| tg::error!(!source, "failed to serialize the response"))?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}
}
