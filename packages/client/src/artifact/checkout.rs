use http_body_util::BodyExt as _;

use crate::{self as tg, util::http::full};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub bundle: bool,
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub force: bool,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<tg::Path>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub path: tg::Path,
}

impl tg::Artifact {
	pub async fn check_out<H>(&self, handle: &H, arg: Arg) -> tg::Result<Output>
	where
		H: tg::Handle,
	{
		let id = self.id(handle, None).await?;
		let output = handle.check_out_artifact(&id, arg).await?;
		Ok(output)
	}
}

impl tg::Client {
	pub async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> tg::Result<tg::artifact::checkout::Output> {
		let method = http::Method::POST;
		let uri = format!("/artifacts/{id}/checkout");
		let body = serde_json::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		Ok(output)
	}
}
