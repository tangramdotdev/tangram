use crate::{self as tg, util::http::full};
use http_body_util::BodyExt as _;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	pub outcome: tg::build::outcome::Data,
}

impl tg::Build {
	pub async fn finish<H>(&self, handle: &H, arg: tg::build::finish::Arg) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		let id = self.id();
		handle.finish_build(id, arg).await?;
		Ok(())
	}
}

impl tg::Client {
	pub async fn finish_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::finish::Arg,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/outcome");
		let mut request = http::request::Builder::default().method(method).uri(uri);
		if let Some(token) = self.token.as_ref() {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = serde_json::to_vec(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);
		let request = request
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
		Ok(())
	}
}
