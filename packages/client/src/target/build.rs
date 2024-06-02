use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub parent: Option<tg::build::Id>,
	pub remote: Option<String>,
	pub retry: tg::build::Retry,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub build: tg::build::Id,
}

impl tg::Target {
	pub async fn build<H>(&self, handle: &H, arg: tg::target::build::Arg) -> tg::Result<tg::Build>
	where
		H: tg::Handle,
	{
		let id = self.id(handle).await?;
		let output = handle.build_target(&id, arg).await?;
		let build = tg::Build::with_id(output.build);
		Ok(build)
	}

	pub async fn outcome<H>(
		&self,
		handle: &H,
		arg: tg::target::build::Arg,
	) -> tg::Result<tg::build::Outcome>
	where
		H: tg::Handle,
	{
		let build = self.build(handle, arg).await?;
		let outcome = build.outcome(handle).await?;
		Ok(outcome)
	}

	pub async fn output<H>(&self, handle: &H, arg: tg::target::build::Arg) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let build = self.build(handle, arg).await?;
		let output = build.output(handle).await?;
		Ok(output)
	}
}

impl tg::Client {
	pub async fn build_target(
		&self,
		id: &tg::target::Id,
		arg: tg::target::build::Arg,
	) -> tg::Result<tg::target::build::Output> {
		let method = http::Method::POST;
		let uri = format!("/targets/{id}/build");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.json(arg)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(output)
	}
}
