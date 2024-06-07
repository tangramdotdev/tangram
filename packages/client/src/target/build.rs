use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(
		default = "crate::util::serde::true_",
		skip_serializing_if = "crate::util::serde::is_true"
	)]
	pub create: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::build::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	#[serde(default, skip_serializing_if = "retry_is_canceled")]
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

impl Default for Arg {
	fn default() -> Self {
		Self {
			create: true,
			parent: None,
			remote: None,
			retry: tg::build::Retry::default(),
		}
	}
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn retry_is_canceled(retry: &tg::build::Retry) -> bool {
	matches!(retry, tg::build::Retry::Canceled)
}
