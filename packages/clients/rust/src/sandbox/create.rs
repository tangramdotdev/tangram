use {
	crate::prelude::*,
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::time::Duration,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cpu: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub host: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub hostname: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub isolation: Option<tg::sandbox::Isolation>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub memory: Option<u64>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub mounts: Vec<tg::sandbox::Mount>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub network: Option<tg::sandbox::Network>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub owner: Option<tg::Principal>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	pub ttl: Option<Duration>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::sandbox::Id,
}

impl tg::Sandbox {
	pub async fn create() -> tg::Result<Self> {
		Self::builder().build().await
	}

	pub async fn create_with_handle<H>(handle: &H) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		Self::builder().build_with_handle(handle).await
	}

	pub async fn create_with_arg(arg: tg::sandbox::create::Arg) -> tg::Result<Self> {
		let handle = tg::handle()?;
		Self::create_with_arg_with_handle(handle, arg).await
	}

	pub async fn create_with_arg_with_handle<H>(
		handle: &H,
		mut arg: tg::sandbox::create::Arg,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		arg.host
			.get_or_insert_with(|| tg::host::current().to_owned());
		let location = arg.location.clone();
		let output = handle.create_sandbox(arg).await?;
		let handle = tg::handle::dynamic::Handle::new(handle.clone());
		let options = tg::sandbox::Options {
			location,
			state: None,
			tokens: tg::authorization::Tokens::default(),
		};
		let sandbox = Self::new_inner(output.id, options, Some(handle));

		Ok(sandbox)
	}
}

impl tg::Session {
	pub async fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		let method = http::Method::POST;
		let uri = "/sandboxes".to_owned();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.json(arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;
		Ok(output)
	}
}
