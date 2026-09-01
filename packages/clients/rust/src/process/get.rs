use {
	crate::prelude::*,
	serde_with::{DisplayFromStr, PickFirst, serde_as},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::is_false,
};

pub const AVAILABILITY_HEADER: &str = "x-tg-process-availability";
pub const METADATA_HEADER: &str = "x-tg-process-metadata";

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub availability: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
	#[serde(default, skip_serializing_if = "is_false")]
	pub metadata: bool,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	pub tokens: tg::authorization::Tokens,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub availability: Option<tg::process::Availability>,

	pub data: tg::process::Data,

	pub id: tg::process::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::Location>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub metadata: Option<tg::process::Metadata>,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	pub tokens: tg::authorization::Tokens,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub availability: bool,
	pub location: Option<tg::location::Arg>,
	pub metadata: bool,
}

impl<O> tg::Process<O> {
	pub async fn get(
		&self,
		options: tg::process::get::Options,
	) -> tg::Result<tg::process::get::Output> {
		let handle = tg::handle()?;
		self.get_with_handle(handle, options).await
	}

	pub async fn get_with_handle<H>(
		&self,
		handle: &H,
		options: tg::process::get::Options,
	) -> tg::Result<tg::process::get::Output>
	where
		H: tg::Handle,
	{
		self.try_get_with_handle(handle, options)
			.await?
			.ok_or_else(|| tg::error!("failed to get the process"))
	}

	pub async fn try_get(
		&self,
		options: tg::process::get::Options,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let handle = tg::handle()?;
		self.try_get_with_handle(handle, options).await
	}

	pub async fn try_get_with_handle<H>(
		&self,
		handle: &H,
		options: tg::process::get::Options,
	) -> tg::Result<Option<tg::process::get::Output>>
	where
		H: tg::Handle,
	{
		let Some(id) = self.id().right() else {
			return Err(tg::error!(
				"getting an unsandboxed process is not supported"
			));
		};
		let arg = tg::process::get::Arg {
			availability: options.availability,
			location: options.location.or_else(|| self.location()),
			metadata: options.metadata,
			tokens: self.tokens(),
		};
		let Some(output) = handle.try_get_process(id, arg).await? else {
			return Ok(None);
		};
		if let Some(location) = &output.location {
			self.0
				.location
				.write()
				.unwrap()
				.replace(location.clone().into());
		}
		if !output.tokens.is_empty() {
			*self.0.tokens.write().unwrap() = output.tokens.clone();
		}

		Ok(Some(output))
	}
}

impl tg::Session {
	pub async fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> tg::Result<Option<tg::process::get::Output>> {
		let method = http::Method::GET;
		let path = format!("/processes/{id}");
		let uri = Uri::builder()
			.path(&path)
			.query_params_strict(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let metadata = response
			.header_json(METADATA_HEADER)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to deserialize the metadata header"))?;
		let availability = response
			.header_json(AVAILABILITY_HEADER)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to deserialize the availability header"))?;
		let mut output = response
			.json::<tg::process::get::Output>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;
		if let Some(metadata) = metadata {
			output.metadata = Some(metadata);
		}
		if let Some(availability) = availability {
			output.availability = Some(availability);
		}
		Ok(Some(output))
	}
}
