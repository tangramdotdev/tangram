use {
	crate::prelude::*,
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::{sync::Arc, time::Duration},
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::{is_default, is_false},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "is_false")]
	pub cached: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,

	#[serde(default, skip_serializing_if = "is_default")]
	pub ttl: tg::remote::cache::Ttl,
}

#[serde_as]
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Output {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub cpu: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub creator: Option<tg::Principal>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub hostname: Option<String>,

	#[tangram_serialize(id = 3)]
	pub id: tg::sandbox::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "Option::is_none")]
	pub isolation: Option<tg::sandbox::Isolation>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::Location>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "Option::is_none")]
	pub memory: Option<u64>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 7, skip_serializing_if = "Vec::is_empty")]
	pub mounts: Vec<tg::sandbox::Mount>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 8, skip_serializing_if = "Option::is_none")]
	pub network: Option<tg::sandbox::Network>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 9, skip_serializing_if = "Option::is_none")]
	pub owner: Option<tg::Principal>,

	#[tangram_serialize(id = 10)]
	pub status: tg::sandbox::Status,

	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	#[tangram_serialize(
		default,
		id = 11,
		skip_serializing_if = "tg::authorization::Tokens::is_empty"
	)]
	pub tokens: tg::authorization::Tokens,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[tangram_serialize(
		deserialize_with = "deserialize_duration",
		id = 12,
		serialize_with = "serialize_duration"
	)]
	pub ttl: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 13, skip_serializing_if = "Option::is_none")]
	pub usage: Option<Usage>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Usage {
	#[tangram_serialize(id = 0)]
	pub cpu: u64,

	#[tangram_serialize(id = 1)]
	pub memory: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub cached: bool,
	pub ttl: tg::remote::cache::Ttl,
}

impl tg::Sandbox {
	pub async fn get(
		&self,
		options: tg::sandbox::get::Options,
	) -> tg::Result<Arc<tg::sandbox::get::Output>> {
		let handle = tg::handle()?;
		self.get_with_handle(handle, options).await
	}

	pub async fn get_with_handle<H>(
		&self,
		handle: &H,
		options: tg::sandbox::get::Options,
	) -> tg::Result<Arc<tg::sandbox::get::Output>>
	where
		H: tg::Handle,
	{
		self.try_get_with_handle(handle, options)
			.await?
			.ok_or_else(|| tg::error!("failed to get the sandbox"))
	}

	pub async fn try_get(
		&self,
		options: tg::sandbox::get::Options,
	) -> tg::Result<Option<Arc<tg::sandbox::get::Output>>> {
		let handle = tg::handle()?;
		self.try_get_with_handle(handle, options).await
	}

	pub async fn try_get_with_handle<H>(
		&self,
		handle: &H,
		options: tg::sandbox::get::Options,
	) -> tg::Result<Option<Arc<tg::sandbox::get::Output>>>
	where
		H: tg::Handle,
	{
		if let Some(state) = self.0.state.read().unwrap().clone() {
			return Ok(Some(state));
		}
		let arg = tg::sandbox::get::Arg {
			cached: options.cached,
			location: self.location(),
			ttl: options.ttl,
		};
		let Some(output) = handle.try_get_sandbox(self.id(), arg).await? else {
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
		let state = Arc::new(output);
		self.0.state.write().unwrap().replace(state.clone());

		Ok(Some(state))
	}

	pub async fn load(&self) -> tg::Result<Arc<tg::sandbox::get::Output>> {
		self.get(tg::sandbox::get::Options::default()).await
	}

	pub async fn load_with_handle<H>(&self, handle: &H) -> tg::Result<Arc<tg::sandbox::get::Output>>
	where
		H: tg::Handle,
	{
		self.get_with_handle(handle, tg::sandbox::get::Options::default())
			.await
	}

	pub async fn try_load(&self) -> tg::Result<Option<Arc<tg::sandbox::get::Output>>> {
		self.try_get(tg::sandbox::get::Options::default()).await
	}

	pub async fn try_load_with_handle<H>(
		&self,
		handle: &H,
	) -> tg::Result<Option<Arc<tg::sandbox::get::Output>>>
	where
		H: tg::Handle,
	{
		self.try_get_with_handle(handle, tg::sandbox::get::Options::default())
			.await
	}
}

fn deserialize_duration(
	deserializer: &mut tangram_serialize::Deserializer<'_>,
) -> std::io::Result<Option<Duration>> {
	let value = deserializer.deserialize::<Option<(u64, u32)>>()?;
	value
		.map(|(seconds, nanoseconds)| {
			if nanoseconds >= 1_000_000_000 {
				return Err(std::io::Error::other("invalid duration nanoseconds"));
			}
			Ok(Duration::new(seconds, nanoseconds))
		})
		.transpose()
}

#[expect(clippy::ref_option)]
fn serialize_duration(
	value: &Option<Duration>,
	serializer: &mut tangram_serialize::Serializer<'_>,
) -> std::io::Result<()> {
	let value = value.map(|value| (value.as_secs(), value.subsec_nanos()));
	serializer.serialize(&value)
}

impl tg::Session {
	pub async fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		let method = http::Method::GET;
		let path = format!("/sandboxes/{id}");
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
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;
		Ok(Some(output))
	}
}
