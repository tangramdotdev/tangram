use {
	crate::prelude::*,
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::time::Duration,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,
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

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[tangram_serialize(
		deserialize_with = "deserialize_duration",
		id = 11,
		serialize_with = "serialize_duration"
	)]
	pub ttl: Option<Duration>,
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
