use {
	crate::prelude::*,
	bytes::{Buf as _, Bytes},
	serde_with::{DisplayFromStr, PickFirst, serde_as},
	std::collections::BTreeMap,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::is_false,
};

#[cfg(test)]
mod tests;

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

#[derive(Clone, Debug)]
pub struct Output {
	pub availability: Option<tg::object::Availability>,
	pub bytes: Bytes,
	pub children: BTreeMap<tg::object::Id, Child>,
	pub metadata: Option<tg::object::Metadata>,
	pub tokens: tg::authorization::Tokens,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Child {
	#[serde(default, skip_serializing_if = "tg::authorization::Tokens::is_empty")]
	#[tangram_serialize(
		default,
		id = 0,
		skip_serializing_if = "tg::authorization::Tokens::is_empty"
	)]
	pub tokens: tg::authorization::Tokens,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub location: Option<tg::location::Arg>,
}

#[derive(tangram_serialize::Deserialize, tangram_serialize::Serialize)]
struct Header {
	#[tangram_serialize(id = 0)]
	availability: Option<tg::object::Availability>,
	#[tangram_serialize(id = 1)]
	children: BTreeMap<tg::object::Id, Child>,
	#[tangram_serialize(id = 2)]
	metadata: Option<tg::object::Metadata>,
	#[tangram_serialize(id = 3)]
	size: usize,
	#[tangram_serialize(id = 4)]
	tokens: tg::authorization::Tokens,
}

impl Output {
	pub fn serialize(self) -> tg::Result<[Bytes; 2]> {
		// Frame the response with a Tangram header followed by the original object bytes.
		let header = Header {
			availability: self.availability,
			children: self.children,
			metadata: self.metadata,
			size: self.bytes.len(),
			tokens: self.tokens,
		};
		let header = tangram_serialize::to_vec(&header)
			.map_err(|error| tg::error!(!error, "failed to serialize the object get header"))?;
		let chunks = [header.into(), self.bytes];

		Ok(chunks)
	}

	pub fn deserialize(mut bytes: Bytes) -> tg::Result<Self> {
		// Decode the header and retain a slice of the response for the object bytes.
		let mut deserializer = tangram_serialize::Deserializer::new(&bytes);
		let header = deserializer
			.deserialize::<Header>()
			.map_err(|error| tg::error!(!error, "failed to deserialize the object get header"))?;
		let position = deserializer.position();
		bytes.advance(position);
		if bytes.len() != header.size {
			return Err(tg::error!("invalid object get body size"));
		}
		let output = Self {
			availability: header.availability,
			bytes,
			children: header.children,
			metadata: header.metadata,
			tokens: header.tokens,
		};

		Ok(output)
	}
}

impl tg::Session {
	pub async fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::get::Output>> {
		let method = http::Method::GET;
		let path = format!("/objects/{id}");
		let uri = Uri::builder().path(&path).build().unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(
				http::header::ACCEPT,
				mime::APPLICATION_OCTET_STREAM.to_string(),
			)
			.empty()
			.unwrap();
		let request = tangram_http::request::with_query_params(request, &arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?;
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
		let bytes = response
			.bytes()
			.await
			.map_err(|error| tg::error!(!error, "failed to read the response body"))?;
		let output = Output::deserialize(bytes)?;
		Ok(Some(output))
	}
}
