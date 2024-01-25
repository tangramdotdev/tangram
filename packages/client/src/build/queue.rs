use crate as tg;
use crate::Client;
use http_body_util::BodyExt;
use itertools::Itertools;
use tangram_error::{error, Result, WrapErr};
use tangram_util::http::full;

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct DequeueArg {
	#[serde(
		deserialize_with = "deserialize_dequeue_arg_hosts",
		serialize_with = "serialize_dequeue_arg_hosts"
	)]
	pub hosts: Option<Vec<tg::System>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DequeueOutput {
	pub build: tg::build::Id,
	pub options: tg::build::Options,
}

impl Client {
	pub async fn try_dequeue_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::queue::DequeueArg,
	) -> Result<Option<tg::build::queue::DequeueOutput>> {
		let method = http::Method::POST;
		let uri = "/builds/dequeue";
		let mut request = http::request::Builder::default().method(method).uri(uri);
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = serde_json::to_vec(&arg).wrap_err("Failed to serialize the body.")?;
		let body = full(body);
		let request = request
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("The request did not succeed."));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let item =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		Ok(item)
	}
}

fn serialize_dequeue_arg_hosts<S>(
	value: &Option<Vec<tg::System>>,
	serializer: S,
) -> Result<S::Ok, S::Error>
where
	S: serde::Serializer,
{
	match value {
		Some(hosts) => serializer.serialize_str(&hosts.iter().join(",")),
		None => serializer.serialize_unit(),
	}
}

fn deserialize_dequeue_arg_hosts<'de, D>(
	deserializer: D,
) -> Result<Option<Vec<tg::System>>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	struct Visitor;

	impl<'de> serde::de::Visitor<'de> for Visitor {
		type Value = Option<Vec<tg::System>>;

		fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
			formatter.write_str("a string with comma-separated values or null")
		}

		fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
		where
			E: serde::de::Error,
		{
			let values = value
				.split(',')
				.map(std::str::FromStr::from_str)
				.try_collect()
				.map_err(|_| serde::de::Error::custom("invalid system"))?;
			Ok(Some(values))
		}

		fn visit_none<E>(self) -> Result<Self::Value, E>
		where
			E: serde::de::Error,
		{
			Ok(None)
		}
	}

	deserializer.deserialize_any(Visitor)
}
