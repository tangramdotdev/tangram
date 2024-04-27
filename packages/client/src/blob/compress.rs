use crate::{self as tg, util::http::full};
use http_body_util::BodyExt as _;

#[derive(Clone, Copy, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub enum Format {
	Bz2,
	Gz,
	Xz,
	Zstd,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub format: Format,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::blob::Id,
}

impl tg::Blob {
	pub async fn compress<H>(&self, handle: &H, format: Format) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let id = self.id(handle, None).await?;
		let arg = Arg { format };
		let output = handle.compress_blob(&id, arg).await?;
		let blob = Self::with_id(output.id);
		Ok(blob)
	}
}

impl tg::Client {
	pub async fn compress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::compress::Arg,
	) -> tg::Result<tg::blob::compress::Output> {
		let method = http::Method::POST;
		let uri = format!("/blobs/{id}/compress");
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

impl std::fmt::Display for Format {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let s = match self {
			Self::Bz2 => "bz2",
			Self::Gz => "gz",
			Self::Xz => "xz",
			Self::Zstd => "zst",
		};
		write!(f, "{s}")?;
		Ok(())
	}
}

impl std::str::FromStr for Format {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"bz2" => Ok(Self::Bz2),
			"gz" => Ok(Self::Gz),
			"xz" => Ok(Self::Xz),
			"zst" => Ok(Self::Zstd),
			extension => Err(tg::error!(%extension, "invalid compression format")),
		}
	}
}

impl From<Format> for String {
	fn from(value: Format) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Format {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self, Self::Error> {
		value.parse()
	}
}
