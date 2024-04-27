use crate::{self as tg, util::http::full};
use http_body_util::BodyExt as _;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub format: Format,
}

#[derive(Clone, Copy, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub enum Format {
	Tar,
	Zip,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub id: tg::blob::Id,
}

impl tg::Artifact {
	pub async fn archive<H>(&self, handle: &H, format: Format) -> tg::Result<tg::Blob>
	where
		H: tg::Handle,
	{
		let id = self.id(handle, None).await?;
		let arg = Arg { format };
		let output = handle.archive_artifact(&id, arg).await?;
		let blob = tg::Blob::with_id(output.id);
		Ok(blob)
	}
}

impl tg::Client {
	pub async fn archive_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::archive::Arg,
	) -> tg::Result<tg::artifact::archive::Output> {
		let method = http::Method::POST;
		let uri = format!("/artifacts/{id}/archive");
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
		match self {
			Self::Tar => {
				write!(f, "tar")?;
			},
			Self::Zip => {
				write!(f, "zip")?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for Format {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"tar" => Ok(Self::Tar),
			"zip" => Ok(Self::Zip),
			extension => Err(tg::error!(%extension, "invalid format")),
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
