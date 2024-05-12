use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

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
	pub blob: tg::blob::Id,
}

impl tg::Blob {
	pub async fn compress<H>(&self, handle: &H, format: Format) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let id = self.id(handle, None).await?;
		let arg = Arg { format };
		let output = handle.compress_blob(&id, arg).await?;
		let blob = Self::with_id(output.blob);
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
