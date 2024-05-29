use crate as tg;
use futures::{Stream, StreamExt as _};
use serde_with::serde_as;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(
	Clone, Copy, Debug, Eq, PartialEq, serde_with::DeserializeFromStr, serde_with::SerializeDisplay,
)]
pub enum Status {
	Created,
	Dequeued,
	Started,
	Finished,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<serde_with::DurationSeconds>")]
	pub timeout: Option<std::time::Duration>,
}

impl tg::Build {
	pub async fn status<H>(
		&self,
		handle: &H,
		arg: tg::build::status::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>
	where
		H: tg::Handle,
	{
		self.try_get_status(handle, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the build"))
	}

	pub async fn try_get_status<H>(
		&self,
		handle: &H,
		arg: tg::build::status::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>>
	where
		H: tg::Handle,
	{
		handle
			.try_get_build_status(self.id(), arg)
			.await
			.map(|option| option.map(futures::StreamExt::boxed))
	}
}

impl tg::Client {
	pub async fn try_get_build_status_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/builds/{id}/status?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.sse().map(|result| {
			let event = result.map_err(|source| tg::error!(!source, "failed to read an event"))?;
			match event.event.as_deref() {
				None | Some("data") => {
					let data = serde_json::from_str(&event.data)
						.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
					Ok(data)
				},
				Some("error") => {
					let error = serde_json::from_str(&event.data)
						.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
					Err(error)
				},
				_ => Err(tg::error!("invalid event")),
			}
		});
		Ok(Some(output))
	}
}

impl std::fmt::Display for Status {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Created => write!(f, "created"),
			Self::Dequeued => write!(f, "dequeued"),
			Self::Started => write!(f, "started"),
			Self::Finished => write!(f, "finished"),
		}
	}
}

impl std::str::FromStr for Status {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"created" => Ok(Self::Created),
			"dequeued" => Ok(Self::Dequeued),
			"started" => Ok(Self::Started),
			"finished" => Ok(Self::Finished),
			status => Err(tg::error!(%status, "invalid value")),
		}
	}
}
