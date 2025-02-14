use crate as tg;
use futures::{stream, Stream};
use std::pin::Pin;
use tangram_either::Either;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	items: Vec<Either<tg::process::Id, tg::object::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	remote: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Event {
	Progress(tg::progress::Event<()>),
	Complete(Either<tg::process::Id, tg::object::Id>),
}

impl tg::Client {
	pub async fn import(
		&self,
		arg: tg::import::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::export::Event>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::import::Event>> + Send + 'static> {
		Ok(stream::empty())
	}
}

impl TryFrom<Event> for tangram_http::sse::Event {
	type Error = tg::Error;

	fn try_from(value: Event) -> Result<Self, Self::Error> {
		let event = match value {
			Event::Progress(data) => data.try_into()?,
			Event::Complete(data) => {
				let data = serde_json::to_string(&data)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
				tangram_http::sse::Event {
					event: Some("complete".to_owned()),
					data,
					..Default::default()
				}
			},
		};
		Ok(event)
	}
}

impl TryFrom<tangram_http::sse::Event> for Event {
	type Error = tg::Error;

	fn try_from(value: tangram_http::sse::Event) -> tg::Result<Self> {
		match value.event.as_deref() {
			Some("complete") => {
				let data = serde_json::from_str(&value.data)
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Ok(Self::Complete(data))
			},
			_ => Ok(Self::Progress(value.try_into()?)),
		}
	}
}
