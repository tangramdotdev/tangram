use crate as tg;
use futures::{stream, Stream};
use std::pin::Pin;
use tangram_either::Either;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	remote: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Event {
	Complete(Either<tg::process::Id, tg::object::Id>),
	End,
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
