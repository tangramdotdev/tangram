use crate as tg;
use bytes::Bytes;
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
pub struct Event {
	pub id: Either<tg::process::Id, tg::object::Id>,
	pub bytes: Bytes,
}

impl tg::Client {
	pub async fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Event>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::export::Event>> + Send + 'static> {
		Ok(stream::empty())
	}
}
