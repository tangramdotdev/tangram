use crate::Server;
use futures::{stream, Stream};
use std::pin::Pin;
use tangram_client as tg;

impl Server {
	pub async fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Event>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::export::Event>> + Send + 'static> {
		Ok(stream::empty())
	}
}
