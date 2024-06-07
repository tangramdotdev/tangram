use crate::Server;
use futures::{stream, Stream};
use tangram_client as tg;

impl Server {
	pub async fn try_read_blob_stream(
		&self,
		_id: &tg::blob::Id,
		_arg: tg::blob::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::blob::read::Event>>>> {
		Ok(Some(stream::empty()))
	}
}
