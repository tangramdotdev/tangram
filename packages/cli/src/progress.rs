use crate::Cli;
use futures::{stream::TryStreamExt as _, Stream};
use std::pin::pin;
use tangram_client as tg;

impl Cli {
	pub async fn drain_progress_stream<T>(
		&self,
		mut stream: impl Stream<Item = tg::Result<tg::Progress<T>>>,
	) -> tg::Result<T> {
		let mut stream = pin!(stream);
		// TODO: spin while waiting for the response from the server.
		let Some(tg::Progress::Begin) = stream.try_next().await? else {
			return Err(tg::error!("invalid stream"));
		};
		while let Some(progress) = stream.try_next().await? {
			match progress {
				tg::Progress::Begin => return Err(tg::error!("invalid stream")),
				tg::Progress::Report(_report) => {
					todo!()
				},
				tg::Progress::End(value) => return Ok(value),
			}
		}
		Err(tg::error!("stream closed early"))
	}
}
