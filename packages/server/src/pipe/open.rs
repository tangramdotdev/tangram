use std::sync::atomic::AtomicUsize;

use crate::Server;
use tangram_client as tg;
use tangram_either::Either;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn open_pipe(&self, arg: tg::pipe::open::Arg) -> tg::Result<tg::pipe::open::Output> {
		if let Some(remote) = arg.remote {
			let remote = self.get_remote_client(remote).await?;
			return remote.open_pipe(tg::pipe::open::Arg::default()).await;
		}

		const CAPACITY: usize = 8;
		let writer_id = tg::pipe::Id::new();
		let reader_id = tg::pipe::Id::new();
		let (sender, receiver) = async_channel::bounded(CAPACITY);
		let writer = super::write::Writer {
			sender,
			refcount: AtomicUsize::new(1),
		};
		let reader = super::read::Reader {
			receiver,
			refcount: AtomicUsize::new(1),
		};
		self.pipes.insert(writer_id.clone(), Either::Left(writer));
		self.pipes.insert(reader_id.clone(), Either::Right(reader));
		eprintln!("inserted {writer_id}");
		eprintln!("inserted {reader_id}");
		let output = tg::pipe::open::Output {
			reader: reader_id,
			writer: writer_id,
		};

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_open_pipe_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = handle.open_pipe(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
