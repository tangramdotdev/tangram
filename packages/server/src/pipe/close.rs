use crate::Server;
use tangram_client as tg;
use tangram_either::Either;
use tangram_http::{incoming::request::Ext, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn close_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::close::Arg) -> tg::Result<()> {
		eprintln!("closing {id}");
		if let Some(remote) = arg.remote {
			let remote = self.get_remote_client(remote).await?;
			return remote.close_pipe(id, tg::pipe::close::Arg::default()).await;
		}
		let pipe = self
			.pipes
			.get(id)
			.ok_or_else(|| tg::error!("failed to find the pipe"))?;
		let remove = match pipe.as_ref() {
			Either::Left(sender) => {
				if sender.release() == 0 {
					sender.sender.close();
					true
				} else {
					false
				}
			},
			Either::Right(receiver) => receiver.release() == 0,
		};
		drop(pipe);
		eprintln!("close(id: {id}, remove: {remove}");
		if remove {
			self.pipes.remove(id);
			eprintln!("removed {id}");
		}
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_close_pipe_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		handle.close_pipe(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
