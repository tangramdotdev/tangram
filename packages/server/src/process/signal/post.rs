use {
	crate::Server,
	tangram_client as tg,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
};

impl Server {
	pub(crate) async fn post_process_signal(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::signal::post::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			return remote.post_process_signal(id, arg).await;
		}

		// Check if the process is cacheable.
		if tg::Process::new(id.clone(), None, None, None, None)
			.load(self)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to load the process"))?
			.cacheable
		{
			return Err(tg::error!(%id, "cannot signal cacheable processes"));
		}

		// Publish the signal message.
		let payload = serde_json::to_vec(&tg::process::signal::get::Event::Signal(arg.signal))
			.unwrap()
			.into();
		self.messenger
			.publish(format!("processes.{id}.signal"), payload)
			.await
			.map_err(|source| tg::error!(!source, "failed to signal the process"))?;

		Ok(())
	}

	pub(crate) async fn handle_post_process_signal_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse process id"))?;
		let arg = request
			.json()
			.await
			.map_err(|_| tg::error!("failed to deserialize the arg"))?;
		handle
			.signal_process(&id, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to post process signal"))?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
