use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
};

impl Server {
	pub(crate) async fn post_process_signal_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> tg::Result<()> {
		// Forward to remote if requested.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self.get_remote_client(remote).await?;
			let arg = tg::process::signal::post::Arg {
				local: None,
				remotes: None,
				signal: arg.signal,
			};
			return client.post_process_signal(id, arg).await;
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

	pub(crate) async fn handle_post_process_signal_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse process id"))?;
		let arg = request
			.json()
			.await
			.map_err(|_| tg::error!("failed to deserialize the arg"))?;
		self.post_process_signal_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to post process signal"))?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
