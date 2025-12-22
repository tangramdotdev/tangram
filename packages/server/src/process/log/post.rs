use {
	crate::{Context, Server},
	bytes::Bytes,
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
	tangram_store::Store,
};

impl Server {
	pub(crate) async fn post_process_log_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> tg::Result<()> {
		// Forward to remote if requested.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::process::log::post::Arg {
				bytes: arg.bytes,
				local: None,
				remotes: None,
				stream: arg.stream,
			};
			client.post_process_log(id, arg).await?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Get the process data.
		let data = self
			.try_get_process_local(id)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?
			.ok_or_else(|| tg::error!("not found"))?
			.data;

		// Compute the timestamp.
		let timestamp = time::OffsetDateTime::now_utc().unix_timestamp()
			- data
				.started_at
				.ok_or_else(|| tg::error!("expected the process to be started"))?;

		// Verify the process is local and started.
		if data.status != tg::process::Status::Started {
			return Err(tg::error!("failed to find the process"));
		}

		// Write to the store.
		let arg = tangram_store::PutLogArg {
			bytes: arg.bytes,
			process: id.clone(),
			stream: arg.stream,
			timestamp,
		};
		self.store
			.put_log(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the log"))?;

		// Publish the message.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				server
					.messenger
					.publish(format!("processes.{id}.log"), ())
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});

		Ok(())
	}

	pub(crate) async fn handle_post_process_log_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		self.post_process_log_with_context(context, &id, arg)
			.await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
