use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _},
	tangram_messenger::prelude::*,
	tangram_store::prelude::*,
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
			.try_get_process_local(id, false)
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
		let arg = tangram_store::PutProcessLogArg {
			bytes: arg.bytes,
			process: id.clone(),
			stream: arg.stream,
			timestamp,
		};
		self.store
			.put_process_log(arg)
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
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Post the process log.
		self.post_process_log_with_context(context, &id, arg)
			.await?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::APPLICATION, mime::JSON)) => (),
			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		}

		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.body(Body::empty())
			.unwrap();
		Ok(response)
	}
}
