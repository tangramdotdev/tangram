use {
	crate::{Context, Server},
	bytes::Bytes,
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
	tokio::io::AsyncWriteExt as _,
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
			let client = self.get_remote_client(remote).await?;
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
			.await?
			.ok_or_else(|| tg::error!("not found"))?
			.data;

		// Verify the process is local and started.
		if data.status != tg::process::Status::Started {
			return Err(tg::error!("failed to find the process"));
		}

		// Write to the log file.
		self.post_process_log_to_file(id, arg.bytes.clone()).await?;

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

	async fn post_process_log_to_file(&self, id: &tg::process::Id, bytes: Bytes) -> tg::Result<()> {
		let path = self.logs_path().join(format!("{id}"));
		let mut file = tokio::fs::File::options()
			.create(true)
			.append(true)
			.open(&path)
			.await
			.map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to open the log file"),
			)?;
		file.write_all(&bytes).await.map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to write to the log file"),
		)?;
		Ok(())
	}

	pub(crate) async fn handle_post_process_log_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id.parse()?;
		let arg = request.json().await?;
		self.post_process_log_with_context(context, &id, arg)
			.await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
