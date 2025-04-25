use crate::Server;
use bytes::Bytes;
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::Messenger as _;
use tokio::io::AsyncWriteExt as _;

impl Server {
	pub async fn post_process_log(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::log::post::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote.clone()).await?;
			let arg = tg::process::log::post::Arg {
				remote: None,
				..arg
			};
			remote.post_process_log(id, arg).await?;
			return Ok(());
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

		// Write logs to stderr if necessary.
		if self.config.advanced.write_process_logs_to_stderr {
			tokio::io::stderr().write_all(&arg.bytes).await.ok();
		}

		// Write to the log file.
		self.post_process_log_to_file(id, arg.bytes.clone())
			.await?;

		// Write to stdout or stderr if necessary.
		if let (Some(stdout), tg::process::log::Stream::Stdout) = (data.stdout, arg.stream) {
			match stdout {
				tg::process::Stdio::Pipe(id) => {
					self.write_pipe_event(&id, tg::pipe::Event::Chunk(arg.bytes.clone()))
						.await?;
				},
				tg::process::Stdio::Pty(id) => {
					self.write_pty_event(&id, tg::pty::Event::Chunk(arg.bytes.clone()), false)
						.await?;
				},
			}
		}
		if let (Some(stderr), tg::process::log::Stream::Stderr) = (data.stderr, arg.stream) {
			match stderr {
				tg::process::Stdio::Pipe(id) => {
					self.write_pipe_event(&id, tg::pipe::Event::Chunk(arg.bytes.clone()))
						.await?;
				},
				tg::process::Stdio::Pty(id) => {
					self.write_pty_event(&id, tg::pty::Event::Chunk(arg.bytes.clone()), false)
						.await?;
				},
			}
		}

		// Publish the message.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				server
					.messenger
					.publish(format!("processes.{id}.log"), Bytes::new())
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});

		Ok(())
	}

	async fn post_process_log_to_file(
		&self,
		id: &tg::process::Id,
		bytes: Bytes,
	) -> tg::Result<()> {
		let path = self.logs_path().join(format!("{id}"));
		let mut file = tokio::fs::File::options()
			.create(true)
			.append(true)
			.open(&path)
			.await
			.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to open the log file"),
			)?;
		file.write_all(&bytes).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to write to the log file"),
		)?;
		Ok(())
	}

	pub(crate) async fn handle_post_process_log_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		handle.post_process_log(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
