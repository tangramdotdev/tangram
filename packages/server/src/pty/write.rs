use crate::Server;
use futures::{
	Stream, StreamExt as _, future,
	stream::{FuturesUnordered, TryStreamExt as _},
};
use indoc::formatdoc;
use num::ToPrimitive as _;
use std::{
	os::fd::{AsRawFd, RawFd},
	pin::pin,
};
use tangram_client::{self as tg, handle::Process};
use tangram_database::{self as db, Database, Query};
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tokio::io::unix::AsyncFd;

impl Server {
	pub async fn write_pty(
		&self,
		id: &tg::pty::Id,
		mut arg: tg::pty::write::Arg,
		stream: impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote.clone()).await?;
			return remote.write_pty(id, arg, stream.boxed()).await;
		}

		let fd = if arg.master {
			self.ptys
				.get(id)
				.ok_or_else(|| tg::error!("failed to get pty"))?
				.host
				.try_clone()
				.map_err(|source| tg::error!(!source, "failed to get pty"))?
		} else {
			self.ptys
				.get(id)
				.ok_or_else(|| tg::error!("failed to get pty"))?
				.guest
				.try_clone()
				.map_err(|source| tg::error!(!source, "failed to get pty"))?
		};

		let fd = AsyncFd::with_interest(fd, tokio::io::Interest::WRITABLE)
			.map_err(|source| tg::error!(!source, "failed to write pty"))?;

		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			match event {
				tg::pty::Event::Chunk(chunk) => {
					self.js_signal_hack(id, &chunk)
						.await
						.inspect_err(|error| {
							tracing::error!(?error, "failed to send signal to process");
						})
						.ok();
					let mut chunk = chunk.as_ref();
					while !chunk.is_empty() {
						let n = fd
							.async_io(tokio::io::Interest::WRITABLE, |fd| unsafe {
								let n =
									libc::write(fd.as_raw_fd(), chunk.as_ptr().cast(), chunk.len());
								if n < 0 {
									return Err(std::io::Error::last_os_error());
								}
								Ok(n.to_usize().unwrap())
							})
							.await
							.map_err(|source| tg::error!(!source, "failed to write pty"))?;
						if n == 0 {
							break;
						}
						chunk = &chunk[n..];
					}
				},
				tg::pty::Event::Size(size) => {
					change_window_size(fd.as_raw_fd(), size)
						.await
						.map_err(|source| tg::error!(!source, "failed to change window size"))?;
				},
				tg::pty::Event::End => {
					break;
				},
			}
		}

		Ok(())
	}

	async fn js_signal_hack(&self, pty: &tg::pty::Id, chunk: &[u8]) -> tg::Result<()> {
		// Scan the input for a signal.
		let signal = chunk.iter().copied().find_map(|byte| {
			if byte == 0x03 {
				Some(tg::process::Signal::SIGINT)
			} else if byte == 0x1c {
				Some(tg::process::Signal::SIGQUIT)
			} else {
				None
			}
		});
		let Some(signal) = signal else {
			return Ok(());
		};

		// Get all the JS processes that are connected to this PTY.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to acquire a connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select id from processes
				where
					host = 'js' and
					status = 'started' and
					stdin = {p}1;
			"
		);
		let params = db::params![pty.to_string()];
		let processes = connection
			.query_all_value_into::<tg::process::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?;
		drop(connection);

		// Signal all of the processes concurrently.
		processes
			.into_iter()
			.map(|process| {
				let server = self.clone();
				async move {
					server
						.signal_process(
							&process,
							tg::process::signal::post::Arg {
								signal,
								remote: None,
							},
						)
						.await
						.inspect_err(
							|error| tracing::error!(%process, ?error, "failed to signal process"),
						)
						.ok();
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<()>()
			.await;

		Ok(())
	}

	pub(crate) async fn handle_write_pty_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move {
			stop.wait().await;
		};

		// Create the stream.
		let stream = request
			.sse()
			.map(|event| match event {
				Ok(event) => event.try_into(),
				Err(source) => Err(source.into()),
			})
			.take_while_inclusive(|event| future::ready(!matches!(event, Ok(tg::pty::Event::End))))
			.take_until(stop)
			.boxed();

		handle.write_pty(&id, arg, stream).await?;

		// Create the response.
		let response = http::Response::builder().empty().unwrap();

		Ok(response)
	}
}

async fn change_window_size(fd: RawFd, size: tg::pty::Size) -> std::io::Result<()> {
	tokio::task::spawn_blocking(move || unsafe {
		let mut winsize = libc::winsize {
			ws_col: size.cols,
			ws_row: size.rows,
			ws_xpixel: 0,
			ws_ypixel: 0,
		};
		if libc::ioctl(fd, libc::TIOCSWINSZ, std::ptr::addr_of_mut!(winsize)) != 0 {
			return Err(std::io::Error::last_os_error());
		}
		Ok(())
	})
	.await
	.unwrap()
}
