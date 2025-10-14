use {
	crate::Server,
	futures::{
		Stream, StreamExt as _, future,
		stream::{FuturesUnordered, TryStreamExt as _},
	},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{
		os::fd::{AsRawFd as _, RawFd},
		pin::pin,
	},
	tangram_client::{self as tg, handle::Process},
	tangram_database::{self as db, Database, Query},
	tangram_futures::{stream::Ext as _, task::Stop},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

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

		let pty = self
			.ptys
			.get_mut(id)
			.ok_or_else(|| tg::error!("failed to get the pty"))?;
		let fd = if arg.master {
			pty.master
				.as_ref()
				.ok_or_else(|| tg::error!("the pty master is closed"))?
				.as_raw_fd()
		} else {
			pty.slave
				.as_ref()
				.ok_or_else(|| tg::error!("the pty slave is closed"))?
				.as_raw_fd()
		};
		drop(pty);

		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			match event {
				tg::pty::Event::Chunk(chunk) => {
					self.js_signal_hack(id, &chunk)
						.await
						.inspect_err(|error| {
							tracing::error!(?error, "failed to signal the process");
						})
						.ok();
					tokio::task::spawn_blocking(move || unsafe {
						let mut chunk = chunk.as_ref();
						while !chunk.is_empty() {
							let n = libc::write(fd, chunk.as_ptr().cast(), chunk.len());
							if n < 0 {
								let error = std::io::Error::last_os_error();
								return Err(error);
							}
							let n = n.to_usize().unwrap();
							if n == 0 {
								break;
							}
							chunk = &chunk[n..];
						}
						Ok(())
					})
					.await
					.unwrap()
					.map_err(|source| tg::error!(!source, "failed to write to the pty"))?;
				},
				tg::pty::Event::Size(size) => {
					Self::pty_write_set_size(fd.as_raw_fd(), size)
						.await
						.map_err(|source| tg::error!(!source, "failed to change the size"))?;
				},
				tg::pty::Event::End => {
					return Err(tg::error!("cannot write an end event"));
				},
			}
		}

		Ok(())
	}

	async fn js_signal_hack(&self, pty: &tg::pty::Id, chunk: &[u8]) -> tg::Result<()> {
		// Scan the input for all signals and deduplicate.
		let mut signals = Vec::new();
		for byte in chunk.iter().copied() {
			let signal = if byte == 0x03 {
				Some(tg::process::Signal::SIGINT)
			} else if byte == 0x1c {
				Some(tg::process::Signal::SIGQUIT)
			} else {
				None
			};
			if let Some(signal) = signal {
				signals.push(signal);
			}
		}
		if signals.is_empty() {
			return Ok(());
		}

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
			.query_all_value_into::<db::value::Serde<tg::process::Id>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?
			.into_iter()
			.map(|value| value.0)
			.collect::<Vec<_>>();
		drop(connection);

		// Signal all of the processes with each signal concurrently.
		signals
			.into_iter()
			.flat_map(|signal| {
				processes.iter().map(move |process| {
					let server = self.clone();
					let process = process.clone();
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
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<()>()
			.await;

		Ok(())
	}

	async fn pty_write_set_size(fd: RawFd, size: tg::pty::Size) -> std::io::Result<()> {
		tokio::task::spawn_blocking(move || unsafe {
			let mut winsize = libc::winsize {
				ws_col: size.cols,
				ws_row: size.rows,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			let ret = libc::ioctl(fd, libc::TIOCSWINSZ, std::ptr::addr_of_mut!(winsize));
			if ret != 0 {
				return Err(std::io::Error::last_os_error());
			}
			Ok(())
		})
		.await
		.unwrap()
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
