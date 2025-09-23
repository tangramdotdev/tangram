use {
	futures::{StreamExt as _, future},
	std::{mem::MaybeUninit, os::fd::RawFd, pin::pin},
	tangram_client as tg,
	tokio::signal::unix::SignalKind,
	tokio_stream::wrappers::ReceiverStream,
};

/// Handle sigwinch.
pub async fn handle_sigwinch<H>(
	handle: &H,
	fd: RawFd,
	stdio: &tg::process::Stdio,
	remote: Option<String>,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	let tg::process::Stdio::Pty(pty) = stdio else {
		return Ok(());
	};

	// Create the size stream.
	let (send, recv) = tokio::sync::mpsc::channel(1);
	let mut signal = tokio::signal::unix::signal(SignalKind::window_change())
		.map_err(|source| tg::error!(!source, "failed to create signal handler"))?;
	tokio::spawn(async move {
		while let Some(()) = signal.recv().await {
			let size = unsafe {
				let mut winsize: MaybeUninit<libc::winsize> = MaybeUninit::uninit();
				let ret = libc::ioctl(fd, libc::TIOCGWINSZ, std::ptr::addr_of_mut!(winsize));
				if ret != 0 {
					break;
				}
				let winsize = winsize.assume_init();
				tg::pty::Size {
					rows: winsize.ws_row,
					cols: winsize.ws_col,
				}
			};
			let event = Ok::<_, tg::Error>(tg::pty::Event::Size(size));
			if send.send(event).await.is_err() {
				break;
			}
		}
	});
	let stream = ReceiverStream::new(recv);

	let arg = tg::pty::write::Arg {
		remote,
		master: true,
	};
	handle
		.write_pty(pty, arg, stream.boxed())
		.await
		.map_err(|source| tg::error!(!source, "failed to post the window change stream"))?;

	Ok(())
}

/// Handle all signals.
pub async fn handle_signals<H>(
	handle: &H,
	process: &tg::process::Id,
	remote: Option<String>,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	// Create signal streams.
	let mut alarm = tokio::signal::unix::signal(SignalKind::alarm())
		.map_err(|source| tg::error!(!source, "failed to create signal handler"))?;
	let mut hangup = tokio::signal::unix::signal(SignalKind::hangup())
		.map_err(|source| tg::error!(!source, "failed to create signal handler"))?;
	let mut interrupt = tokio::signal::unix::signal(SignalKind::interrupt())
		.map_err(|source| tg::error!(!source, "failed to create signal handler"))?;
	let mut pipe = tokio::signal::unix::signal(SignalKind::pipe())
		.map_err(|source| tg::error!(!source, "failed to create signal handler"))?;
	let mut quit = tokio::signal::unix::signal(SignalKind::quit())
		.map_err(|source| tg::error!(!source, "failed to create signal handler"))?;
	let mut terminate = tokio::signal::unix::signal(SignalKind::terminate())
		.map_err(|source| tg::error!(!source, "failed to create signal handler"))?;
	let mut user_defined1 = tokio::signal::unix::signal(SignalKind::user_defined1())
		.map_err(|source| tg::error!(!source, "failed to create signal handler"))?;
	let mut user_defined2 = tokio::signal::unix::signal(SignalKind::user_defined2())
		.map_err(|source| tg::error!(!source, "failed to create signal handler"))?;

	// Handle signals in a loop.
	loop {
		let alarm = pin!(alarm.recv());
		let hangup = pin!(hangup.recv());
		let interrupt = pin!(interrupt.recv());
		let pipe = pin!(pipe.recv());
		let quit = pin!(quit.recv());
		let terminate = pin!(terminate.recv());
		let user_defined1 = pin!(user_defined1.recv());
		let user_defined2 = pin!(user_defined2.recv());
		let fut = future::select_all([
			alarm,
			hangup,
			interrupt,
			pipe,
			quit,
			terminate,
			user_defined1,
			user_defined2,
		]);
		let signal = match pin!(fut).await {
			(Some(()), 0, ..) => tg::process::Signal::SIGALRM,
			(Some(()), 1, ..) => tg::process::Signal::SIGHUP,
			(Some(()), 2, ..) => tg::process::Signal::SIGINT,
			(Some(()), 3, ..) => tg::process::Signal::SIGPIPE,
			(Some(()), 4, ..) => tg::process::Signal::SIGQUIT,
			(Some(()), 5, ..) => tg::process::Signal::SIGTERM,
			(Some(()), 6, ..) => tg::process::Signal::SIGUSR1,
			(Some(()), 7, ..) => tg::process::Signal::SIGUSR2,
			_ => break,
		};
		let arg = tg::process::signal::post::Arg {
			remote: remote.clone(),
			signal,
		};
		handle
			.signal_process(process, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to post signal"))?;
	}

	Ok(())
}
