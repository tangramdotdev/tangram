use futures::{StreamExt as _, future};
use std::{mem::MaybeUninit, os::fd::RawFd, pin::pin};
use tangram_client as tg;
use tokio::signal::unix::SignalKind;
use tokio_stream::wrappers::ReceiverStream;

/// Handle sigwinch.
pub async fn handle_sigwinch<H>(
	handle: &H,
	fd: RawFd,
	io: &tg::process::Io,
	remote: Option<String>,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	let tg::process::Io::Pty(pty) = io else {
		return Ok(());
	};

	let (send, recv) = tokio::sync::mpsc::channel(1);

	let mut signal = tokio::signal::unix::signal(SignalKind::window_change())
		.map_err(|source| tg::error!(!source, "failed to create signal handler"))?;
	tokio::task::spawn(async move {
		while let Some(()) = signal.recv().await {
			let window_size = unsafe {
				let mut winsize: MaybeUninit<libc::winsize> = MaybeUninit::uninit();
				if libc::ioctl(fd, libc::TIOCGWINSZ, std::ptr::addr_of_mut!(winsize)) != 0 {
					break;
				}
				let window_size = winsize.assume_init();
				tg::pty::WindowSize {
					rows: window_size.ws_row,
					cols: window_size.ws_col,
					xpos: window_size.ws_xpixel,
					ypos: window_size.ws_ypixel,
				}
			};
			let event = Ok::<_, tg::Error>(tg::pty::Event::WindowSize(window_size));
			if send.send(event).await.is_err() {
				break;
			}
		}
	});

	let arg = tg::pty::post::Arg {
		remote,
		master: true,
	};
	let stream = ReceiverStream::new(recv);
	handle
		.post_pty(pty, arg, stream.boxed())
		.await
		.map_err(|source| tg::error!(!source, "failed to post the window change stream"))
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
			.post_process_signal(process, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to post signal"))?;
	}

	Ok(())
}
