use {crate::Cli, std::path::PathBuf, tangram_client as tg};

#[derive(Clone, Debug, clap::Args)]
pub struct Args {
	#[arg(long)]
	pty: String,

	#[arg(long)]
	path: PathBuf,
}

impl Cli {
	#[must_use]
	pub fn command_internal_session(args: Args) -> std::process::ExitCode {
		let result = Self::command_internal_session_inner(args);
		match result {
			Ok(()) => 0.into(),
			Err(error) => {
				eprintln!("{error}");
				1.into()
			},
		}
	}

	pub fn command_internal_session_inner(args: Args) -> tg::Result<()> {
		// Open the pty and set up the controlling tty.
		unsafe {
			// Ignore signals.
			libc::signal(libc::SIGHUP, libc::SIG_IGN);
			libc::signal(libc::SIGINT, libc::SIG_IGN);
			libc::signal(libc::SIGQUIT, libc::SIG_IGN);

			// Disconnect from the old controlling terminal.
			let tty = libc::open(c"/dev/tty".as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
			if tty > 0 {
				#[allow(clippy::useless_conversion)]
				libc::ioctl(tty, libc::TIOCNOTTY.into(), std::ptr::null_mut::<()>());
				libc::close(tty);
			}

			// Set the current process as session leader.
			let ret = libc::setsid();
			if ret == -1 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to create a session"
				));
			}

			// Open the pty.
			let fd = libc::open(
				std::ffi::CString::new(args.pty.as_bytes())
					.unwrap()
					.as_ptr(),
				libc::O_RDWR,
			);
			if fd < 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to open the pty"
				));
			}

			// Set the pty as the controlling tty.
			#[allow(clippy::useless_conversion)]
			let ret = libc::ioctl(fd, libc::TIOCSCTTY.into(), 0);
			if ret < 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to set the controlling tty"
				));
			}

			// Duplicate the pty to stdin, stdout, stderr.
			if libc::dup2(fd, 0) < 0 || libc::dup2(fd, 1) < 0 || libc::dup2(fd, 2) < 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to duplicate the tty to stdio"
				));
			}
			libc::close(fd);
		}

		// Create a single-threaded tokio runtime.
		let runtime = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.map_err(|source| tg::error!(!source, "failed to create the runtime"))?;

		// Run the server.
		runtime.block_on(async { tangram_session::Server::run(args.path).await })?;

		Ok(())
	}
}
