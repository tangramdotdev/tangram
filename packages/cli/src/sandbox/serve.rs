use {
	crate::Cli,
	std::{io::Write as _, os::fd::FromRawFd as _, path::PathBuf},
	tangram_client::prelude::*,
	tangram_sandbox as sandbox,
};

/// Start a sandbox server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub root: PathBuf,

	#[command(flatten)]
	pub options: super::Options,

	/// The path to the socket.
	#[arg(long)]
	pub socket: PathBuf,

	/// A file descriptor to write a ready signal to.
	#[arg(long)]
	pub ready_fd: Option<i32>,
}

impl Cli {
	#[must_use]
	pub fn command_sandbox_serve(args: Args) -> std::process::ExitCode {
		let result = Self::command_sandbox_serve_inner(args);
		if let Err(error) = result {
			Cli::print_error_basic(tg::Referent::with_item(error));
			return std::process::ExitCode::FAILURE;
		}
		std::process::ExitCode::SUCCESS
	}

	fn command_sandbox_serve_inner(args: Args) -> tg::Result<()> {
		unsafe {
			// Ignore signals.
			libc::signal(libc::SIGHUP, libc::SIG_IGN);
			libc::signal(libc::SIGINT, libc::SIG_IGN);
			libc::signal(libc::SIGQUIT, libc::SIG_IGN);

			// Disconnect from the old controlling terminal.
			let tty = libc::open(c"/dev/tty".as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
			if tty > 0 {
				#[cfg_attr(target_os = "linux", expect(clippy::useless_conversion))]
				libc::ioctl(tty, libc::TIOCNOTTY.into(), std::ptr::null_mut::<()>());
				libc::close(tty);
			}
		}

		// Create the sandbox options.
		let options = sandbox::Options {
			chroot: Some(args.root),
			hostname: args.options.hostname,
			mounts: args
				.options
				.mounts
				.into_iter()
				.map(|mount| tangram_sandbox::Mount {
					source: mount.source,
					target: mount.target,
					fstype: mount.fstype,
					flags: mount.flags,
					data: mount.data,
				})
				.collect(),
			network: args.options.network.get(),
			user: args.options.user,
		};

		// Enter the sandbox.
		unsafe { sandbox::server::Server::enter(&options)? };

		// Spawn a tokio runtime.
		let runtime = tokio::runtime::Builder::new_current_thread()
			.enable_io()
			.build()
			.map_err(|source| tg::error!(!source, "failed to start tokio runtime"))?;
		runtime.block_on(async move {
			let server = sandbox::server::Server::new()
				.map_err(|source| tg::error!(!source, "failed to start the server"))?;
			eprintln!("created the server");

			// Bind the socket.
			let listener = sandbox::server::Server::bind(&args.socket)?;
			eprintln!("bound the listener");

			// Signal readiness if a ready fd was provided.
			if let Some(fd) = args.ready_fd {
				let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
				file.write_all(&[0x00])
					.map_err(|source| tg::error!(!source, "failed to write the ready signal"))?;
				drop(file);
				eprintln!("signalled ready");
			}

			// Serve.
			server
				.serve(listener)
				.await
				.inspect_err(|error| eprintln!("failed to serve: {error}"))?;
			Ok::<_, tg::Error>(())
		})
	}
}
