use {
	crate::{Server, temp::Temp},
	futures::{future, prelude::*},
	num::ToPrimitive as _,
	std::{
		collections::BTreeMap,
		os::{
			fd::{AsFd as _, AsRawFd as _},
			unix::process::ExitStatusExt as _,
		},
		path::PathBuf,
		pin::pin,
	},
	tangram_client::prelude::*,
	tangram_futures::{read::Ext as _, task::Task, write::Ext as _},
	tangram_uri::Uri,
	tokio::io::{AsyncRead, AsyncWrite},
	tokio_util::io::ReaderStream,
};

pub struct Arg<'a> {
	pub args: Vec<String>,
	pub command: &'a tg::command::Data,
	pub cwd: PathBuf,
	pub env: BTreeMap<String, String>,
	pub executable: PathBuf,
	pub id: &'a tg::process::Id,
	pub remote: Option<&'a String>,
	pub serve_task: Option<(Task<()>, Uri)>,
	pub server: &'a Server,
	pub state: &'a tg::process::State,
	pub temp: &'a Temp,
}

pub async fn run(mut arg: Arg<'_>) -> tg::Result<super::Output> {
	let pty = None
		.or_else(|| {
			arg.state.stdin.as_ref().and_then(|stdio| match stdio {
				tg::process::Stdio::Pty(id) => Some(id),
				tg::process::Stdio::Pipe(_) => None,
			})
		})
		.or_else(|| {
			arg.state.stdout.as_ref().and_then(|stdio| match stdio {
				tg::process::Stdio::Pty(id) => Some(id),
				tg::process::Stdio::Pipe(_) => None,
			})
		})
		.or_else(|| {
			arg.state.stderr.as_ref().and_then(|stdio| match stdio {
				tg::process::Stdio::Pty(id) => Some(id),
				tg::process::Stdio::Pipe(_) => None,
			})
		});

	let server = arg.server;
	let temp = arg.temp;
	let serve_task = arg.serve_task.take();

	let exit = if let Some(pty) = pty {
		run_session(arg, pty)
			.await
			.map_err(|source| tg::error!(!source, "failed to run the process session"))?
	} else {
		run_inner(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to run the process"))?
	};

	// Stop and await the serve task.
	if let Some((task, _)) = serve_task {
		task.stop();
		task.wait()
			.await
			.map_err(|source| tg::error!(!source, "the serve task panicked"))?;
	}

	// Create the output.
	let mut output = super::Output {
		checksum: None,
		error: None,
		exit,
		output: None,
	};

	// Get the output path.
	let path = temp.path().join("output/output");
	let exists = tokio::fs::try_exists(&path)
		.await
		.map_err(|source| tg::error!(!source, "failed to determine if the output path exists"))?;

	// Try to read the user.tangram.output xattr.
	if let Ok(Some(bytes)) = xattr::get(&path, "user.tangram.output") {
		let tgon = String::from_utf8(bytes)
			.map_err(|source| tg::error!(!source, "failed to decode the output xattr"))?;
		output.output = Some(
			tgon.parse::<tg::Value>()
				.map_err(|source| tg::error!(!source, "failed to parse the output xattr"))?,
		);
	}

	// Try to read the user.tangram.error xattr.
	if let Ok(Some(bytes)) = xattr::get(&path, "user.tangram.error") {
		let error = serde_json::from_slice::<tg::error::Data>(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the error xattr"))?;
		let error = tg::Error::try_from(error)
			.map_err(|source| tg::error!(!source, "failed to convert the error data"))?;
		output.error = Some(error);
	}

	// Check in the output.
	if output.output.is_none() && exists {
		let arg = tg::checkin::Arg {
			options: tg::checkin::Options {
				destructive: true,
				deterministic: true,
				ignore: false,
				lock: None,
				locked: true,
				..Default::default()
			},
			path,
			updates: Vec::new(),
		};
		let artifact = tg::checkin(server, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the output"))?;
		output.output = Some(tg::Value::from(artifact));
	}

	Ok(output)
}

async fn run_session(arg: Arg<'_>, pty: &tg::pty::Id) -> tg::Result<u8> {
	let Arg {
		server,
		id,
		remote,
		command,
		state,
		args,
		cwd,
		env,
		executable,
		..
	} = arg;

	// Get the PTY session socket path.
	let path = server
		.ptys
		.get(pty)
		.ok_or_else(|| tg::error!("failed to find the pty"))?
		.temp
		.path()
		.to_owned();

	// Connect to the session.
	let mut client = tangram_session::Client::connect(path)
		.await
		.map_err(|source| tg::error!(!source, %id, "failed to connect to the session"))?;

	let mut fds = Vec::new();

	// Handle stdin.
	let (stdin, stdin_writer) = if command.stdin.is_some() {
		let (sender, receiver) = tokio::net::unix::pipe::pipe()
			.map_err(|source| tg::error!(!source, "failed to create a pipe for stdin"))?;
		let sender = sender.boxed();
		let fd = receiver
			.into_blocking_fd()
			.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
		let stdin = tangram_session::Stdio::Fd(fd.as_raw_fd());
		fds.push(fd);
		(stdin, Some(sender))
	} else {
		match state.stdin.as_ref() {
			Some(tg::process::Stdio::Pipe(pipe)) => {
				let pipe = server
					.pipes
					.get(pipe)
					.ok_or_else(|| tg::error!("failed to find the pipe"))?;
				let fd = pipe
					.receiver
					.as_fd()
					.try_clone_to_owned()
					.map_err(|source| tg::error!(!source, "failed to clone the receiver"))?;
				let receiver = tokio::net::unix::pipe::Receiver::from_owned_fd_unchecked(fd)
					.map_err(|source| tg::error!(!source, "io error"))?;
				let fd = receiver
					.into_blocking_fd()
					.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
				let stdin = tangram_session::Stdio::Fd(fd.as_raw_fd());
				fds.push(fd);
				(stdin, None)
			},
			Some(tg::process::Stdio::Pty(_)) => {
				let stdin = tangram_session::Stdio::Inherit;
				(stdin, None)
			},
			None => {
				let stdin = tangram_session::Stdio::Null;
				(stdin, None)
			},
		}
	};

	// Handle stdout.
	let (stdout, stdout_reader) = match state.stdout.as_ref() {
		Some(tg::process::Stdio::Pipe(pipe)) => {
			let pipe = server
				.pipes
				.get(pipe)
				.ok_or_else(|| tg::error!("failed to find the pipe"))?;
			let fd = pipe
				.sender
				.as_ref()
				.ok_or_else(|| tg::error!("the pipe is closed"))?
				.as_fd()
				.try_clone_to_owned()
				.map_err(|source| tg::error!(!source, "failed to clone the sender"))?;
			let sender = tokio::net::unix::pipe::Sender::from_owned_fd_unchecked(fd)
				.map_err(|source| tg::error!(!source, "io error"))?;
			let fd = sender
				.into_blocking_fd()
				.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
			let stdout = tangram_session::Stdio::Fd(fd.as_raw_fd());
			fds.push(fd);
			(stdout, None)
		},
		Some(tg::process::Stdio::Pty(_)) => {
			let stdout = tangram_session::Stdio::Inherit;
			(stdout, None)
		},
		None => {
			let (sender, receiver) = tokio::net::unix::pipe::pipe()
				.map_err(|source| tg::error!(!source, "failed to create a pipe for stdout"))?;
			let fd = sender
				.into_blocking_fd()
				.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
			let stdout = tangram_session::Stdio::Fd(fd.as_raw_fd());
			fds.push(fd);
			let receiver = receiver.boxed();
			(stdout, Some(receiver))
		},
	};

	// Handle stderr.
	let (stderr, stderr_reader) = match state.stderr.as_ref() {
		Some(tg::process::Stdio::Pipe(pipe)) => {
			let pipe = server
				.pipes
				.get(pipe)
				.ok_or_else(|| tg::error!("failed to find the pipe"))?;
			let fd = pipe
				.sender
				.as_ref()
				.ok_or_else(|| tg::error!("the pipe is closed"))?
				.as_fd()
				.try_clone_to_owned()
				.map_err(|source| tg::error!(!source, "failed to clone the sender"))?;
			let sender = tokio::net::unix::pipe::Sender::from_owned_fd_unchecked(fd)
				.map_err(|source| tg::error!(!source, "io error"))?;
			let fd = sender
				.into_blocking_fd()
				.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
			let stderr = tangram_session::Stdio::Fd(fd.as_raw_fd());
			fds.push(fd);
			(stderr, None)
		},
		Some(tg::process::Stdio::Pty(_)) => {
			let stderr = tangram_session::Stdio::Inherit;
			(stderr, None)
		},
		None => {
			let (sender, receiver) = tokio::net::unix::pipe::pipe()
				.map_err(|source| tg::error!(!source, "failed to create a pipe for stderr"))?;
			let fd = sender
				.into_blocking_fd()
				.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
			let stderr = tangram_session::Stdio::Fd(fd.as_raw_fd());
			fds.push(fd);
			let receiver = receiver.boxed();
			(stderr, Some(receiver))
		},
	};

	// Create the command.
	let cmd = tangram_session::Command {
		args,
		cwd,
		env,
		executable,
		stdin,
		stdout,
		stderr,
	};

	// Spawn via session.
	let pid = client
		.spawn(cmd)
		.await
		.map_err(|source| tg::error!(!source, %id, "failed to spawn the command via session"))?
		.to_i32()
		.unwrap();

	// Drop the FDs.
	drop(fds);

	// Spawn the stdio task.
	let stdio_task = tokio::spawn({
		let server = server.clone();
		let id = id.clone();
		let remote = remote.cloned();
		let stdin_blob = command.stdin.clone().map(tg::Blob::with_id);
		async move {
			stdio_task(
				&server,
				&id,
				remote.as_ref(),
				stdin_blob,
				stdin_writer,
				stdout_reader,
				stderr_reader,
			)
			.await?;
			Ok::<_, tg::Error>(())
		}
	});

	// Spawn the signal task.
	let signal_task = tokio::spawn({
		let server = server.clone();
		let id = id.clone();
		let remote = remote.cloned();
		async move {
			signal_task(&server, pid, &id, remote.as_ref())
				.await
				.inspect_err(|source| tracing::error!(?source, "the signal task failed"))
				.ok();
		}
	});

	// Await the process.
	let exit = client
		.wait()
		.await
		.map_err(|source| tg::error!(!source, %id, "failed to wait for the process"))?;

	// Abort the signal task.
	signal_task.abort();

	// Await the stdio task.
	stdio_task
		.await
		.map_err(|source| tg::error!(!source, "the stdio task panicked"))??;

	Ok(exit)
}

async fn run_inner(arg: Arg<'_>) -> tg::Result<u8> {
	let Arg {
		server,
		id,
		remote,
		command,
		state,
		args,
		cwd,
		env,
		executable,
		..
	} = arg;

	// Create the command.
	let mut cmd = tokio::process::Command::new(executable);
	cmd.args(args).current_dir(cwd).env_clear().envs(env);

	// Handle stdin.
	let (stdin, stdin_writer) = if command.stdin.is_some() {
		let (sender, receiver) = tokio::net::unix::pipe::pipe()
			.map_err(|source| tg::error!(!source, "failed to create a pipe for stdin"))?;
		let sender = sender.boxed();
		let fd = receiver
			.into_blocking_fd()
			.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
		let stdin = std::process::Stdio::from(fd);
		(stdin, Some(sender))
	} else {
		match state.stdin.as_ref() {
			Some(tg::process::Stdio::Pipe(pipe)) => {
				let pipe = server
					.pipes
					.get(pipe)
					.ok_or_else(|| tg::error!("failed to find the pipe"))?;
				let fd = pipe
					.receiver
					.as_fd()
					.try_clone_to_owned()
					.map_err(|source| tg::error!(!source, "failed to clone the receiver"))?;
				let receiver = tokio::net::unix::pipe::Receiver::from_owned_fd_unchecked(fd)
					.map_err(|source| tg::error!(!source, "io error"))?;
				let fd = receiver
					.into_blocking_fd()
					.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
				let stdin = std::process::Stdio::from(fd);
				(stdin, None)
			},
			Some(tg::process::Stdio::Pty(_)) => {
				unreachable!()
			},
			None => {
				let stdin = std::process::Stdio::null();
				(stdin, None)
			},
		}
	};

	// Handle stdout.
	let (stdout, stdout_reader) = match state.stdout.as_ref() {
		Some(tg::process::Stdio::Pipe(pipe)) => {
			let pipe = server
				.pipes
				.get(pipe)
				.ok_or_else(|| tg::error!("failed to find the pipe"))?;
			let fd = pipe
				.sender
				.as_ref()
				.ok_or_else(|| tg::error!("the pipe is closed"))?
				.as_fd()
				.try_clone_to_owned()
				.map_err(|source| tg::error!(!source, "failed to clone the sender"))?;
			let sender = tokio::net::unix::pipe::Sender::from_owned_fd_unchecked(fd)
				.map_err(|source| tg::error!(!source, "io error"))?;
			let fd = sender
				.into_blocking_fd()
				.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
			let stdout = std::process::Stdio::from(fd);
			(stdout, None)
		},
		Some(tg::process::Stdio::Pty(_)) => {
			unreachable!()
		},
		None => {
			let (sender, receiver) = tokio::net::unix::pipe::pipe()
				.map_err(|source| tg::error!(!source, "failed to create a pipe for stdout"))?;
			let fd = sender
				.into_blocking_fd()
				.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
			let stdout = std::process::Stdio::from(fd);
			let receiver = receiver.boxed();
			(stdout, Some(receiver))
		},
	};

	// Handle stderr.
	let (stderr, stderr_reader) = match state.stderr.as_ref() {
		Some(tg::process::Stdio::Pipe(pipe)) => {
			let pipe = server
				.pipes
				.get(pipe)
				.ok_or_else(|| tg::error!("failed to find the pipe"))?;
			let fd = pipe
				.sender
				.as_ref()
				.ok_or_else(|| tg::error!("the pipe is closed"))?
				.as_fd()
				.try_clone_to_owned()
				.map_err(|source| tg::error!(!source, "failed to clone the sender"))?;
			let sender = tokio::net::unix::pipe::Sender::from_owned_fd_unchecked(fd)
				.map_err(|source| tg::error!(!source, "io error"))?;
			let fd = sender
				.into_blocking_fd()
				.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
			let stderr = std::process::Stdio::from(fd);
			(stderr, None)
		},
		Some(tg::process::Stdio::Pty(_)) => {
			unreachable!()
		},
		None => {
			let (sender, receiver) = tokio::net::unix::pipe::pipe()
				.map_err(|source| tg::error!(!source, "failed to create a pipe for stderr"))?;
			let fd = sender
				.into_blocking_fd()
				.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
			let stderr = std::process::Stdio::from(fd);
			let receiver = receiver.boxed();
			(stderr, Some(receiver))
		},
	};

	// Set stdio.
	cmd.stdin(stdin).stdout(stdout).stderr(stderr);

	// Spawn the process.
	let mut child = cmd
		.spawn()
		.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;
	drop(cmd);
	let pid = child.id().unwrap().to_i32().unwrap();

	// Spawn the stdio task.
	let stdio_task = tokio::spawn({
		let server = server.clone();
		let id = id.clone();
		let remote = remote.cloned();
		let stdin_blob = command.stdin.clone().map(tg::Blob::with_id);
		async move {
			stdio_task(
				&server,
				&id,
				remote.as_ref(),
				stdin_blob,
				stdin_writer,
				stdout_reader,
				stderr_reader,
			)
			.await?;
			Ok::<_, tg::Error>(())
		}
	});

	// Spawn the signal task.
	let signal_task = tokio::spawn({
		let server = server.clone();
		let id = id.clone();
		let remote = remote.cloned();
		async move {
			signal_task(&server, pid, &id, remote.as_ref())
				.await
				.inspect_err(|source| tracing::error!(?source, "the signal task failed"))
				.ok();
		}
	});

	// Await the process.
	let exit = child.wait().await.map_err(
		|source| tg::error!(!source, process = %id, "failed to wait for the child process"),
	)?;
	let exit = None
		.or(exit.code())
		.or(exit.signal().map(|signal| 128 + signal))
		.unwrap()
		.to_u8()
		.unwrap();

	// Abort the signal task.
	signal_task.abort();

	// Await the stdio task.
	stdio_task
		.await
		.map_err(|source| tg::error!(!source, "the stdio task panicked"))??;

	Ok(exit)
}

async fn stdio_task<I, O, E>(
	server: &Server,
	id: &tg::process::Id,
	remote: Option<&String>,
	stdin_blob: Option<tg::Blob>,
	stdin: Option<I>,
	stdout: Option<O>,
	stderr: Option<E>,
) -> tg::Result<()>
where
	I: AsyncWrite + Unpin + Send + 'static,
	O: AsyncRead + Unpin + Send + 'static,
	E: AsyncRead + Unpin + Send + 'static,
{
	// Write the stdin blob to stdin if necessary.
	let stdin = Task::spawn({
		let server = server.clone();
		|_| {
			async move {
				let Some(mut stdin) = stdin else {
					return Ok(());
				};
				let Some(blob) = stdin_blob else {
					return Ok(());
				};
				let mut reader = blob.read(&server, tg::read::Options::default()).await?;
				tokio::io::copy(&mut reader, &mut stdin)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the blob to stdin"))?;
				Ok::<_, tg::Error>(())
			}
			.inspect_err(|error| {
				tracing::error!(?error);
			})
		}
	});

	let stdout = Task::spawn({
		let server = server.clone();
		let id = id.clone();
		let remote = remote.cloned();
		|_| async move {
			let Some(stdout) = stdout else {
				return Ok(());
			};
			stdio_task_inner(
				&server,
				&id,
				remote.as_ref(),
				tg::process::log::Stream::Stdout,
				stdout,
			)
			.await?;
			Ok::<_, tg::Error>(())
		}
	});

	let stderr = Task::spawn({
		let server = server.clone();
		let id = id.clone();
		let remote = remote.cloned();
		|_| async move {
			let Some(stderr) = stderr else {
				return Ok(());
			};
			stdio_task_inner(
				&server,
				&id,
				remote.as_ref(),
				tg::process::log::Stream::Stderr,
				stderr,
			)
			.await?;
			Ok::<_, tg::Error>(())
		}
	});

	// Join the tasks.
	let (stdout, stderr) = future::join(stdout.wait(), stderr.wait()).await;
	stdout
		.map_err(|source| tg::error!(!source, "the stdout task panicked"))?
		.map_err(|source| tg::error!(!source, "failed to read stdout from pipe"))?;
	stderr
		.map_err(|source| tg::error!(!source, "the stderr task panicked"))?
		.map_err(|source| tg::error!(!source, "failed to read stderr from pipe"))?;

	// Abort the stdin task.
	stdin.abort();

	Ok::<_, tg::Error>(())
}

async fn stdio_task_inner(
	server: &Server,
	id: &tg::process::Id,
	remote: Option<&String>,
	stream: tg::process::log::Stream,
	reader: impl AsyncRead + Unpin + Send + 'static,
) -> tg::Result<()> {
	let stream_ = ReaderStream::new(reader)
		.map_err(|source| tg::error!(!source, "failed to read from the reader"));
	let mut stream_ = pin!(stream_);
	while let Some(bytes) = stream_
		.try_next()
		.await
		.map_err(|source| tg::error!(!source, %id, "failed to read from the stream"))?
	{
		let arg = tg::process::log::post::Arg {
			bytes,
			local: None,
			remotes: remote.cloned().map(|r| vec![r]),
			stream,
		};
		server
			.post_process_log(id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to post the process log"))?;
	}
	Ok(())
}

async fn signal_task(
	server: &Server,
	pid: libc::pid_t,
	id: &tg::process::Id,
	remote: Option<&String>,
) -> tg::Result<()> {
	// Get the signal stream for the process.
	let arg = tg::process::signal::get::Arg {
		local: None,
		remotes: remote.map(|r| vec![r.clone()]),
	};
	let mut stream = server
		.try_get_process_signal_stream(id, arg)
		.await
		.map_err(
			|source| tg::error!(!source, process = %id, "failed to get the process's signal stream"),
		)?
		.ok_or_else(
			|| tg::error!(process = %id, "expected the process's signal stream to exist"),
		)?;

	// Handle the events.
	while let Some(event) = stream.try_next().await.map_err(
		|source| tg::error!(!source, process = %id, "failed to get the next signal event"),
	)? {
		match event {
			tg::process::signal::get::Event::Signal(signal) => unsafe {
				let ret = libc::kill(pid, signal_number(signal));
				if ret != 0 {
					let error = std::io::Error::last_os_error();
					tracing::error!(?error, "failed to send the signal");
				}
			},
			tg::process::signal::get::Event::End => break,
		}
	}

	Ok(())
}

fn signal_number(signal: tg::process::Signal) -> i32 {
	match signal {
		tg::process::Signal::SIGABRT => libc::SIGABRT,
		tg::process::Signal::SIGFPE => libc::SIGFPE,
		tg::process::Signal::SIGILL => libc::SIGILL,
		tg::process::Signal::SIGALRM => libc::SIGALRM,
		tg::process::Signal::SIGHUP => libc::SIGHUP,
		tg::process::Signal::SIGINT => libc::SIGINT,
		tg::process::Signal::SIGKILL => libc::SIGKILL,
		tg::process::Signal::SIGPIPE => libc::SIGPIPE,
		tg::process::Signal::SIGQUIT => libc::SIGQUIT,
		tg::process::Signal::SIGSEGV => libc::SIGSEGV,
		tg::process::Signal::SIGTERM => libc::SIGTERM,
		tg::process::Signal::SIGUSR1 => libc::SIGUSR1,
		tg::process::Signal::SIGUSR2 => libc::SIGUSR2,
	}
}
