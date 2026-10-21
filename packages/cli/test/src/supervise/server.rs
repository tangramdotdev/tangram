use {
	crate::{Result, process, signal::Signals},
	std::{
		ffi::OsString,
		fs::File,
		io::{self, Read as _, Write as _},
		os::{
			fd::{AsRawFd as _, OwnedFd},
			unix::process::CommandExt as _,
		},
		path::{Path, PathBuf},
		process::{Command, Stdio},
		sync::mpsc,
		time::{Duration, Instant},
	},
};

/// The time that the process's session has to exit after SIGKILL.
const KILL: Duration = Duration::from_secs(1);

/// The time to wait for the supervisor after SIGKILL.
const STOP: Duration = Duration::from_secs(5);

/// The time that the process's session has to exit after SIGTERM once the process exits.
const TERMINATE: Duration = Duration::from_secs(3);

/// Start a background process, such as a server, and wait for it to signal readiness.
#[derive(clap::Args)]
pub struct StartArgs {
	/// The command to run.
	#[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
	command: Vec<OsString>,

	/// The noun that describes the process in error messages.
	#[arg(default_value = "process", long)]
	description: String,

	/// The directory for the supervisor's lock, pid, exit, and log files.
	#[arg(long)]
	directory: PathBuf,

	/// The name that prefixes each line of the process's output.
	#[arg(long)]
	name: String,

	/// The file descriptor on which the process writes a zero byte when it is ready. Without it, the process is ready once it is spawned.
	#[arg(long)]
	ready_fd: Option<libc::c_int>,

	/// The number of seconds to wait for readiness.
	#[arg(default_value_t = 30, long)]
	ready_timeout: u64,

	/// Paths to remove after the process exits.
	#[arg(long)]
	remove: Vec<PathBuf>,
}

/// Exit successfully if the supervised process is running.
#[derive(clap::Args)]
pub struct StatusArgs {
	/// The supervisor's directory.
	#[arg(long)]
	directory: PathBuf,
}

/// Stop the supervised process with SIGTERM, then SIGKILL, and wait for it to exit.
#[derive(clap::Args)]
pub struct StopArgs {
	/// The supervisor's directory.
	#[arg(long)]
	directory: PathBuf,

	/// The number of seconds to wait after SIGTERM before sending SIGKILL.
	#[arg(default_value_t = 10, long)]
	timeout: u64,
}

/// Wait for the supervised process to exit and print its exit code.
#[derive(clap::Args)]
pub struct WaitArgs {
	/// The supervisor's directory.
	#[arg(long)]
	directory: PathBuf,

	/// The number of seconds to wait.
	#[arg(default_value_t = 10, long)]
	timeout: u64,
}

/// A message from the supervisor to the process that started it.
enum Message {
	Error(String),
	Exited(u8),
	Ready,
}

/// Prefix each complete line of output with a name and write it to stderr.
struct Lines {
	buffer: Vec<u8>,
	prefix: Vec<u8>,
}

pub fn start(args: &StartArgs) -> Result<u8> {
	// Create the pipe on which the supervisor reports readiness.
	std::fs::create_dir_all(&args.directory)?;
	let (read, write) = process::pipe()?;

	// Fork the supervisor.
	// SAFETY: This process is single threaded, so the child may continue to run arbitrary code.
	let pid = unsafe { libc::fork() };
	if pid < 0 {
		return Err(io::Error::last_os_error().into());
	}
	if pid == 0 {
		drop(read);
		let code = supervise(args, write);
		std::process::exit(i32::from(code));
	}
	drop(write);

	// Wait for the supervisor to report.
	let deadline = Instant::now() + Duration::from_secs(args.ready_timeout);
	let message = receive(&read, deadline)?;
	match message {
		Some(Message::Ready) => {
			let server = std::fs::read_to_string(args.directory.join("pid"))?;
			let server = server.lines().nth(1).unwrap_or_default().to_owned();
			println!("{{\"pid\":{pid},\"server\":{server}}}");
			Ok(0)
		},
		Some(Message::Exited(_)) => {
			let message = format!("the {} exited before signaling readiness", args.description);
			Err(failure(&args.directory, &message))
		},
		Some(Message::Error(error)) => Err(failure(&args.directory, &error)),
		None => {
			process::kill(pid, libc::SIGTERM);
			wait_lock(&args.directory, Duration::from_secs(10));
			let message = format!(
				"the {} did not signal readiness within {} seconds",
				args.description, args.ready_timeout
			);
			Err(failure(&args.directory, &message))
		},
	}
}

pub fn status(args: &StatusArgs) -> Result<u8> {
	let code = if running(&args.directory)? { 0 } else { 1 };
	Ok(code)
}

pub fn stop(args: &StopArgs) -> Result<u8> {
	// Ask the supervisor to forward SIGTERM to the process.
	if !running(&args.directory)? {
		return Ok(0);
	}
	let pid = supervisor_pid(&args.directory)?;
	process::kill(pid, libc::SIGTERM);
	if wait_lock(&args.directory, Duration::from_secs(args.timeout)) {
		return Ok(0);
	}

	// Ask the supervisor to kill the process.
	process::kill(pid, libc::SIGUSR1);
	if wait_lock(&args.directory, STOP) {
		return Ok(0);
	}

	Err(format!("the process did not stop: {}", args.directory.display()).into())
}

pub fn wait(args: &WaitArgs) -> Result<u8> {
	if !wait_lock(&args.directory, Duration::from_secs(args.timeout)) {
		return Err(format!("the process did not exit: {}", args.directory.display()).into());
	}
	let exit = std::fs::read_to_string(args.directory.join("exit"))?;
	print!("{exit}");
	Ok(0)
}

impl Lines {
	fn new(name: &str) -> Self {
		Self {
			buffer: Vec::new(),
			prefix: format!("{name}: ").into_bytes(),
		}
	}

	fn write(&mut self, bytes: &[u8]) {
		self.buffer.extend_from_slice(bytes);
		let Some(end) = self.buffer.iter().rposition(|byte| *byte == b'\n') else {
			return;
		};
		let mut output = Vec::new();
		for line in self.buffer[..end].split(|byte| *byte == b'\n') {
			output.extend_from_slice(&self.prefix);
			output.extend_from_slice(line);
			output.push(b'\n');
		}
		io::stderr().write_all(&output).ok();
		self.buffer.drain(..=end);
	}

	fn finish(&mut self) {
		if !self.buffer.is_empty() {
			self.buffer.push(b'\n');
			let bytes = std::mem::take(&mut self.buffer);
			self.write(&bytes);
		}
	}
}

/// Run the supervisor in the forked child, and return its exit code.
fn supervise(args: &StartArgs, write: OwnedFd) -> u8 {
	let mut reporter = Some(write);
	match supervise_inner(args, &mut reporter) {
		Ok(code) => code,
		Err(error) => {
			report(&mut reporter, &Message::Error(error.to_string()));
			1
		},
	}
}

fn supervise_inner(args: &StartArgs, reporter: &mut Option<OwnedFd>) -> Result<u8> {
	// Detach from the caller's session and output.
	// SAFETY: setsid has no memory safety preconditions.
	unsafe {
		libc::setsid();
	}
	let null = File::options().read(true).write(true).open("/dev/null")?;
	// SAFETY: Both file descriptors are open.
	unsafe {
		libc::dup2(null.as_raw_fd(), 0);
		libc::dup2(null.as_raw_fd(), 1);
	}

	// Hold the lock for the supervisor's lifetime, so that waiters are woken when it exits.
	let lock = File::options()
		.create(true)
		.truncate(false)
		.write(true)
		.open(args.directory.join("supervisor.lock"))?;
	// SAFETY: flock has no memory safety preconditions.
	let result = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
	if result != 0 {
		return Err("the process is already running".into());
	}
	std::fs::write(args.directory.join("exit"), "")?;

	// Install the signal handlers before spawning, so that no exit is missed.
	let signals = Signals::new(&[
		libc::SIGCHLD,
		libc::SIGHUP,
		libc::SIGINT,
		libc::SIGTERM,
		libc::SIGUSR1,
	])?;

	// Create the readiness and output pipes.
	let ready = args.ready_fd.map(|_| process::pipe()).transpose()?;
	let (output, output_write) = process::pipe()?;
	process::set_nonblocking(&output)?;

	// Spawn the process in its own session on Linux, or its own process group on macOS.
	let mut command = Command::new(&args.command[0]);
	command
		.args(&args.command[1..])
		.stdin(Stdio::null())
		.stdout(output_write.try_clone()?)
		.stderr(output_write);
	if let (Some(target), Some((_, ready_write))) = (args.ready_fd, &ready) {
		let source = ready_write.as_raw_fd();
		// SAFETY: The closure only calls async-signal-safe functions.
		unsafe {
			command.pre_exec(move || {
				detach()?;
				if source == target {
					let flags = libc::fcntl(source, libc::F_GETFD);
					if flags < 0
						|| libc::fcntl(source, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0
					{
						return Err(io::Error::last_os_error());
					}
				} else if libc::dup2(source, target) < 0 {
					return Err(io::Error::last_os_error());
				}
				Ok(())
			});
		}
	} else {
		// SAFETY: The closure only calls async-signal-safe functions.
		unsafe {
			command.pre_exec(detach);
		}
	}
	let child = command.spawn()?;
	drop(command);
	let server = libc::pid_t::try_from(child.id())?;
	let ready = ready.map(|(read, _)| read);
	// SAFETY: getpid has no preconditions.
	let supervisor = unsafe { libc::getpid() };
	std::fs::write(
		args.directory.join("pid"),
		format!("{supervisor}\n{server}\n"),
	)?;
	if ready.is_none() {
		report(reporter, &Message::Ready);
	}

	// Copy the output, report readiness, and forward signals until the process exits.
	let mut log = File::options()
		.append(true)
		.create(true)
		.open(args.directory.join("log"))?;
	let mut lines = Lines::new(&args.name);
	let mut ready = ready;
	let mut output = Some(output);
	let status = loop {
		let mut fds = vec![libc::pollfd {
			events: libc::POLLIN,
			fd: signals.fd(),
			revents: 0,
		}];
		if let Some(ready) = &ready {
			fds.push(libc::pollfd {
				events: libc::POLLIN,
				fd: ready.as_raw_fd(),
				revents: 0,
			});
		}
		if let Some(output) = &output {
			fds.push(libc::pollfd {
				events: libc::POLLIN,
				fd: output.as_raw_fd(),
				revents: 0,
			});
		}
		process::poll(&mut fds, None)?;

		// Read the readiness byte.
		if let Some(fd) = &ready
			&& fds
				.iter()
				.any(|pollfd| pollfd.fd == fd.as_raw_fd() && pollfd.revents != 0)
		{
			let mut byte = [0u8; 1];
			let n = File::from(fd.try_clone()?).read(&mut byte).unwrap_or(0);
			if n == 1 && byte[0] == 0 {
				report(reporter, &Message::Ready);
			} else if n == 1 {
				let error = format!(
					"the {} signaled an invalid readiness byte: {}",
					args.description, byte[0]
				);
				report(reporter, &Message::Error(error));
				process::kill(server, libc::SIGKILL);
			}
			ready = None;
		}

		// Copy the output.
		if let Some(fd) = &output
			&& fds
				.iter()
				.any(|pollfd| pollfd.fd == fd.as_raw_fd() && pollfd.revents != 0)
			&& !copy(fd, &mut log, &mut lines)
		{
			output = None;
		}

		// Forward signals and reap the process.
		let mut status = None;
		for signal in signals.take() {
			match signal {
				libc::SIGCHLD => {
					status = process::reap()
						.statuses
						.into_iter()
						.find_map(|(pid, status)| (pid == server).then_some(status));
				},
				libc::SIGUSR1 => process::kill(server, libc::SIGKILL),
				signal => process::kill(server, signal),
			}
		}
		if let Some(status) = status {
			break status;
		}
	};
	let code = process::code(status);

	// Terminate the rest of the process's session.
	terminate(server)?;

	// Copy the output that remains in the pipe without waiting for escaped descendants to close it.
	if let Some(fd) = &output {
		copy(fd, &mut log, &mut lines);
	}
	lines.finish();

	// Remove the paths and record the exit code.
	for path in &args.remove {
		std::fs::remove_file(path).ok();
	}
	std::fs::write(args.directory.join("exit"), format!("{code}\n"))?;
	report(reporter, &Message::Exited(code));

	Ok(code)
}

/// Move the process into its own session on Linux, or its own process group on macOS.
fn detach() -> io::Result<()> {
	#[cfg(target_os = "linux")]
	// SAFETY: setsid is async-signal-safe.
	let result = unsafe { libc::setsid() };
	#[cfg(target_os = "macos")]
	// SAFETY: setpgid is async-signal-safe.
	let result = unsafe { libc::setpgid(0, 0) };
	if result < 0 {
		return Err(io::Error::last_os_error());
	}
	Ok(())
}

/// Terminate the processes remaining in the exited process's session or process group, first with SIGTERM and then with SIGKILL.
fn terminate(server: libc::pid_t) -> Result<()> {
	for (signal, duration) in [(libc::SIGTERM, TERMINATE), (libc::SIGKILL, KILL)] {
		#[cfg(target_os = "linux")]
		let pids = process::session(server);
		#[cfg(target_os = "macos")]
		let pids = process::group(server);
		if pids.is_empty() {
			return Ok(());
		}
		for pid in &pids {
			process::kill(*pid, signal);
		}
		process::wait(&pids, Instant::now() + duration)?;
	}
	Ok(())
}

/// Copy the available output to the log and to stderr. Return false when the pipe is closed.
fn copy(fd: &OwnedFd, log: &mut File, lines: &mut Lines) -> bool {
	let mut buffer = vec![0u8; 65536];
	loop {
		// SAFETY: The buffer is valid for writes of its length.
		let n = unsafe { libc::read(fd.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
		match usize::try_from(n) {
			Ok(0) => return false,
			Ok(n) => {
				log.write_all(&buffer[..n]).ok();
				lines.write(&buffer[..n]);
			},
			Err(_) => return process::errno() == libc::EAGAIN || process::errno() == libc::EINTR,
		}
	}
}

/// Write a message to the process that started the supervisor, once.
fn report(reporter: &mut Option<OwnedFd>, message: &Message) {
	let Some(fd) = reporter.take() else {
		return;
	};
	let line = match message {
		Message::Error(error) => format!("error {error}\n"),
		Message::Exited(code) => format!("exited {code}\n"),
		Message::Ready => "ready\n".to_owned(),
	};
	File::from(fd).write_all(line.as_bytes()).ok();
}

/// Read the supervisor's message. Return None if the deadline passes first.
fn receive(fd: &OwnedFd, deadline: Instant) -> Result<Option<Message>> {
	let mut data = Vec::new();
	loop {
		let mut fds = [libc::pollfd {
			events: libc::POLLIN,
			fd: fd.as_raw_fd(),
			revents: 0,
		}];
		process::poll(&mut fds, Some(deadline))?;
		if fds[0].revents == 0 {
			if Instant::now() >= deadline {
				return Ok(None);
			}
			continue;
		}
		let mut buffer = [0u8; 4096];
		let n = File::from(fd.try_clone()?).read(&mut buffer)?;
		if n == 0 {
			break;
		}
		data.extend_from_slice(&buffer[..n]);
		if data.ends_with(b"\n") {
			break;
		}
	}
	let line = String::from_utf8_lossy(&data);
	let line = line.trim_end();
	let message = if line == "ready" {
		Message::Ready
	} else if let Some(code) = line.strip_prefix("exited ") {
		Message::Exited(code.parse().unwrap_or(1))
	} else if let Some(error) = line.strip_prefix("error ") {
		Message::Error(error.to_owned())
	} else {
		Message::Error("the supervisor exited unexpectedly".to_owned())
	};
	Ok(Some(message))
}

/// Determine whether the supervisor holds its lock.
fn running(directory: &Path) -> Result<bool> {
	let Ok(lock) = File::open(directory.join("supervisor.lock")) else {
		return Ok(false);
	};
	// SAFETY: flock has no memory safety preconditions.
	let result = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) };
	if result == 0 {
		return Ok(false);
	}
	let error = io::Error::last_os_error();
	if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
		return Ok(true);
	}
	Err(error.into())
}

fn supervisor_pid(directory: &Path) -> Result<libc::pid_t> {
	let contents = std::fs::read_to_string(directory.join("pid"))?;
	let pid = contents
		.lines()
		.next()
		.ok_or("the pid file is empty")?
		.parse()?;
	Ok(pid)
}

/// Wait for the supervisor to release its lock. Return false if the timeout passes first.
fn wait_lock(directory: &Path, timeout: Duration) -> bool {
	let Ok(lock) = File::open(directory.join("supervisor.lock")) else {
		return true;
	};
	let (sender, receiver) = mpsc::channel();
	std::thread::spawn(move || {
		// SAFETY: flock has no memory safety preconditions.
		let result = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_SH) };
		sender.send(result).ok();
	});
	receiver.recv_timeout(timeout).is_ok()
}

/// Record a startup failure's message in the error file, and return an error that includes the process's output.
fn failure(directory: &Path, message: &str) -> crate::Error {
	std::fs::write(directory.join("error"), message).ok();
	let log = std::fs::read_to_string(directory.join("log")).unwrap_or_default();
	let log = log.trim();
	if log.is_empty() {
		message.into()
	} else {
		format!("{message}\noutput:\n{log}").into()
	}
}
