use bytes::Bytes;
use futures::future;
use std::{
	io::{IsTerminal as _, Read as _, Write as _},
	mem::MaybeUninit,
	process::ExitCode,
};
use tangram_futures::task::{Stop, Task};
use tangram_sandbox::{self as sandbox, ExitStatus, Stdio, Tty};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

#[tokio::main]
async fn main() -> std::io::Result<ExitCode> {
	// Enable raw mode and add a scope guard to disable it.
	let stdin = std::io::stdin()
		.is_terminal()
		.then(|| enable_raw_mode(libc::STDIN_FILENO))
		.transpose()?;
	scopeguard::defer! {
		if let Some(tstdin) = stdin {
			disable_raw_mode(libc::STDIN_FILENO, tstdin);
		}
	}

	// Create the command.
	let mut command = sandbox::Command::new("/bin/cat");
	if let Some(tty) = get_window_size(libc::STDIN_FILENO)? {
		command.stdin(Stdio::Tty(tty)).stdout(Stdio::Tty(tty));
	} else {
		command.stdin(Stdio::Pipe).stdout(Stdio::Pipe);
	}
	command.envs(std::env::vars_os());

	// Spawn the command.
	let mut child = command.spawn().await?;

	// Create tasks to read from stdin and write to stdout.
	let stdin = Task::spawn(|stop| stdin_task(child.stdin.take().unwrap(), stop));
	let stdout = tokio::spawn(stdout_task(child.stdout.take().unwrap()));

	// Wait for the child process.
	let result = child.wait().await;

	// Await the stdio tasks.
	stdin.stop();
	stdout.await.unwrap();
	stdin.wait().await.unwrap();

	let exit = result?;

	// Get the exit code.
	match exit {
		ExitStatus::Code(code) => {
			println!("code: {code}");
			if code == 0 {
				Ok(ExitCode::SUCCESS)
			} else {
				Ok(ExitCode::FAILURE)
			}
		},
		ExitStatus::Signal(signal) => {
			println!("signal: {signal}");
			Ok(ExitCode::FAILURE)
		},
	}
}

fn enable_raw_mode(fd: i32) -> std::io::Result<libc::termios> {
	unsafe {
		let mut old = MaybeUninit::uninit();
		if libc::tcgetattr(fd, old.as_mut_ptr()) != 0 {
			return Err(std::io::Error::last_os_error());
		}
		let old = old.assume_init();
		let mut new = old;
		new.c_lflag &= !(libc::ECHO | libc::ICANON | libc::ISIG | libc::IEXTEN);
		new.c_iflag &= !(libc::IXON | libc::ICRNL | libc::BRKINT | libc::INPCK | libc::ISTRIP);
		new.c_oflag &= !(libc::OPOST);
		if libc::tcsetattr(fd, libc::TCSAFLUSH, std::ptr::addr_of!(new)) != 0 {
			return Err(std::io::Error::last_os_error());
		}
		Ok(old)
	}
}

fn disable_raw_mode(fd: i32, old: libc::termios) {
	unsafe {
		libc::tcsetattr(fd, libc::TCSANOW, std::ptr::addr_of!(old));
	}
}

fn get_window_size(fd: i32) -> std::io::Result<Option<Tty>> {
	unsafe {
		if libc::isatty(fd) == 0 {
			return Ok(None);
		}
		let mut winsize = libc::winsize {
			ws_col: 0,
			ws_row: 0,
			ws_xpixel: 0,
			ws_ypixel: 0,
		};
		if libc::ioctl(fd, libc::TIOCGWINSZ, std::ptr::addr_of_mut!(winsize)) != 0 {
			let error = std::io::Error::last_os_error();
			return Err(error);
		}
		Ok(Some(Tty {
			rows: winsize.ws_row,
			cols: winsize.ws_col,
		}))
	}
}

async fn stdin_task(mut stdin: impl AsyncWrite + Unpin, stop: Stop) {
	let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
	std::thread::spawn(move || {
		let mut buf = vec![0u8; 256];
		loop {
			match std::io::stdin().read(&mut buf) {
				Err(_) | Ok(0) => break,
				Ok(n) => {
					let message = Bytes::from(buf[0..n].to_vec());
					if sender.blocking_send(message).is_err() {
						break;
					}
				},
			}
		}
	});
	while let future::Either::Right((Some(message), _)) =
		future::select(std::pin::pin!(stop.wait()), std::pin::pin!(receiver.recv())).await
	{
		stdin.write_all(&message).await.unwrap();
	}
	stdin.shutdown().await.unwrap();
}

async fn stdout_task(mut stdout: impl AsyncRead + Unpin) {
	let (sender, mut receiver) = tokio::sync::mpsc::channel::<Bytes>(1);
	tokio::task::spawn_blocking(move || {
		while let Some(message) = receiver.blocking_recv() {
			std::io::stdout().write_all(&message).unwrap();
			std::io::stdout().flush().unwrap();
		}
	});
	let mut buf = vec![0u8; 256];
	loop {
		match stdout.read(&mut buf).await {
			Err(_) | Ok(0) => break,
			Ok(n) => {
				let chunk = Bytes::from(buf[0..n].to_vec());
				sender.send(chunk).await.ok();
			},
		}
	}
}
