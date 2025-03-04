use bytes::Bytes;
use futures::future;
use std::{
	io::{IsTerminal, Read, Write},
	mem::MaybeUninit,
	process::ExitCode,
};
use tangram_futures::task::{Stop, Task};
use tangram_sandbox::{self as sandbox, ExitStatus, Stdio, Tty};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[tokio::main]
async fn main() -> std::io::Result<ExitCode> {
	let result = {
		// Enable raw mode and add a scope guard to disable it.
		let tstdin = std::io::stdin()
			.is_terminal()
			.then(|| enable_raw_mode(libc::STDIN_FILENO))
			.transpose()?;
		scopeguard::defer! {
			if let Some(tstdin) = tstdin {
				disable_raw_mode(libc::STDIN_FILENO, tstdin);
			}
		}

		// Create the command.
		let mut command = sandbox::Command::new("/bin/cat");
		if let Some(tty) = get_window_size(libc::STDIN_FILENO)? {
			command.stdin(Stdio::Tty(tty)).stdout(Stdio::Tty(tty));
			// .stderr(Stdio::Piped);
		} else {
			command.stdin(Stdio::Piped).stdout(Stdio::Piped);
			// .stderr(Stdio::Piped);
		}
		command.envs(std::env::vars_os());

		#[cfg(target_os = "macos")]
		command.sandbox(false);

		// Spawn the command.
		let mut child = command.spawn().await?;

		// Create tasks to read from stdin and write to stdout.
		let stdin = Task::spawn(|stop| stdin(child.stdin.take().unwrap(), stop));
		let stdout = tokio::task::spawn(stdout(child.stdout.take().unwrap()));

		// Wait for the child process.
		let result = child.wait().await;

		// Await i/o tasks.
		stdin.stop();
		stdout.await.unwrap();
		stdin.wait().await.unwrap();
		result?
	};

	// Get the exit code.
	match result {
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
		return Ok(old);
	}
}

fn disable_raw_mode(fd: i32, old: libc::termios) {
	unsafe {
		if libc::tcsetattr(fd, libc::TCSANOW, std::ptr::addr_of!(old)) != 0 {
			eprintln!(
				"failed to disable raw mode: {}",
				std::io::Error::last_os_error()
			);
		}
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
			eprintln!("ioctl failed");
			let error = std::io::Error::last_os_error();
			return Err(error);
		}
		Ok(Some(Tty {
			rows: winsize.ws_row,
			cols: winsize.ws_col,
			x: winsize.ws_xpixel,
			y: winsize.ws_ypixel,
		}))
	}
}

async fn stdin(mut stdin: impl AsyncWrite + Unpin, stop: Stop) {
	let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
	std::thread::spawn(move || {
		let mut buf = vec![0u8; 256];
		loop {
			match std::io::stdin().read(&mut buf) {
				Ok(0) => break,
				Ok(n) => {
					let message = Bytes::from(buf[0..n].to_vec());
					if sender.blocking_send(message).is_err() {
						break;
					}
				},
				Err(_) => break,
			}
		}
	});
	loop {
		match future::select(std::pin::pin!(stop.wait()), std::pin::pin!(receiver.recv())).await {
			future::Either::Right((Some(message), _)) => {
				stdin.write_all(&message).await.unwrap();
			},
			_ => break,
		}
	}
	stdin.shutdown().await.unwrap();
}

async fn stdout(mut stdout: impl AsyncRead + Unpin) {
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
			Ok(0) => break,
			Ok(n) => {
				let chunk = Bytes::from(buf[0..n].to_vec());
				sender.send(chunk).await.ok();
			},
			Err(_) => break,
		}
	}
}
