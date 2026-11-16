use std::{
	io,
	process::{ExitStatus, Output, Stdio},
	sync::{PoisonError, RwLock, RwLockReadGuard},
};

static LOCK: RwLock<()> = RwLock::new(());

/// Spawn a child process. Spawns run concurrently with each other, and `wait` waits for them.
#[expect(
	clippy::disallowed_methods,
	reason = "this is the spawn that a file write can wait for"
)]
pub fn spawn(command: &mut std::process::Command) -> io::Result<std::process::Child> {
	let _guard = guard();
	command.spawn()
}

/// Spawn an asynchronous child process. Spawns run concurrently with each other, and `wait` waits for them.
#[expect(
	clippy::disallowed_methods,
	reason = "this is the spawn that a file write can wait for"
)]
pub fn spawn_async(command: &mut tokio::process::Command) -> io::Result<tokio::process::Child> {
	let _guard = guard();
	command.spawn()
}

/// Run a command and capture its output. A call of `wait` waits for the command's spawn, but not for the command to exit.
pub fn output(command: &mut std::process::Command) -> io::Result<Output> {
	command
		.stdin(Stdio::null())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped());
	let child = spawn(command)?;
	let output = child.wait_with_output()?;
	Ok(output)
}

/// Run an asynchronous command and capture its output. A call of `wait` waits for the command's spawn, but not for the command to exit.
pub async fn output_async(command: &mut tokio::process::Command) -> io::Result<Output> {
	command
		.stdin(Stdio::null())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped());
	let child = spawn_async(command)?;
	let output = child.wait_with_output().await?;
	Ok(output)
}

/// Run a command and get its exit status. A call of `wait` waits for the command's spawn, but not for the command to exit.
pub fn status(command: &mut std::process::Command) -> io::Result<ExitStatus> {
	let mut child = spawn(command)?;
	let status = child.wait()?;
	Ok(status)
}

/// Run an asynchronous command and get its exit status. A call of `wait` waits for the command's spawn, but not for the command to exit.
pub async fn status_async(command: &mut tokio::process::Command) -> io::Result<ExitStatus> {
	let mut child = spawn_async(command)?;
	let status = child.wait().await?;
	Ok(status)
}

/// Wait for the spawns in progress to finish. A child process holds copies of its parent's file descriptors from the time it forks until it executes its program, and the kernel refuses to execute a file that any descriptor holds open for writing, so call this after closing a written file and before another process may execute it.
pub fn wait() {
	drop(LOCK.write().unwrap_or_else(PoisonError::into_inner));
}

/// Hold off a call of `wait` until the guard is dropped. A spawn holds the guard until its child has executed its program or failed to, which is when `Command::spawn` returns.
fn guard() -> RwLockReadGuard<'static, ()> {
	LOCK.read().unwrap_or_else(PoisonError::into_inner)
}
