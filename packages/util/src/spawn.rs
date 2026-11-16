use std::sync::{PoisonError, RwLock};

static LOCK: RwLock<()> = RwLock::new(());

/// Spawn a child process with a function that returns once the child has executed its program or failed to, as `std::process::Command::spawn` does. Spawns run concurrently with each other, and `wait` waits for them.
pub fn spawn<T>(f: impl FnOnce() -> T) -> T {
	let _guard = LOCK.read().unwrap_or_else(PoisonError::into_inner);
	f()
}

/// Wait for the spawns in progress to finish. A child process holds copies of its parent's file descriptors from the time it forks until it executes its program, and the kernel refuses to execute a file that any descriptor holds open for writing, so call this after closing a written file and before another process may execute it.
pub fn wait() {
	drop(LOCK.write().unwrap_or_else(PoisonError::into_inner));
}
