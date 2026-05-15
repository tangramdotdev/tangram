use {
	std::process::ExitStatus,
	tokio::process::{ChildStdin, ChildStdout},
};

#[cfg(target_os = "linux")]
use {
	crate::rootless::client::Helper,
	std::{
		os::{
			fd::{FromRawFd as _, OwnedFd},
			unix::process::ExitStatusExt as _,
		},
		sync::Arc,
	},
	tangram_client::prelude::*,
};

/// An abstraction over a sandbox wrapper process. Variants:
///
/// - `Direct`: a `tokio::process::Child` spawned by the server. Used for
///   container sandboxes that do not need the rootless helper (root mode with
///   veth, host networking, vm isolation, seatbelt).
/// - `Helper`: a wrapper process forked from inside the rootless helper. Owned
///   by sandbox id; the helper's `destroy` request cleans up pasta, the
///   wrapper, and the per-sandbox netns.
pub(crate) enum ProcessHandle {
	Direct(tokio::process::Child),

	#[cfg(target_os = "linux")]
	Helper(HelperProcess),
}

#[cfg(target_os = "linux")]
pub(crate) struct HelperProcess {
	helper: Arc<Helper>,
	sandbox_id: String,
	wrapper_pid: libc::pid_t,
	pidfd: Option<tokio::io::unix::AsyncFd<OwnedFd>>,
	cached_status: Option<ExitStatus>,
}

impl ProcessHandle {
	pub(crate) async fn wait(&mut self) -> std::io::Result<ExitStatus> {
		match self {
			Self::Direct(child) => child.wait().await,
			#[cfg(target_os = "linux")]
			Self::Helper(helper) => helper.wait().await,
		}
	}

	pub(crate) fn start_kill(&mut self) -> std::io::Result<()> {
		match self {
			Self::Direct(child) => child.start_kill(),
			#[cfg(target_os = "linux")]
			Self::Helper(helper) => helper.start_kill(),
		}
	}

	pub(crate) fn take_stdin(&mut self) -> Option<ChildStdin> {
		match self {
			Self::Direct(child) => child.stdin.take(),
			#[cfg(target_os = "linux")]
			Self::Helper(_) => None,
		}
	}

	pub(crate) fn take_stdout(&mut self) -> Option<ChildStdout> {
		match self {
			Self::Direct(child) => child.stdout.take(),
			#[cfg(target_os = "linux")]
			Self::Helper(_) => None,
		}
	}
}

#[cfg(target_os = "linux")]
impl HelperProcess {
	/// Create a new helper process handle. Opens a `pidfd` on the wrapper so the
	/// server can poll for exit asynchronously without blocking the helper's
	/// control socket.
	pub(crate) fn new(
		helper: Arc<Helper>,
		sandbox_id: String,
		wrapper_pid: libc::pid_t,
	) -> tg::Result<Self> {
		let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, wrapper_pid, 0) };
		if raw < 0 {
			let error = std::io::Error::last_os_error();
			return Err(tg::error!(
				!error,
				%wrapper_pid,
				"failed to open the wrapper pidfd"
			));
		}
		let fd = i32::try_from(raw)
			.map_err(|source| tg::error!(!source, "the pidfd does not fit in an i32"))?;
		let fd = unsafe { OwnedFd::from_raw_fd(fd) };
		let pidfd = tokio::io::unix::AsyncFd::new(fd).map_err(|error| {
			tg::error!(!error, "failed to register the wrapper pidfd with tokio")
		})?;
		Ok(Self {
			helper,
			sandbox_id,
			wrapper_pid,
			pidfd: Some(pidfd),
			cached_status: None,
		})
	}

	async fn wait(&mut self) -> std::io::Result<ExitStatus> {
		if let Some(status) = self.cached_status {
			return Ok(status);
		}
		let pidfd = self
			.pidfd
			.as_mut()
			.ok_or_else(|| std::io::Error::other("the wrapper has already been waited on"))?;
		let _ = pidfd.readable_mut().await?;
		let exit_code = self
			.helper
			.wait_wrapper(&self.sandbox_id)
			.map_err(|error| std::io::Error::other(error.to_string()))?;
		let status = exit_status_from_code(exit_code);
		self.cached_status = Some(status);
		self.pidfd.take();
		Ok(status)
	}

	fn start_kill(&mut self) -> std::io::Result<()> {
		if self.cached_status.is_some() {
			return Ok(());
		}
		let result = unsafe { libc::kill(self.wrapper_pid, libc::SIGKILL) };
		if result < 0 {
			let error = std::io::Error::last_os_error();
			// ESRCH means the process is already gone. Tokio's Child::start_kill
			// is also tolerant of this case.
			if error.raw_os_error() == Some(libc::ESRCH) {
				return Ok(());
			}
			return Err(error);
		}
		Ok(())
	}
}

#[cfg(target_os = "linux")]
impl Drop for HelperProcess {
	fn drop(&mut self) {
		// Tell the helper to tear down the per-sandbox state: pasta, the netns
		// bind-mount, and the netns-holder. If the wrapper has not been waited
		// on, the helper will reap it as part of cleanup.
		let _ = self.helper.destroy(&self.sandbox_id);
	}
}

/// Convert the helper's `wait_wrapper` exit code (i32) back into an
/// `ExitStatus`. The helper returns:
/// - `0..=255` for a normal exit with that code (packed into bits 8..15).
/// - `128 + sig` for a signaled exit (sig packed into bits 0..6).
/// - `-1` for an unknown waitpid status (mapped to exit code 255).
#[cfg(target_os = "linux")]
fn exit_status_from_code(code: i32) -> ExitStatus {
	if (0..=255).contains(&code) {
		ExitStatus::from_raw(code << 8)
	} else if (128..256).contains(&code) {
		let sig = code - 128;
		ExitStatus::from_raw(sig & 0x7f)
	} else {
		ExitStatus::from_raw(0xff << 8)
	}
}
