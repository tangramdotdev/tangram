use {
	super::{
		helper::{build_helper_command, populate_helper_namespace, socketpair},
		protocol::{
			self, OP_DESTROY, OP_SHUTDOWN, OP_SPAWN_NETNS, OP_SPAWN_PASTA, OP_SPAWN_WRAPPER,
			OP_WAIT_WRAPPER, Request, STATUS_OK, encode_spawn_payload, write_request,
		},
		subuid::UserRanges,
	},
	std::{
		os::unix::{net::UnixStream, process::CommandExt as _},
		path::{Path, PathBuf},
		process::Child,
		sync::Mutex,
	},
	tangram_client::prelude::*,
};

/// Server-side handle to the rootless helper subprocess. Owns the helper
/// process and the control socket. Sends requests synchronously; for now,
/// concurrent callers are serialized by a `Mutex` over the socket.
pub struct Helper {
	child: Child,
	control: Mutex<UnixStream>,
}

impl Helper {
	/// Spawn the helper subprocess, complete the `newuidmap`/`newgidmap`
	/// handshake, and return a handle. Blocking — must run in
	/// `tokio::task::spawn_blocking` or similar from async contexts.
	pub fn start(tangram_path: &Path) -> tg::Result<Self> {
		let ranges = UserRanges::lookup()?;
		let (parent, child_fd) = socketpair()?;
		let mut command = build_helper_command(tangram_path, child_fd);
		// Ensure the child fd is not closed before exec. The `socketpair`
		// helper already cleared FD_CLOEXEC; this `pre_exec` is a no-op safety
		// net for any future fd cleanup we add.
		unsafe {
			command.pre_exec(|| Ok(()));
		}
		// Inherit stdio so the helper's stderr surfaces in the server's logs.
		command
			.stdin(std::process::Stdio::null())
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::inherit());
		let mut child = command
			.spawn()
			.map_err(|error| tg::error!(!error, "failed to spawn the rootless helper"))?;
		// Close our copy of the child fd so only the helper holds it.
		unsafe {
			libc::close(child_fd);
		}
		let helper_pid = libc::pid_t::try_from(child.id())
			.map_err(|source| tg::error!(!source, "invalid helper pid"))?;

		let mut control = parent;
		populate_helper_namespace(&mut control, helper_pid, &ranges).inspect_err(|_| {
			let _ = child.kill();
		})?;

		Ok(Self {
			child,
			control: Mutex::new(control),
		})
	}

	/// Ask the helper to create a fresh netns for the given sandbox. Returns
	/// the path of the bind-mounted netns file, visible inside the helper's
	/// mount namespace.
	pub fn spawn_netns(&self, sandbox_id: &str) -> tg::Result<PathBuf> {
		let response = self.request(&Request {
			opcode: OP_SPAWN_NETNS,
			payload: sandbox_id.as_bytes().to_vec(),
		})?;
		if response.status != STATUS_OK {
			let message = String::from_utf8_lossy(&response.payload);
			return Err(tg::error!("the helper failed to spawn netns: {message}"));
		}
		let path = std::str::from_utf8(&response.payload)
			.map_err(|source| tg::error!(!source, "invalid netns path encoding"))?;
		Ok(PathBuf::from(path))
	}

	/// Ask the helper to fork+exec pasta with the given argv inside the helper's
	/// user and mount namespaces. Returns the pasta pid.
	pub fn spawn_pasta(&self, sandbox_id: &str, argv: &[String]) -> tg::Result<libc::pid_t> {
		let payload = encode_spawn_payload(sandbox_id, argv);
		let response = self.request(&Request {
			opcode: OP_SPAWN_PASTA,
			payload,
		})?;
		if response.status != STATUS_OK {
			let message = String::from_utf8_lossy(&response.payload);
			return Err(tg::error!("the helper failed to spawn pasta: {message}"));
		}
		decode_pid(&response.payload)
	}

	/// Ask the helper to fork+exec the wrapper inside the helper's user
	/// namespace and the sandbox's netns. Returns the wrapper pid.
	pub fn spawn_wrapper(&self, sandbox_id: &str, argv: &[String]) -> tg::Result<libc::pid_t> {
		let payload = encode_spawn_payload(sandbox_id, argv);
		let response = self.request(&Request {
			opcode: OP_SPAWN_WRAPPER,
			payload,
		})?;
		if response.status != STATUS_OK {
			let message = String::from_utf8_lossy(&response.payload);
			return Err(tg::error!("the helper failed to spawn wrapper: {message}"));
		}
		decode_pid(&response.payload)
	}

	/// Block-wait for the wrapper to exit. Returns the exit code (0–255 for
	/// normal exit, 128+signal for signal-killed). Serializes across the
	/// control socket — for an async-friendly variant a future iteration can
	/// return a `pidfd` via `SCM_RIGHTS` so the server polls without blocking
	/// the helper's request loop.
	pub fn wait_wrapper(&self, sandbox_id: &str) -> tg::Result<i32> {
		let response = self.request(&Request {
			opcode: OP_WAIT_WRAPPER,
			payload: sandbox_id.as_bytes().to_vec(),
		})?;
		if response.status != STATUS_OK {
			let message = String::from_utf8_lossy(&response.payload);
			return Err(tg::error!(
				"the helper failed to wait for wrapper: {message}"
			));
		}
		if response.payload.len() != 4 {
			return Err(tg::error!(
				len = %response.payload.len(),
				"expected a 4-byte exit code"
			));
		}
		Ok(i32::from_le_bytes(
			response.payload[..4].try_into().unwrap(),
		))
	}

	/// Ask the helper to tear down a sandbox's netns.
	pub fn destroy(&self, sandbox_id: &str) -> tg::Result<()> {
		let response = self.request(&Request {
			opcode: OP_DESTROY,
			payload: sandbox_id.as_bytes().to_vec(),
		})?;
		if response.status != STATUS_OK {
			let message = String::from_utf8_lossy(&response.payload);
			return Err(tg::error!("the helper failed to destroy netns: {message}"));
		}
		Ok(())
	}

	/// Tell the helper to shut down cleanly. The helper will tear down all
	/// remaining sandboxes and exit.
	pub fn shutdown(&self) -> tg::Result<()> {
		let response = self.request(&Request {
			opcode: OP_SHUTDOWN,
			payload: Vec::new(),
		})?;
		if response.status != STATUS_OK {
			let message = String::from_utf8_lossy(&response.payload);
			return Err(tg::error!("the helper failed to shut down: {message}"));
		}
		Ok(())
	}

	fn request(&self, request: &Request) -> tg::Result<protocol::Response> {
		let mut control = self
			.control
			.lock()
			.map_err(|_| tg::error!("the helper control mutex was poisoned"))?;
		write_request(&mut *control, request)?;
		protocol::read_response(&mut *control)
	}
}

fn decode_pid(payload: &[u8]) -> tg::Result<libc::pid_t> {
	if payload.len() != 4 {
		return Err(tg::error!(len = %payload.len(), "expected a 4-byte pid"));
	}
	Ok(libc::pid_t::from_le_bytes(payload[..4].try_into().unwrap()))
}

impl Drop for Helper {
	fn drop(&mut self) {
		let _ = self.shutdown();
		let _ = self.child.kill();
		let _ = self.child.wait();
	}
}
