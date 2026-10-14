use {std::process::ExitCode, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub enum Arg {
	#[cfg(target_os = "linux")]
	Builtin(crate::builtin::init::Arg),

	#[cfg(target_os = "linux")]
	Bubblewrap(crate::bubblewrap::init::Arg),

	#[cfg(target_os = "linux")]
	CloudHypervisor(crate::cloud_hypervisor::init::Arg),

	#[cfg(target_os = "linux")]
	Gvisor(crate::gvisor::init::Arg),

	#[cfg(target_os = "macos")]
	Seatbelt(crate::seatbelt::init::Arg),
}

pub fn run(arg: &Arg) -> tg::Result<ExitCode> {
	match arg {
		#[cfg(target_os = "linux")]
		Arg::Builtin(arg) => crate::builtin::init::run(arg),

		#[cfg(target_os = "linux")]
		Arg::Bubblewrap(arg) => crate::bubblewrap::init::run(arg),

		#[cfg(target_os = "linux")]
		Arg::CloudHypervisor(arg) => crate::cloud_hypervisor::init::run(arg),

		#[cfg(target_os = "linux")]
		Arg::Gvisor(arg) => crate::gvisor::init::run(arg),

		#[cfg(target_os = "macos")]
		Arg::Seatbelt(arg) => crate::seatbelt::init::run(arg),
	}
}

#[must_use]
pub fn command(arg: &crate::serve::Arg) -> std::process::Command {
	// Create the sandbox server command.
	let mut command = std::process::Command::new(&arg.tangram_path);
	command
		.arg("sandbox")
		.arg("serve")
		.arg(if arg.listen { "--listen" } else { "--connect" })
		.arg("--output-path")
		.arg(&arg.output_path)
		.arg("--url")
		.arg(arg.url.to_string())
		.arg("--tangram-path")
		.arg(&arg.tangram_path)
		.args(crate::util::http2_args(&arg.http2));

	// Supply the Tangram server environment defaults.
	command.env("SSL_CERT_DIR", crate::Sandbox::guest_ssl_cert_dir());
	if let Some(home) = std::env::var_os("HOME") {
		command.env("HOME", home);
	}

	// Add the runtime library paths.
	for path in &arg.library_paths {
		command.arg("--library-path").arg(path);
	}

	command
}

#[cfg(target_os = "linux")]
pub(crate) fn serve(arg: &crate::serve::Arg) -> tg::Result<ExitCode> {
	// Prevent same-UID workloads from inspecting PID 1 through /proc.
	crate::util::set_dumpable(false)?;
	let mut command = command(arg);
	let child = command
		.spawn()
		.map_err(|error| tg::error!(!error, "failed to spawn the sandbox server"))?;
	let pid: libc::pid_t = child
		.id()
		.try_into()
		.map_err(|_| tg::error!("failed to get the sandbox server pid"))?;

	// Reap the sandbox processes until the server exits.
	loop {
		let (reaped_pid, status) = wait_for_child()?;
		if reaped_pid == pid {
			return Ok(ExitCode::from(status));
		}
	}
}

#[cfg(target_os = "linux")]
fn wait_for_child() -> tg::Result<(libc::pid_t, u8)> {
	loop {
		let mut status = 0;
		let pid = unsafe { libc::waitpid(-1, std::ptr::addr_of_mut!(status), 0) };
		if pid >= 0 {
			return Ok((pid, exit_code_from_status(status)));
		}
		let error = std::io::Error::last_os_error();
		if error.raw_os_error() == Some(libc::EINTR) {
			continue;
		}
		return Err(tg::error!(!error, "failed to wait for a sandbox child"));
	}
}

#[cfg(target_os = "linux")]
fn exit_code_from_status(status: libc::c_int) -> u8 {
	if libc::WIFEXITED(status) {
		return u8::try_from(libc::WEXITSTATUS(status).min(255)).unwrap_or(u8::MAX);
	}
	if libc::WIFSIGNALED(status) {
		let signal = libc::WTERMSIG(status);
		return u8::try_from((128 + signal).min(255)).unwrap_or(u8::MAX);
	}
	1
}
