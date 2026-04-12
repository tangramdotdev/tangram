use {std::process::ExitCode, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub struct Arg {
	pub serve: crate::serve::Arg,
}

pub fn run(arg: &Arg) -> tg::Result<ExitCode> {
	let mut command = std::process::Command::new(&arg.serve.tangram_path);
	command
		.arg("sandbox")
		.arg("serve")
		.arg("--url")
		.arg(arg.serve.url.to_string())
		.arg("--tangram-path")
		.arg(&arg.serve.tangram_path)
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit());
	for path in &arg.serve.library_paths {
		command.arg("--library-path").arg(path);
	}
	let child = command
		.spawn()
		.map_err(|source| tg::error!(!source, "failed to spawn the sandbox server"))?;
	let pid: libc::pid_t = child
		.id()
		.try_into()
		.map_err(|_| tg::error!("failed to get the sandbox server pid"))?;

	loop {
		let (reaped_pid, status) = wait_for_child()?;
		if reaped_pid == pid {
			return Ok(ExitCode::from(status));
		}
	}
}

fn wait_for_child() -> tg::Result<(libc::pid_t, u8)> {
	loop {
		let mut status = 0;
		let pid = unsafe { libc::waitpid(-1, std::ptr::addr_of_mut!(status), 0) };
		if pid >= 0 {
			return Ok((pid, exit_code_from_status(status)));
		}
		let source = std::io::Error::last_os_error();
		if source.raw_os_error() == Some(libc::EINTR) {
			continue;
		}
		return Err(tg::error!(!source, "failed to wait for a sandbox child"));
	}
}

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
