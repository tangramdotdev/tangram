use {
	std::{
		ffi::{CStr, CString, OsString},
		os::unix::ffi::OsStrExt as _,
		path::PathBuf,
		process::ExitCode,
	},
	tangram_client::prelude::*,
};

unsafe extern "C" {
	fn sandbox_init(
		profile: *const libc::c_char,
		flags: u64,
		errorbuf: *mut *const libc::c_char,
	) -> libc::c_int;
	fn sandbox_free_error(errorbuf: *const libc::c_char) -> libc::c_void;
}

#[derive(Clone, Debug)]
pub struct Arg {
	pub command: Vec<OsString>,
	pub profile: PathBuf,
}

pub fn run(arg: &Arg) -> tg::Result<ExitCode> {
	if arg.command.is_empty() {
		return Err(tg::error!("a command is required"));
	}
	let profile = std::fs::read(&arg.profile).map_err(|source| {
		tg::error!(
			!source,
			path = %arg.profile.display(),
			"failed to read the sandbox profile"
		)
	})?;
	let profile = CString::new(profile)
		.map_err(|source| tg::error!(!source, "failed to encode the sandbox profile"))?;
	let mut errorbuf = std::ptr::null();
	let result = unsafe { sandbox_init(profile.as_ptr(), 0, &raw mut errorbuf) };
	if result != 0 {
		let error = sandbox_error_message(errorbuf);
		return Err(tg::error!(error = %error, "failed to initialize the seatbelt sandbox"));
	}
	if !errorbuf.is_null() {
		unsafe {
			sandbox_free_error(errorbuf);
		}
	}
	exec_command(arg)?;
	unreachable!()
}

fn exec_command(arg: &Arg) -> tg::Result<()> {
	let executable = CString::new(arg.command[0].as_os_str().as_bytes())
		.map_err(|source| tg::error!(!source, "failed to encode the executable"))?;
	let argv = arg
		.command
		.iter()
		.map(|arg| {
			CString::new(arg.as_os_str().as_bytes())
				.map_err(|source| tg::error!(!source, "failed to encode an argument"))
		})
		.collect::<Result<Vec<_>, _>>()?;
	let mut argv = argv.iter().map(|arg| arg.as_ptr()).collect::<Vec<_>>();
	argv.push(std::ptr::null());
	unsafe {
		libc::execvp(executable.as_ptr(), argv.as_ptr());
	}
	let source = std::io::Error::last_os_error();
	Err(tg::error!(!source, "failed to execute the command"))
}

fn sandbox_error_message(errorbuf: *const libc::c_char) -> String {
	if errorbuf.is_null() {
		return "sandbox_init failed".to_owned();
	}
	let error = unsafe { CStr::from_ptr(errorbuf) }
		.to_string_lossy()
		.into_owned();
	unsafe {
		sandbox_free_error(errorbuf);
	}
	error
}
