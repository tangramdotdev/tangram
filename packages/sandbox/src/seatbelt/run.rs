use {
	std::{
		ffi::{CStr, CString},
		os::unix::process::CommandExt as _,
		path::PathBuf,
	},
	tangram_client::prelude::*,
};

unsafe extern "C" {
	fn sandbox_init(
		profile: *const libc::c_char,
		flags: u64,
		errorbuf: *mut *const libc::c_char,
	) -> libc::c_int;
	fn sandbox_free_error(errorbuf: *const libc::c_char);
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Arg {
	#[serde(default)]
	pub cleanup_fd: Option<libc::c_int>,

	pub profile: PathBuf,
}

pub fn run(command: &crate::run::Command, arg: &Arg) -> tg::Result<u8> {
	// Read the sandbox profile.
	let profile = std::fs::read(&arg.profile).map_err(|error| {
		tg::error!(
			!error,
			path = %arg.profile.display(),
			"failed to read the sandbox profile"
		)
	})?;
	let profile = CString::new(profile)
		.map_err(|error| tg::error!(!error, "failed to encode the sandbox profile"))?;

	// Initialize the seatbelt sandbox.
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

	// Start the descendant cleanup helper inside the sandbox policy.
	if let Some(fd) = arg.cleanup_fd {
		super::cleanup::start(fd)?;
	}

	// Execute the command.
	let mut command = command.command()?;
	let error = command.exec();

	Err(tg::error!(!error, "failed to execute the command"))
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
