use {
	std::{
		ffi::{CStr, OsStr, c_int, c_void},
		os::unix::ffi::OsStrExt as _,
		path::PathBuf,
	},
	tangram_client::prelude::*,
};

#[expect(clippy::unnecessary_wraps)]
pub(super) fn resolve() -> tg::Result<Vec<PathBuf>> {
	let mut libraries = Vec::new();
	let data = std::ptr::from_mut(&mut libraries).cast::<c_void>();
	// SAFETY: `data` points to `libraries`, which outlives the call because `dl_iterate_phdr` invokes the callback synchronously.
	unsafe {
		libc::dl_iterate_phdr(Some(callback), data);
	}
	Ok(libraries)
}

unsafe extern "C" fn callback(
	info: *mut libc::dl_phdr_info,
	_size: usize,
	data: *mut c_void,
) -> c_int {
	// SAFETY: `data` is the pointer to `libraries` passed to `dl_iterate_phdr`, and `info` and its name are valid for the duration of the callback.
	let (libraries, name) = unsafe {
		let libraries = &mut *data.cast::<Vec<PathBuf>>();
		let name = (*info).dlpi_name;
		if name.is_null() {
			return 0;
		}
		(libraries, CStr::from_ptr(name))
	};
	let path = PathBuf::from(OsStr::from_bytes(name.to_bytes()));
	// The main executable is reported with an empty name and the vDSO with a bare name, so keep only the paths the loader resolved.
	if path.is_absolute() {
		libraries.push(path);
	}
	0
}
