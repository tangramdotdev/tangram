use {
	std::{
		ffi::CStr,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

const SUBUID_PATH: &str = "/etc/subuid";
const SUBGID_PATH: &str = "/etc/subgid";

#[derive(Clone, Copy, Debug)]
pub struct Range {
	pub start: u32,
	pub count: u32,
}

/// The configured subordinate uid/gid ranges for the current user.
#[derive(Clone, Debug)]
pub struct UserRanges {
	pub username: String,
	pub uid: Range,
	pub gid: Range,
}

impl UserRanges {
	/// Look up the current user's `/etc/subuid` and `/etc/subgid` entries.
	/// Returns a clear, actionable error if either is missing.
	pub fn lookup() -> tg::Result<Self> {
		let username = current_username()?;
		let uid = lookup_first(Path::new(SUBUID_PATH), &username)?
			.ok_or_else(|| missing_error(&username, SUBUID_PATH))?;
		let gid = lookup_first(Path::new(SUBGID_PATH), &username)?
			.ok_or_else(|| missing_error(&username, SUBGID_PATH))?;
		Ok(Self { username, uid, gid })
	}
}

fn current_username() -> tg::Result<String> {
	let uid = unsafe { libc::geteuid() };
	let passwd = unsafe { libc::getpwuid(uid) };
	if passwd.is_null() {
		return Err(tg::error!("failed to resolve the current user"));
	}
	let name_ptr = unsafe { (*passwd).pw_name };
	if name_ptr.is_null() {
		return Err(tg::error!("the current user has no name"));
	}
	let name = unsafe { CStr::from_ptr(name_ptr) }
		.to_str()
		.map_err(|source| tg::error!(!source, "the current username is not valid utf-8"))?
		.to_owned();
	Ok(name)
}

fn lookup_first(path: &Path, username: &str) -> tg::Result<Option<Range>> {
	let path = PathBuf::from(path);
	let contents = match std::fs::read_to_string(&path) {
		Ok(contents) => contents,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
		Err(source) => {
			return Err(
				tg::error!(!source, path = %path.display(), "failed to read the subuid file"),
			);
		},
	};
	for line in contents.lines() {
		let line = line.trim();
		if line.is_empty() || line.starts_with('#') {
			continue;
		}
		let mut parts = line.split(':');
		let name = parts.next();
		let start = parts.next().and_then(|s| s.parse::<u32>().ok());
		let count = parts.next().and_then(|s| s.parse::<u32>().ok());
		if let (Some(name), Some(start), Some(count)) = (name, start, count)
			&& name == username
		{
			return Ok(Some(Range { start, count }));
		}
	}
	Ok(None)
}

fn missing_error(username: &str, path: &str) -> tg::Error {
	tg::error!(
		"the current user {username} has no entry in {path}. tangram requires a subordinate uid/gid range for rootless container networking. run 'sudo usermod --add-subuids 100000-165535 {username} && sudo usermod --add-subgids 100000-165535 {username}' and try again."
	)
}
