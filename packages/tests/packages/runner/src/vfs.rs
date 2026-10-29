use {
	crate::Result,
	std::{path::Path, time::Duration},
	tokio::process::Command,
};

/// The name of the marker file that a test writes before starting a server with a VFS.
#[cfg(target_os = "macos")]
pub const MARKER: &str = ".tangram_test_vfs_cleanup";

/// Unmount every VFS left under a test's temporary directory. A server that exits cleanly unmounts itself, so this only catches the ones that crashed.
pub async fn unmount(path: &Path, fskit: bool) -> Result<()> {
	#[cfg(target_os = "linux")]
	{
		let _ = fskit;
		unmount_linux(path).await
	}

	#[cfg(target_os = "macos")]
	{
		if !fskit && !path.join(MARKER).exists() {
			return Ok(());
		}
		unmount_macos(path).await
	}
}

#[cfg(target_os = "linux")]
async fn unmount_linux(path: &Path) -> Result<()> {
	// Find the FUSE mounts under the path, deepest first.
	let mountinfo = tokio::fs::read_to_string("/proc/self/mountinfo").await?;
	let mut targets = mountinfo
		.lines()
		.filter_map(|line| {
			let (fields, rest) = line.split_once(" - ")?;
			let target = unescape(fields.split(' ').nth(4)?);
			let kind = rest.split(' ').next()?;
			kind.starts_with("fuse").then_some(target)
		})
		.filter(|target| Path::new(target).starts_with(path))
		.collect::<Vec<_>>();
	targets.sort_by_key(|target| std::cmp::Reverse(target.len()));

	// Lazily unmount each one.
	for target in targets {
		let output = Command::new("fusermount3")
			.args(["-u", "-z"])
			.arg(&target)
			.output();
		tokio::time::timeout(Duration::from_secs(5), output)
			.await
			.ok();
	}

	Ok(())
}

#[cfg(target_os = "macos")]
async fn unmount_macos(path: &Path) -> Result<()> {
	// Find the Tangram mounts under the path, deepest first.
	let output = Command::new("mount").output();
	let Ok(Ok(output)) = tokio::time::timeout(Duration::from_secs(5), output).await else {
		return Ok(());
	};
	let stdout = String::from_utf8_lossy(&output.stdout);
	let mut targets = stdout
		.lines()
		.filter_map(|line| {
			let (_, rest) = line.split_once(" on ")?;
			let (target, options) = rest.rsplit_once(" (")?;
			(options.starts_with("tangram,") || options.starts_with("tangram)"))
				.then(|| target.to_owned())
		})
		.filter(|target| Path::new(target).starts_with(path))
		.collect::<Vec<_>>();
	targets.sort_by_key(|target| std::cmp::Reverse(target.len()));

	// Forcibly unmount each one.
	for target in targets {
		let output = Command::new("umount").args(["-f", &target]).output();
		let output = tokio::time::timeout(Duration::from_secs(5), output)
			.await
			.map_err(|_| format!("timed out unmounting the test VFS: {target}"))??;
		if !output.status.success() {
			let stderr = String::from_utf8_lossy(&output.stderr);
			return Err(
				format!("failed to unmount the test VFS {target}: {}", stderr.trim()).into(),
			);
		}
	}

	Ok(())
}

/// Decode the octal escapes that mountinfo uses for spaces, tabs, newlines, and backslashes.
#[cfg(target_os = "linux")]
fn unescape(value: &str) -> String {
	let bytes = value.as_bytes();
	let mut output = Vec::with_capacity(bytes.len());
	let mut index = 0;
	while index < bytes.len() {
		if bytes[index] == b'\\'
			&& index + 3 < bytes.len()
			&& let Some(byte) = std::str::from_utf8(&bytes[index + 1..index + 4])
				.ok()
				.and_then(|digits| u8::from_str_radix(digits, 8).ok())
		{
			output.push(byte);
			index += 4;
		} else {
			output.push(bytes[index]);
			index += 1;
		}
	}
	String::from_utf8_lossy(&output).into_owned()
}
