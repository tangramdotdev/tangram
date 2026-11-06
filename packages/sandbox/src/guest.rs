use {
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

/// The directory that holds the paths that the sandbox mounts into every native guest.
const TANGRAM_PATH: &str = "/opt/tangram";

/// The mandatory guest paths that a custom mount may not replace.
const RESERVED_PATHS: [&str; 3] = ["/opt/tangram/output", "/opt/tangram/socket", "/socket"];

/// The mandatory guest directories that a custom mount may neither replace nor mount inside.
const RESERVED_DIRECTORIES: [&str; 5] = [
	"/opt/tangram/bin",
	"/opt/tangram/etc",
	"/opt/tangram/lib",
	"/opt/tangram/libexec",
	"/opt/tangram/store",
];

/// The guest directories that come from the root filesystem rather than a mount of their own.
const ROOTFS_DIRECTORIES: [&str; 3] = ["/opt/tangram/bin", "/opt/tangram/etc", "/opt/tangram/lib"];

/// Reject the custom mounts of a native sandbox that cannot coexist with its mandatory paths.
pub fn validate_mounts(mounts: &[tg::sandbox::Mount]) -> tg::Result<()> {
	for mount in mounts {
		let target = &mount.target;
		let reserved = RESERVED_PATHS.iter().any(|path| target == Path::new(path))
			|| RESERVED_DIRECTORIES
				.iter()
				.any(|path| target.starts_with(path));
		if reserved {
			return Err(tg::error!(
				target = %target.display(),
				"the mount target is reserved for the sandbox"
			));
		}
		if mount.readonly && Path::new(TANGRAM_PATH).starts_with(target) {
			return Err(tg::error!(
				target = %target.display(),
				"a read-only mount cannot contain {TANGRAM_PATH}, which the sandbox mounts its own paths into, so make the mount writable or choose another target"
			));
		}
	}
	Ok(())
}

/// Get the root filesystem directories that the mounts would cover, which must be mounted again on top of them.
pub(crate) fn covered_rootfs_directories(
	root: &Path,
	mounts: &[tg::sandbox::Mount],
) -> Vec<PathBuf> {
	ROOTFS_DIRECTORIES
		.iter()
		.map(PathBuf::from)
		.filter(|path| mounts.iter().any(|mount| path.starts_with(&mount.target)))
		.filter(|path| rootfs_path(root, path).exists())
		.collect()
}

/// Get the host path of a guest path in the root filesystem.
pub(crate) fn rootfs_path(root: &Path, path: &Path) -> PathBuf {
	root.join(path.strip_prefix("/").unwrap_or(path))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn mount(target: &str, readonly: bool) -> tg::sandbox::Mount {
		tg::sandbox::Mount {
			readonly,
			source: "/source".into(),
			target: target.into(),
		}
	}

	#[test]
	fn validate_mounts_allows_what_the_sandbox_can_support() {
		for (target, readonly) in [
			("/opt", false),
			("/opt/tangram", false),
			("/opt/other", true),
			("/opt/tangram/output/extra", false),
			("/opt/tangramfoo", true),
		] {
			validate_mounts(&[mount(target, readonly)]).unwrap();
		}
	}

	#[test]
	fn validate_mounts_rejects_what_the_sandbox_cannot_support() {
		for (target, readonly) in [
			("/", true),
			("/opt", true),
			("/opt/tangram", true),
			("/opt/tangram/bin", false),
			("/opt/tangram/output", false),
			("/opt/tangram/socket", false),
			("/opt/tangram/store", true),
			("/opt/tangram/store/nested", false),
			("/socket", false),
		] {
			validate_mounts(&[mount(target, readonly)]).unwrap_err();
		}
	}
}
