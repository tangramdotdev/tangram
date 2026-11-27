use {
	std::{fmt::Write as _, path::Path},
	tangram_client::prelude::*,
};

/// An extent of an identity map, which maps `count` sandbox identities from `inside` to host identities from `outside`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Extent {
	count: u32,
	inside: u32,
	outside: u32,
}

/// Install the maps of the mapped layout in the user namespace of a process, which map each range to the sandbox identities from zero and the runner's identities after them.
pub(crate) async fn write_mapped(pid: libc::pid_t, maps: &crate::IdMaps) -> tg::Result<()> {
	// SAFETY: These functions have no preconditions.
	let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
	let extents = mapped_extents(&maps.uid, uid);
	write(pid, "uid", &maps.uid.helper, &extents).await?;
	let extents = mapped_extents(&maps.gid, gid);
	write(pid, "gid", &maps.gid.helper, &extents).await?;
	Ok(())
}

/// Install the maps of the rootless layout in the user namespace of a process, which map the sandbox's root to the runner's identities and the other sandbox identities to the ranges.
pub(crate) async fn write_rootless(pid: libc::pid_t, maps: &crate::IdMaps) -> tg::Result<()> {
	// SAFETY: These functions have no preconditions.
	let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
	let extents = rootless_extents(&maps.uid, uid);
	write(pid, "uid", &maps.uid.helper, &extents).await?;
	let extents = rootless_extents(&maps.gid, gid);
	write(pid, "gid", &maps.gid.helper, &extents).await?;
	Ok(())
}

fn mapped_extents(map: &crate::IdMap, runner: u32) -> Vec<Extent> {
	let range = Extent {
		count: map.count,
		inside: 0,
		outside: map.host,
	};
	let runner = Extent {
		count: 1,
		inside: map.count,
		outside: runner,
	};
	vec![range, runner]
}

/// Compute the extents of the rootless layout, which map the sandbox's root to the runner's identity and the sandbox identities from one through the range's count to the range.
fn rootless_extents(map: &crate::IdMap, runner: u32) -> Vec<Extent> {
	let root = Extent {
		count: 1,
		inside: 0,
		outside: runner,
	};
	let range = Extent {
		count: map.count,
		inside: 1,
		outside: map.host,
	};
	vec![root, range]
}

async fn write(pid: libc::pid_t, kind: &str, helper: &Path, extents: &[Extent]) -> tg::Result<()> {
	// Write the map directly as root, which needs no helper.
	// SAFETY: This function has no preconditions.
	if unsafe { libc::geteuid() } == 0 {
		let path = format!("/proc/{pid}/{kind}_map");
		let mut contents = String::new();
		for extent in extents {
			writeln!(
				contents,
				"{} {} {}",
				extent.inside, extent.outside, extent.count
			)
			.unwrap();
		}
		std::fs::write(&path, contents)
			.map_err(|error| tg::error!(!error, %path, "failed to write the user namespace map"))?;
		return Ok(());
	}

	// Run the helper, which installs the subordinate ranges that the host grants the runner's user.
	let mut command = tokio::process::Command::new(helper);
	command.kill_on_drop(true).arg(pid.to_string());
	for extent in extents {
		command
			.arg(extent.inside.to_string())
			.arg(extent.outside.to_string())
			.arg(extent.count.to_string());
	}
	let output = command.output().await.map_err(|error| {
		tg::error!(
			!error,
			path = %helper.display(),
			"failed to run the {kind} map helper"
		)
	})?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
		return Err(tg::error!(
			%stderr,
			status = %output.status,
			"the {kind} map helper failed"
		));
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn map() -> crate::IdMap {
		crate::IdMap {
			count: 65_536,
			helper: "/usr/bin/newuidmap".into(),
			host: 165_536,
		}
	}

	#[test]
	fn map_the_root_to_the_runner() {
		let extents = rootless_extents(&map(), 1000);
		assert_eq!(
			extents,
			[
				Extent {
					count: 1,
					inside: 0,
					outside: 1000,
				},
				Extent {
					count: 65_536,
					inside: 1,
					outside: 165_536,
				},
			]
		);
	}

	#[test]
	fn map_the_range_before_the_runner() {
		let extents = mapped_extents(&map(), 1000);
		assert_eq!(
			extents,
			[
				Extent {
					count: 65_536,
					inside: 0,
					outside: 165_536,
				},
				Extent {
					count: 1,
					inside: 65_536,
					outside: 1000,
				},
			]
		);
	}
}
