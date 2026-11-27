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

/// Install the maps of the rootless layout in the user namespace of a process, which map the workload's identities to the runner's identities and the other sandbox identities to the ranges.
pub(crate) async fn write_rootless(
	pid: libc::pid_t,
	maps: &crate::IdMaps,
	uid: u32,
	gid: u32,
) -> tg::Result<()> {
	// SAFETY: These functions have no preconditions.
	let (runner_uid, runner_gid) = unsafe { (libc::geteuid(), libc::getegid()) };
	let extents = rootless_extents(&maps.uid, uid, runner_uid)?;
	write(pid, "uid", &maps.uid.helper, &extents).await?;
	let extents = rootless_extents(&maps.gid, gid, runner_gid)?;
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

/// Compute the extents of the rootless layout, which map the sandbox identities from zero through the range's count, the workload's identity to the runner's identity, and the others in order to the range.
fn rootless_extents(map: &crate::IdMap, workload: u32, runner: u32) -> tg::Result<Vec<Extent>> {
	if workload > map.count {
		return Err(tg::error!(
			count = %map.count,
			id = %workload,
			"the rootless identity map does not contain the workload identity"
		));
	}
	let below = Extent {
		count: workload,
		inside: 0,
		outside: map.host,
	};
	let workload_extent = Extent {
		count: 1,
		inside: workload,
		outside: runner,
	};
	let above = Extent {
		count: map.count - workload,
		inside: workload + 1,
		outside: map.host + workload,
	};
	let extents = [below, workload_extent, above]
		.into_iter()
		.filter(|extent| extent.count > 0)
		.collect();
	Ok(extents)
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
	fn map_a_root_workload_to_the_runner() {
		let extents = rootless_extents(&map(), 0, 1000).unwrap();
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
	fn map_a_workload_with_a_nonzero_identity_to_the_runner() {
		let extents = rootless_extents(&map(), 1000, 1000).unwrap();
		assert_eq!(
			extents,
			[
				Extent {
					count: 1000,
					inside: 0,
					outside: 165_536,
				},
				Extent {
					count: 1,
					inside: 1000,
					outside: 1000,
				},
				Extent {
					count: 64_536,
					inside: 1001,
					outside: 166_536,
				},
			]
		);
	}

	#[test]
	fn map_a_workload_at_the_last_identity_to_the_runner() {
		let extents = rootless_extents(&map(), 65_536, 1000).unwrap();
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
		assert!(rootless_extents(&map(), 65_537, 1000).is_err());
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
