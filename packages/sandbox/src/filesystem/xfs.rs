use {
	std::{fs::File, path::Path},
	tangram_client::prelude::*,
};

#[cfg(test)]
mod tests;

// Preserve the field order required by the Linux quota ABI.
#[repr(C)]
struct XfsDiskQuota {
	version: i8,
	flags: i8,
	fieldmask: u16,
	id: u32,
	block_hard: u64,
	block_soft: u64,
	inode_hard: u64,
	inode_soft: u64,
	block_count: u64,
	inode_count: u64,
	inode_timer: i32,
	block_timer: i32,
	inode_warnings: u16,
	block_warnings: u16,
	inode_timer_high: i8,
	block_timer_high: i8,
	realtime_block_timer_high: i8,
	padding2: i8,
	realtime_block_hard: u64,
	realtime_block_soft: u64,
	realtime_block_count: u64,
	realtime_block_timer: i32,
	realtime_block_warnings: u16,
	padding3: i16,
	padding4: [u8; 8],
}

pub(super) fn create(
	path: &Path,
	project: u32,
	size: Option<u64>,
	inodes: Option<u64>,
) -> tg::Result<()> {
	let directory = super::project::create(path, project)?;
	set_limit(&directory, project, size, inodes)?;
	Ok(())
}

pub(super) fn set_limit(
	directory: &File,
	project: u32,
	size: Option<u64>,
	inodes: Option<u64>,
) -> tg::Result<()> {
	let block_mask = if size.is_some() {
		(1 << 2) | (1 << 3)
	} else {
		0
	};
	let inode_mask = if inodes.is_some() {
		(1 << 0) | (1 << 1)
	} else {
		0
	};
	let limits = XfsDiskQuota {
		block_count: 0,
		block_hard: size.unwrap_or_default().div_ceil(512),
		block_soft: size.unwrap_or_default().div_ceil(512),
		block_timer: 0,
		block_timer_high: 0,
		block_warnings: 0,
		fieldmask: block_mask | inode_mask,
		flags: 1 << 1,
		id: project,
		inode_count: 0,
		inode_hard: inodes.unwrap_or_default(),
		inode_soft: inodes.unwrap_or_default(),
		inode_timer: 0,
		inode_timer_high: 0,
		inode_warnings: 0,
		padding2: 0,
		padding3: 0,
		padding4: [0; 8],
		realtime_block_count: 0,
		realtime_block_hard: 0,
		realtime_block_soft: 0,
		realtime_block_timer: 0,
		realtime_block_timer_high: 0,
		realtime_block_warnings: 0,
		version: 1,
	};
	let command = libc::QCMD(
		(u32::from(b'X') << 8).cast_signed() | 4,
		super::project::PRJQUOTA,
	);
	let result = super::project::control(directory, command, project, (&raw const limits).cast());
	result.map_err(|error| {
		tg::error!(
			!error,
			%project,
			"failed to configure the sandbox filesystem project quota; enable project quota enforcement and grant CAP_SYS_ADMIN"
		)
	})?;

	Ok(())
}
