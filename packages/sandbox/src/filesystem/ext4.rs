use {
	std::{fs::File, path::Path},
	tangram_client::prelude::*,
};

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
	let limits = libc::dqblk {
		dqb_bhardlimit: size.unwrap_or_default().div_ceil(1024),
		dqb_bsoftlimit: size.unwrap_or_default().div_ceil(1024),
		dqb_btime: 0,
		dqb_curinodes: 0,
		dqb_curspace: 0,
		dqb_ihardlimit: inodes.unwrap_or_default(),
		dqb_isoftlimit: inodes.unwrap_or_default(),
		dqb_itime: 0,
		dqb_valid: (u32::from(size.is_some()) * libc::QIF_BLIMITS)
			| (u32::from(inodes.is_some()) * libc::QIF_ILIMITS),
	};
	let command = libc::QCMD(libc::Q_SETQUOTA, super::project::PRJQUOTA);
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
