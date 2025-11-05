use {
	crate::Server,
	std::{
		os::unix::fs::PermissionsExt as _,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

#[derive(Debug)]
pub struct Message {
	pub path: PathBuf,
	pub metadata: std::fs::Metadata,
}

impl Server {
	pub(super) fn checkin_fixup_task(
		receiver: &std::sync::mpsc::Receiver<Message>,
	) -> tg::Result<()> {
		while let Ok(message) = receiver.recv() {
			Self::checkin_fixup_task_inner(&message.path, &message.metadata).map_err(
				|source| tg::error!(!source, %path = message.path.display(), "failed to set permissions"),
			)?;
		}
		Ok::<_, tg::Error>(())
	}

	fn checkin_fixup_task_inner(path: &Path, metadata: &std::fs::Metadata) -> tg::Result<()> {
		if !metadata.is_symlink() {
			let mode = metadata.permissions().mode();
			let executable = mode & 0o111 != 0;
			let new_mode = if metadata.is_dir() || executable {
				0o555
			} else {
				0o444
			};
			if new_mode != mode {
				let permissions = std::fs::Permissions::from_mode(new_mode);
				std::fs::set_permissions(path, permissions).map_err(
					|source| tg::error!(!source, %path = path.display(), "failed to set the permissions"),
				)?;
			}
		}
		let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
		filetime::set_symlink_file_times(path, epoch, epoch).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to set the modified time"),
		)?;
		Ok(())
	}
}
