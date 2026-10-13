use {
	std::{io::Write as _, os::unix::fs::OpenOptionsExt as _, path::Path},
	tangram_client::prelude::*,
};

#[cfg(target_os = "linux")]
mod validate;

#[cfg(target_os = "linux")]
pub mod linux;

pub fn write_config<T: serde::Serialize>(path: &Path, arg: &T) -> tg::Result<()> {
	let contents = serde_json::to_vec(arg).map_err(|error| {
		tg::error!(
			!error,
			"failed to serialize the sandbox launch configuration"
		)
	})?;
	let mut file = std::fs::OpenOptions::new().create_new(true).mode(0o600).write(true).open(path)
        .map_err(|error| tg::error!(!error, path = %path.display(), "failed to create the sandbox launch configuration"))?;
	file.write_all(&contents)
        .map_err(|error| tg::error!(!error, path = %path.display(), "failed to write the sandbox launch configuration"))?;
	Ok(())
}
