use {std::path::Path, tangram_client::prelude::*};

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
	std::fs::write(path, contents).map_err(|error| tg::error!(!error, path = %path.display(), "failed to write the sandbox launch configuration"))?;
	Ok(())
}
