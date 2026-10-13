use {
	crate::config::Config,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

pub(crate) struct Assets {
	pub image: Option<PathBuf>,
	pub image_ready: tokio::sync::Mutex<bool>,
	pub root: PathBuf,
	pub snapshot: Option<PathBuf>,
	pub snapshot_lock: tokio::sync::Mutex<()>,
}

impl Assets {
	pub fn new(path: &Path, tangram_path: &Path, config: &Config) -> tg::Result<Self> {
		// Prepare the shared root before warming any instances.
		#[cfg(target_os = "linux")]
		let (root, image, snapshot) = {
			let vm = config.sandbox.isolation.vm.is_some();
			let root = path.join("container/root");
			let arg = tangram_sandbox::root::Arg {
				build_attempts: config.sandbox.build_attempts,
				path: root.clone(),
				tangram_path: tangram_path.to_owned(),
				vm,
			};
			tangram_sandbox::root::create(&arg)?;
			let (image, snapshot) = if let Some(backend) = &config.sandbox.isolation.vm {
				let image = path.join("vm/image.squashfs");
				let snapshot = backend
					.snapshot
					.clone()
					.unwrap_or_else(|| path.join("vm/snapshot"));
				(Some(image), Some(snapshot))
			} else {
				(None, None)
			};
			(root, image, snapshot)
		};
		#[cfg(target_os = "macos")]
		let (root, image, snapshot) = {
			let root = path.join("seatbelt/root");
			let arg = tangram_sandbox::seatbelt::root::Arg {
				path: root.clone(),
				tangram_path: tangram_path.to_owned(),
			};
			tangram_sandbox::seatbelt::root::create(&arg)?;
			(root, None, None)
		};
		let assets = Self {
			image,
			image_ready: tokio::sync::Mutex::new(false),
			root,
			snapshot,
			snapshot_lock: tokio::sync::Mutex::new(()),
		};
		Ok(assets)
	}
}
