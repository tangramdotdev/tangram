use {
	crate::config::Config,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

pub(crate) struct Assets {
	pub image: Option<PathBuf>,
	#[cfg(target_os = "linux")]
	pub image_ready: tokio::sync::Mutex<bool>,
	pub root: PathBuf,
	pub snapshot: Option<PathBuf>,
	#[cfg(target_os = "linux")]
	pub snapshot_lock: tokio::sync::Mutex<()>,
}

impl Assets {
	pub fn new(path: &Path, tangram_path: &Path, config: &Config) -> tg::Result<Self> {
		// Prepare the shared root before warming any instances.
		#[cfg(target_os = "linux")]
		let (root, image, snapshot) = {
			let vm = matches!(
				config.runner.backend,
				crate::config::RunnerBackend::CloudHypervisor(_)
			);
			let root = path.join("container/root");
			let arg = tangram_sandbox::root::Arg {
				build_attempts: config.sandbox.build_attempts,
				path: root.clone(),
				tangram_path: tangram_path.to_owned(),
				vm,
			};
			tangram_sandbox::root::create(&arg)?;
			let (image, snapshot) = match &config.runner.backend {
				crate::config::RunnerBackend::CloudHypervisor(backend) => {
					let image = path.join("vm/image.squashfs");
					let snapshot = backend
						.snapshot
						.clone()
						.unwrap_or_else(|| path.join("vm/snapshot"));
					(Some(image), Some(snapshot))
				},
				crate::config::RunnerBackend::Builtin(_)
				| crate::config::RunnerBackend::Bubblewrap(_)
				| crate::config::RunnerBackend::Gvisor(_)
				| crate::config::RunnerBackend::Seatbelt => (None, None),
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
			#[cfg(target_os = "linux")]
			image_ready: tokio::sync::Mutex::new(false),
			root,
			snapshot,
			#[cfg(target_os = "linux")]
			snapshot_lock: tokio::sync::Mutex::new(()),
		};
		Ok(assets)
	}
}
