use {
	crate::{Server, temp::Temp},
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

impl Server {
	pub(super) async fn get_sandbox_args(
		&self,
		_id: &tg::sandbox::Id,
		arg: &tg::sandbox::create::Arg,
		temp: &Temp,
	) -> tg::Result<(PathBuf, Vec<String>)> {
		// Determine if the root is mounted.
		let root_mounted = arg.mounts.iter().any(|mount| {
			mount
				.source
				.as_ref()
				.left()
				.is_some_and(|source| source == &mount.target && mount.target == Path::new("/"))
		});

		let mut args = Vec::new();
		let root = temp.path().to_path_buf();

		// Add bind mounts.
		for mount in &arg.mounts {
			match &mount.source {
				tg::Either::Left(path) => {
					let mount_arg = if mount.readonly {
						format!("source={},ro", path.display())
					} else {
						format!("source={}", path.display())
					};
					args.push("--mount".to_owned());
					args.push(mount_arg);
				},
				tg::Either::Right(_) => {
					return Err(tg::error!("overlay mounts are not supported on darwin"));
				},
			}
		}

		if !root_mounted {
			// Create the .tangram directory.
			let path = temp.path().join(".tangram");
			tokio::fs::create_dir_all(&path).await.map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to create the data directory"),
			)?;

			// Add mounts for the temp directory, artifacts, and the working directory.
			args.push("--mount".to_owned());
			args.push(format!("source={}", temp.path().display()));
			args.push("--mount".to_owned());
			args.push(format!("source={},ro", self.artifacts_path().display()));
		}

		// Add the network flag.
		if arg.network {
			args.push("--network".to_owned());
		}

		Ok((root, args))
	}
}
