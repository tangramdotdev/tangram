use {
	crate::{Server, temp::Temp},
	indoc::formatdoc,
	std::{
		collections::HashMap,
		os::unix::ffi::OsStrExt as _,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

impl Server {
	pub(super) async fn get_sandbox_args(
		&self,
		id: &tg::sandbox::Id,
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
		let root = temp.path().join("root");
		let mut overlays = HashMap::new();
		for mount in &arg.mounts {
			match &mount.source {
				tg::Either::Left(source) => {
					args.push("--mount".to_owned());
					args.push(bind(source, &mount.target, mount.readonly));
				},
				tg::Either::Right(id) => {
					// Create the overlay state if it does not exist. Since we use async here, we can't use the .entry() api.
					if !overlays.contains_key(&mount.target) {
						let lowerdirs = Vec::new();
						let upperdir = temp.path().join("upper").join(overlays.len().to_string());
						let workdir = temp.path().join("work").join(overlays.len().to_string());
						tokio::fs::create_dir_all(&upperdir).await.ok();
						tokio::fs::create_dir_all(&workdir).await.ok();
						overlays.insert(mount.target.clone(), (lowerdirs, upperdir, workdir));
					}

					// Compute the path.
					let path = self.artifacts_path().join(id.to_string());

					// Get the lower dirs.
					let (lowerdirs, _, _) = overlays.get_mut(&mount.target).unwrap();

					// Add this path to the lowerdirs.
					lowerdirs.push(path);
				},
			}
		}

		if !root_mounted {
			// Create the .tangram directory.
			let path = temp.path().join(".tangram");
			tokio::fs::create_dir_all(&path).await.map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to create the data directory"),
			)?;

			// Create /etc.
			tokio::fs::create_dir_all(temp.path().join("lower/etc"))
				.await
				.ok();

			// Create /tmp.
			tokio::fs::create_dir_all(temp.path().join("lower/tmp"))
				.await
				.ok();

			// Create nsswitch.conf.
			tokio::fs::write(
				temp.path().join("lower/etc/nsswitch.conf"),
				formatdoc!(
					"
						passwd: files compat
						shadow: files compat
						hosts: files dns compat
					"
				),
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to create /etc/nsswitch.conf"))?;

			// Create /etc/passwd.
			tokio::fs::write(
				temp.path().join("lower/etc/passwd"),
				formatdoc!(
					"
						root:!:0:0:root:/nonexistent:/bin/false
						nobody:!:65534:65534:nobody:/nonexistent:/bin/false
					"
				),
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to create /etc/passwd"))?;

			// Copy resolv.conf.
			if arg.network {
				tokio::fs::copy(
					"/etc/resolv.conf",
					temp.path().join("lower/etc/resolv.conf"),
				)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to copy /etc/resolv.conf to the sandbox")
				})?;
				args.push("--network".to_owned());
			}

			// Get or create the root overlay.
			if !overlays.contains_key(Path::new("/")) {
				let lowerdirs = Vec::new();
				let upperdir = temp.path().join("upper").join(overlays.len().to_string());
				let workdir = temp.path().join("work").join(overlays.len().to_string());
				tokio::fs::create_dir_all(&upperdir).await.ok();
				tokio::fs::create_dir_all(&workdir).await.ok();
				overlays.insert("/".into(), (lowerdirs, upperdir, workdir));
			}
			let (lowerdirs, _, _) = overlays.get_mut(Path::new("/")).unwrap();
			lowerdirs.push(temp.path().join("lower"));

			// Add mounts for /dev, /proc, /tmp, /.tangram, /.tangram/artifacts, and /output.
			args.push("--mount".to_owned());
			args.push(bind("/dev", "/dev", false));
			args.push("--mount".to_owned());
			args.push(bind("/proc", "/proc", false));
			args.push("--mount".to_owned());
			args.push(bind(temp.path().join(".tangram"), "/.tangram", false));
			args.push("--mount".to_owned());
			args.push(bind(self.artifacts_path(), "/.tangram/artifacts", true));
			args.push("--mount".to_owned());
			args.push(bind(temp.path().join("output"), "/output", false));
		}

		// Add the overlay mounts.
		for (merged, (lowerdirs, upperdir, workdir)) in &overlays {
			args.push("--mount".to_owned());
			args.push(overlay(lowerdirs, upperdir, workdir, merged));
		}

		// Set the user.
		let user = arg.user.as_deref().unwrap_or("root");
		args.push("--user".to_owned());
		args.push(user.to_owned());

		// Set the hostname
		args.push("--hostname".to_owned());
		args.push(id.to_string());

		// Set the chroot.
		args.push("--root".to_owned());
		args.push(root.display().to_string());

		Ok((root, args))
	}
}

fn bind(source: impl AsRef<Path>, target: impl AsRef<Path>, readonly: bool) -> String {
	let mut string = format!(
		"type=bind,source={},target={}",
		source.as_ref().display(),
		target.as_ref().display()
	);
	if readonly {
		string.push_str(",ro");
	}
	string
}

fn overlay(lowerdirs: &[PathBuf], upperdir: &Path, workdir: &Path, merged: &Path) -> String {
	fn escape(out: &mut Vec<u8>, path: &[u8]) {
		for byte in path.iter().copied() {
			if byte == 0 {
				break;
			}
			if byte == b':' {
				out.push(b'\\');
			}
			out.push(byte);
		}
	}

	// Create the mount options.
	let mut data = b"type=overlay,source=overlay,target=".to_vec();
	data.extend_from_slice(merged.as_os_str().as_bytes());

	// Add the lower directories.
	data.extend_from_slice(b",userxattr,lowerdir=");
	for (n, dir) in lowerdirs.iter().enumerate() {
		escape(&mut data, dir.as_os_str().as_bytes());
		if n != lowerdirs.len() - 1 {
			data.push(b':');
		}
	}

	// Add the upper directory.
	data.extend_from_slice(b",upperdir=");
	data.extend_from_slice(upperdir.as_os_str().as_bytes());

	// Add the working directory.
	data.extend_from_slice(b",workdir=");
	data.extend_from_slice(workdir.as_os_str().as_bytes());

	String::from_utf8(data).unwrap()
}
