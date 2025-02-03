use super::{
	Runtime, HOME_DIRECTORY_GUEST_PATH, OUTPUT_PARENT_DIRECTORY_GUEST_PATH,
	SERVER_DIRECTORY_GUEST_PATH, TANGRAM_GID, TANGRAM_UID, WORKING_DIRECTORY_GUEST_PATH,
};
use crate::temp::Temp;
use indoc::formatdoc;
use std::{ffi::CString, os::unix::ffi::OsStrExt as _, path::Path};
use tangram_client as tg;

pub struct Chroot {
	pub temp: Temp,
	pub mounts: Vec<Mount>,
	pub root: CString,
}

pub struct Mount {
	pub source: CString,
	pub target: CString,
	pub fstype: Option<CString>,
	pub flags: libc::c_ulong,
	pub data: Option<Vec<u8>>,
	pub readonly: bool,
}

impl Chroot {
	pub async fn new(
		runtime: &Runtime,
		network: bool,
		output_parent_directory: &Path,
	) -> tg::Result<Self> {
		// Create a temp to serve as the new root.
		let temp = Temp::new(&runtime.server);
		let root = temp.path();

		// Compute the artifacts path in the guest.
		let artifacts_directory_guest_path =
			Path::new(SERVER_DIRECTORY_GUEST_PATH).join("artifacts");

		// Create symlinks for /usr/bin/env and /bin/sh.
		let env_path = root.join("usr/bin/env");
		let sh_path = root.join("bin/sh");
		tokio::fs::create_dir_all(&env_path.parent().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
		tokio::fs::create_dir_all(&sh_path.parent().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
		let env_guest_path =
			artifacts_directory_guest_path.join(runtime.env.id(&runtime.server).await?.to_string());
		let sh_guest_path =
			artifacts_directory_guest_path.join(runtime.sh.id(&runtime.server).await?.to_string());

		tokio::fs::symlink(&env_guest_path, &env_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the env symlink"))?;
		tokio::fs::symlink(&sh_guest_path, &sh_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the sh symlink"))?;

		// Create /etc.
		tokio::fs::create_dir_all(root.join("etc"))
			.await
			.map_err(|source| tg::error!(!source, "failed to create /etc"))?;

		// Create /etc/passwd.
		tokio::fs::write(
			root.join("etc/passwd"),
			formatdoc!(
				r#"
					root:!:0:0:root:/nonexistent:/bin/false
					tangram:!:{TANGRAM_UID}:{TANGRAM_GID}:tangram:{HOME_DIRECTORY_GUEST_PATH}:/bin/false
					nobody:!:65534:65534:nobody:/nonexistent:/bin/false
				"#
			),
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to create /etc/passwd"))?;

		// Create /etc/group.
		tokio::fs::write(
			root.join("etc/group"),
			formatdoc!(
				r#"
					tangram:x:{TANGRAM_GID}:tangram
				"#
			),
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to create /etc/group"))?;

		// Create /etc/nsswitch.conf.
		tokio::fs::write(
			root.join("etc/nsswitch.conf"),
			formatdoc!(
				r#"
					passwd: files compat
					shadow: files compat
					hosts: files dns compat
				"#
			),
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to create /etc/nsswitch.conf"))?;

		// If network access is enabled, then copy /etc/resolv.conf from the host.
		if network {
			tokio::fs::copy("/etc/resolv.conf", root.join("etc/resolv.conf"))
				.await
				.map_err(|source| tg::error!(!source, "failed to copy /etc/resolv.conf"))?;
		}

		// Create the host and guest paths for the home directory, with inner .tangram directory.
		let home_directory_host_path =
			root.join(HOME_DIRECTORY_GUEST_PATH.strip_prefix('/').unwrap());
		tokio::fs::create_dir_all(&home_directory_host_path.join(".tangram"))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the home directory"))?;

		// Create the host and guest paths for the working directory.
		let working_directory_host_path =
			root.join(WORKING_DIRECTORY_GUEST_PATH.strip_prefix('/').unwrap());
		tokio::fs::create_dir_all(&working_directory_host_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the working directory"))?;

		// Create the mounts.
		let mut mounts = Vec::new();

		// Add /dev to the mounts.
		let dev_host_path = Path::new("/dev");
		let dev_guest_path = Path::new("/dev");
		let dev_source_path = dev_host_path;
		let dev_target_path = root.join(dev_guest_path.strip_prefix("/").unwrap());
		tokio::fs::create_dir_all(&dev_target_path)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					r#"failed to create the mountpoint for "/dev""#
				)
			})?;
		let dev_source_path = CString::new(dev_source_path.as_os_str().as_bytes()).unwrap();
		let dev_target_path = CString::new(dev_target_path.as_os_str().as_bytes()).unwrap();
		mounts.push(Mount {
			source: dev_source_path,
			target: dev_target_path,
			fstype: None,
			flags: libc::MS_BIND | libc::MS_REC,
			data: None,
			readonly: false,
		});

		// Add /proc to the mounts.
		let proc_host_path = Path::new("/proc");
		let proc_guest_path = Path::new("/proc");
		let proc_source_path = proc_host_path;
		let proc_target_path = root.join(proc_guest_path.strip_prefix("/").unwrap());
		tokio::fs::create_dir_all(&proc_target_path)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					r#"failed to create the mount point for "/proc""#
				)
			})?;
		let proc_source_path = CString::new(proc_source_path.as_os_str().as_bytes()).unwrap();
		let proc_target_path = CString::new(proc_target_path.as_os_str().as_bytes()).unwrap();
		mounts.push(Mount {
			source: proc_source_path,
			target: proc_target_path,
			fstype: Some(CString::new("proc").unwrap()),
			flags: 0,
			data: None,
			readonly: false,
		});

		// Add /tmp to the mounts.
		let tmp_host_path = Path::new("/tmp");
		let tmp_guest_path = Path::new("/tmp");
		let tmp_source_path = tmp_host_path;
		let tmp_target_path = root.join(tmp_guest_path.strip_prefix("/").unwrap());
		tokio::fs::create_dir_all(&tmp_target_path)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					r#"failed to create the mount point for "/tmp""#
				)
			})?;
		let tmp_source_path = CString::new(tmp_source_path.as_os_str().as_bytes()).unwrap();
		let tmp_target_path = CString::new(tmp_target_path.as_os_str().as_bytes()).unwrap();
		mounts.push(Mount {
			source: tmp_source_path,
			target: tmp_target_path,
			fstype: Some(CString::new("tmpfs").unwrap()),
			flags: 0,
			data: None,
			readonly: false,
		});

		// Add the &runtime.server directory to the mounts.
		let server_directory_source_path = &runtime.server.path;
		let server_directory_guest_path = Path::new(SERVER_DIRECTORY_GUEST_PATH);
		let server_directory_target_path =
			root.join(server_directory_guest_path.strip_prefix("/").unwrap());
		tokio::fs::create_dir_all(&server_directory_target_path)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					"failed to create the mount point for the tangram directory"
				)
			})?;
		let server_directory_source_path =
			CString::new(server_directory_source_path.as_os_str().as_bytes()).unwrap();
		let server_directory_target_path =
			CString::new(server_directory_target_path.as_os_str().as_bytes()).unwrap();
		mounts.push(Mount {
			source: server_directory_source_path,
			target: server_directory_target_path,
			fstype: None,
			flags: libc::MS_BIND | libc::MS_REC,
			data: None,
			readonly: false,
		});

		// Add the home directory to the mounts.
		let home_directory_source_path = home_directory_host_path.clone();
		let home_directory_target_path = home_directory_host_path.clone();
		let home_directory_source_path =
			CString::new(home_directory_source_path.as_os_str().as_bytes()).unwrap();
		let home_directory_target_path =
			CString::new(home_directory_target_path.as_os_str().as_bytes()).unwrap();
		mounts.push(Mount {
			source: home_directory_source_path,
			target: home_directory_target_path,
			fstype: None,
			flags: libc::MS_BIND | libc::MS_REC,
			data: None,
			readonly: false,
		});

		// Add the output parent directory to the mounts.
		let output_parent_directory_source_path = output_parent_directory;
		let output_parent_directory_target_path = root.join(
			OUTPUT_PARENT_DIRECTORY_GUEST_PATH
				.strip_prefix("/")
				.unwrap(),
		);
		// Create the directory for the parent.
		tokio::fs::create_dir_all(&output_parent_directory_target_path)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					"failed to create the mount point for the output parent directory"
				)
			})?;
		let output_parent_directory_source_path =
			CString::new(output_parent_directory_source_path.as_os_str().as_bytes()).unwrap();
		let output_parent_directory_target_path =
			CString::new(output_parent_directory_target_path.as_os_str().as_bytes()).unwrap();
		mounts.push(Mount {
			source: output_parent_directory_source_path,
			target: output_parent_directory_target_path,
			fstype: None,
			flags: libc::MS_BIND | libc::MS_REC,
			data: None,
			readonly: false,
		});

		// Convert the new root path as a C string.
		let root = CString::new(root.as_os_str().as_bytes()).map_err(|error| {
			tg::error!(
				source = error,
				"the root directory host path is not a valid C string"
			)
		})?;

		Ok(Self { temp, root, mounts })
	}
}
