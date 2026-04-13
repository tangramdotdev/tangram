use {
	crate::Sandbox,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
pub(crate) use self::linux::ensure_mount_target;

#[derive(Clone, Debug)]
pub struct Arg {
	pub path: PathBuf,
	pub tangram_path: PathBuf,
}

pub fn prepare(arg: &Arg) -> tg::Result<()> {
	#[cfg(target_os = "macos")]
	{
		self::darwin::prepare_runtime_libraries(arg)
	}
	#[cfg(target_os = "linux")]
	{
		self::linux::prepare_runtime_libraries(arg)
	}
}

impl Sandbox {
	#[must_use]
	pub fn host_path_for_guest_path(&self, path: &Path) -> Option<PathBuf> {
		#[cfg(target_os = "macos")]
		{
			Some(path.to_owned())
		}

		#[cfg(target_os = "linux")]
		{
			let mut path_maps = self
				.0
				.mounts
				.iter()
				.map(|mount| (mount.target.clone(), mount.source.clone()))
				.collect::<Vec<_>>();
			if matches!(self.0.isolation, tg::sandbox::Isolation::Container) {
				path_maps.extend([
					(
						Self::guest_listen_path_from_root(&self.0.path),
						Self::host_listen_path_from_root(&self.0.path),
					),
					(
						self.guest_tangram_socket_path(),
						self.host_tangram_socket_path(),
					),
				]);
			}
			path_maps.extend([
				(
					self.guest_tmp_path(),
					Self::host_tmp_path_from_root(&self.0.path),
				),
				(self.guest_output_path(), self.host_output_path()),
				(self.guest_artifacts_path(), self.0.artifacts_path.clone()),
			]);
			if matches!(self.0.isolation, tg::sandbox::Isolation::Container) {
				path_maps.push((
					PathBuf::from("/"),
					Self::host_upper_path_from_root(&self.0.path),
				));
			}
			Self::map_path(
				path,
				path_maps
					.iter()
					.map(|(guest, host)| (guest.as_path(), host.as_path())),
			)
		}
	}

	#[must_use]
	pub fn guest_artifacts_path(&self) -> PathBuf {
		Self::guest_artifacts_path_from_host_artifacts_path(&self.0.artifacts_path)
	}

	#[must_use]
	pub fn guest_output_path(&self) -> PathBuf {
		Self::guest_output_path_from_root(&self.0.path)
	}

	#[must_use]
	pub fn guest_output_path_for_process(&self, id: &tg::process::Id) -> PathBuf {
		self.guest_output_path().join(id.to_string())
	}

	#[must_use]
	pub fn guest_path_for_host_path(&self, path: &Path) -> Option<PathBuf> {
		#[cfg(target_os = "macos")]
		{
			Some(path.to_owned())
		}

		#[cfg(target_os = "linux")]
		{
			let mut path_maps = self
				.0
				.mounts
				.iter()
				.map(|mount| (mount.source.clone(), mount.target.clone()))
				.collect::<Vec<_>>();
			if matches!(self.0.isolation, tg::sandbox::Isolation::Container) {
				path_maps.extend([
					(
						Self::host_listen_path_from_root(&self.0.path),
						Self::guest_listen_path_from_root(&self.0.path),
					),
					(
						self.host_tangram_socket_path(),
						self.guest_tangram_socket_path(),
					),
				]);
			}
			path_maps.extend([
				(
					Self::host_tmp_path_from_root(&self.0.path),
					self.guest_tmp_path(),
				),
				(self.host_output_path(), self.guest_output_path()),
				(self.0.artifacts_path.clone(), self.guest_artifacts_path()),
			]);
			if matches!(self.0.isolation, tg::sandbox::Isolation::Container) {
				path_maps.push((
					Self::host_upper_path_from_root(&self.0.path),
					PathBuf::from("/"),
				));
			}
			Self::map_path(
				path,
				path_maps
					.iter()
					.map(|(host, guest)| (host.as_path(), guest.as_path())),
			)
		}
	}

	#[must_use]
	pub fn guest_tangram_socket_path(&self) -> PathBuf {
		Self::guest_tangram_socket_path_from_root(&self.0.path)
	}

	#[must_use]
	pub fn guest_tangram_path(&self) -> PathBuf {
		Self::guest_tangram_path_from_host_tangram_path(&self.0.tangram_path)
	}

	#[must_use]
	pub fn guest_tmp_path(&self) -> PathBuf {
		Self::guest_tmp_path_from_root(&self.0.path)
	}

	#[must_use]
	pub fn host_output_path(&self) -> PathBuf {
		Self::host_output_path_from_root(&self.0.path)
	}

	#[must_use]
	pub fn host_output_path_for_process(&self, id: &tg::process::Id) -> PathBuf {
		self.host_output_path().join(id.to_string())
	}

	#[must_use]
	pub fn host_scratch_path(&self) -> PathBuf {
		Self::host_scratch_path_from_root(&self.0.path)
	}

	#[must_use]
	pub fn host_tangram_socket_path(&self) -> PathBuf {
		Self::host_tangram_socket_path_from_root(&self.0.path)
	}

	#[must_use]
	pub(crate) fn guest_artifacts_path_from_host_artifacts_path(artifacts_path: &Path) -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			artifacts_path.to_owned()
		}
		#[cfg(target_os = "linux")]
		{
			let _ = artifacts_path;
			"/opt/tangram/artifacts".into()
		}
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub(crate) fn guest_libexec_tangram_path() -> PathBuf {
		"/opt/tangram/libexec/tangram".into()
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	#[cfg_attr(target_os = "linux", allow(dead_code))]
	pub(crate) fn guest_listen_path_from_root(root_path: &Path) -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			Self::host_listen_path_from_root(root_path)
		}
		#[cfg(target_os = "linux")]
		{
			let _ = root_path;
			"/socket".into()
		}
	}

	#[must_use]
	pub(crate) fn guest_output_path_from_root(root_path: &Path) -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			Self::host_output_path_from_root(root_path)
		}
		#[cfg(target_os = "linux")]
		{
			let _ = root_path;
			"/opt/tangram/output".into()
		}
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub(crate) fn guest_ssl_cert_dir() -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			"/etc/ssl/certs".into()
		}
		#[cfg(target_os = "linux")]
		{
			"/opt/tangram/etc/ssl/certs".into()
		}
	}

	#[must_use]
	pub fn guest_tangram_socket_path_from_root(root_path: &Path) -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			Self::host_tangram_socket_path_from_root(root_path)
		}
		#[cfg(target_os = "linux")]
		{
			let _ = root_path;
			"/opt/tangram/socket".into()
		}
	}

	#[must_use]
	pub(crate) fn guest_tangram_path_from_host_tangram_path(tangram_path: &Path) -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			tangram_path.to_owned()
		}
		#[cfg(target_os = "linux")]
		{
			let _ = tangram_path;
			"/opt/tangram/bin/tangram".into()
		}
	}

	#[must_use]
	pub(crate) fn guest_tmp_path_from_root(root_path: &Path) -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			Self::host_tmp_path_from_root(root_path)
		}
		#[cfg(target_os = "linux")]
		{
			let _ = root_path;
			"/tmp".into()
		}
	}

	#[must_use]
	pub(crate) fn host_etc_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("etc")
	}

	#[must_use]
	#[cfg_attr(target_os = "linux", allow(dead_code))]
	pub(crate) fn host_listen_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("socket")
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub(crate) fn host_nsswitch_path_from_root(root_path: &Path) -> PathBuf {
		Self::host_etc_path_from_root(root_path).join("nsswitch.conf")
	}

	#[must_use]
	pub(crate) fn host_output_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("output")
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub(crate) fn host_passwd_path_from_root(root_path: &Path) -> PathBuf {
		Self::host_etc_path_from_root(root_path).join("passwd")
	}

	#[cfg(target_os = "macos")]
	#[must_use]
	pub(crate) fn host_profile_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("sandbox.sb")
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	#[cfg_attr(target_os = "linux", allow(dead_code))]
	pub(crate) fn host_resolv_conf_path_from_root(root_path: &Path) -> PathBuf {
		Self::host_etc_path_from_root(root_path).join("resolv.conf")
	}

	#[must_use]
	pub(crate) fn host_scratch_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("scratch")
	}

	#[must_use]
	pub fn host_tangram_socket_path_from_root(root_path: &Path) -> PathBuf {
		#[cfg(target_os = "macos")]
		{
			root_path.join("tg")
		}
		#[cfg(target_os = "linux")]
		{
			Self::host_upper_path_from_root(root_path).join("opt/tangram/socket")
		}
	}

	#[must_use]
	pub(crate) fn host_tmp_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("tmp")
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub(crate) fn host_upper_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("upper")
	}

	#[must_use]
	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub(crate) fn host_work_path_from_root(root_path: &Path) -> PathBuf {
		root_path.join("work")
	}

	#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
	pub(crate) fn map_path<'a>(
		path: &Path,
		path_maps: impl Iterator<Item = (&'a Path, &'a Path)>,
	) -> Option<PathBuf> {
		let mut best: Option<(&Path, &Path, usize)> = None;
		for (from, to) in path_maps {
			if !path.starts_with(from) {
				continue;
			}
			let len = from.components().count();
			if best.is_none_or(|(_, _, best_len)| len >= best_len) {
				best = Some((from, to, len));
			}
		}
		let (from, to, _) = best?;
		let suffix = path.strip_prefix(from).unwrap();
		Some(to.join(suffix))
	}
}
