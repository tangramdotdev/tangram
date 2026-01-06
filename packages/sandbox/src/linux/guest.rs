use {
	super::Context,
	crate::abort_errno,
	std::{ffi::CString, mem::MaybeUninit, os::fd::AsRawFd},
};

pub fn main(mut context: Context) -> ! {
	unsafe {
		// Set hostname.
		if let Some(hostname) = context.hostname.take()
			&& libc::sethostname(hostname.as_ptr(), hostname.as_bytes().len()) != 0
		{
			abort_errno!("failed to set hostname");
		}

		// Wait for the notification from the host process to continue.
		let mut notification = 0u8;
		let ret = libc::recv(
			context.socket.as_raw_fd(),
			std::ptr::addr_of_mut!(notification).cast(),
			std::mem::size_of_val(&notification),
			0,
		);
		if ret == -1 {
			abort_errno!(
				"the guest process failed to receive the notification from the host process to continue"
			);
		}
		assert_eq!(notification, 1);

		// If requested to spawn in a chroot, perform the mounts and chroot.
		if context.root.is_some() {
			mount_and_chroot(&mut context);
		}

		// Set the working directory.
		let ret = libc::chdir(context.cwd.as_ptr());
		if ret == -1 {
			abort_errno!("failed to set the working directory");
		}

		// Finally, exec the process.
		libc::execvpe(
			context.executable.as_ptr(),
			context.argv.as_ptr().cast(),
			context.envp.as_ptr().cast(),
		);

		abort_errno!("execvpe failed")
	}
}

fn mount_and_chroot(context: &mut Context) {
	unsafe {
		let root = context.root.as_ref().unwrap();
		for mount in &mut context.mounts {
			// Create the mount point.
			if let (Some(source), Some(target)) = (&mount.source, &mut mount.target) {
				create_mountpoint_if_not_exists(source, target);
			}
			let source = mount
				.source
				.as_ref()
				.map_or_else(std::ptr::null, |s| s.as_ptr());
			let target = mount
				.target
				.as_ref()
				.map_or_else(std::ptr::null, |s| s.as_ptr());
			let fstype = mount
				.fstype
				.as_ref()
				.map_or_else(std::ptr::null, |value| value.as_ptr());
			let flags = mount.flags;
			let data = mount
				.data
				.as_ref()
				.map_or_else(std::ptr::null, |bytes| bytes.as_ptr())
				.cast();
			let ret = libc::mount(source, target, fstype, flags, data);
			if ret == -1 {
				abort_errno!("failed to mount {mount:#?}");
			}

			// The initial bind mount ignores flags other than MS_BIND and MS_REC. To support read-only bind mounts we must immediately remount the source/target pair with MS_REMOUNT to change the permissions of the mount point.
			if (flags & libc::MS_BIND != 0) && (flags & libc::MS_RDONLY != 0) {
				let flags = flags | libc::MS_REMOUNT;
				let ret = libc::mount(source, target, fstype, flags, data);
				if ret == -1 {
					abort_errno!("failed to remount bind mount as read-only {mount:#?}");
				}
			}
		}

		// Mount the root, required by the pivot_root syscall.
		let ret = libc::mount(
			root.as_ptr(),
			root.as_ptr(),
			std::ptr::null(),
			libc::MS_BIND | libc::MS_REC,
			std::ptr::null(),
		);
		if ret == -1 {
			abort_errno!("failed to mount the root");
		}

		// Change the working directory to the pivoted root.
		if let Some(root) = &context.root {
			let ret = libc::chdir(root.as_ptr());
			if ret == -1 {
				abort_errno!("failed to change directory to the root");
			}
		}

		// Pivot the root.
		let ret = libc::syscall(libc::SYS_pivot_root, c".".as_ptr(), c".".as_ptr());
		if ret == -1 {
			abort_errno!("failed to pivot the root");
		}

		// Lazily unmount the root.
		let ret = libc::umount2(c".".as_ptr().cast(), libc::MNT_DETACH);
		if ret == -1 {
			abort_errno!("failed to unmount the root");
		}
	}
}

fn create_mountpoint_if_not_exists(source: &CString, target: &mut CString) {
	unsafe {
		#[cfg_attr(all(target_arch = "x86_64"), expect(clippy::cast_possible_wrap))]
		const BACKSLASH: libc::c_char = b'\\' as _;
		#[cfg_attr(all(target_arch = "x86_64"), expect(clippy::cast_possible_wrap))]
		const SLASH: libc::c_char = b'/' as _;
		const NULL: libc::c_char = 0;

		// Determine if the target is a directory or not.
		let is_dir = 'a: {
			if source.as_bytes() == b"overlay" {
				break 'a true;
			}
			let mut stat = MaybeUninit::<libc::stat>::zeroed();
			if libc::stat(source.as_ptr(), stat.as_mut_ptr().cast()) < 0 {
				abort_errno!("failed to stat source");
			}
			let stat = stat.assume_init();
			if !(stat.st_mode & libc::S_IFDIR != 0 || stat.st_mode & libc::S_IFREG != 0) {
				abort_errno!("mount source is not a directory or regular file");
			}
			stat.st_mode & libc::S_IFDIR != 0
		};

		let ptr = target.as_ptr().cast_mut();
		let len = target.as_bytes_with_nul().len();
		let mut esc = false;
		for n in 1..len {
			match (*ptr.add(n), esc) {
				(SLASH, false) => {
					*ptr.add(n) = 0;
					libc::mkdir(target.as_ptr(), 0o755);
					*ptr.add(n) = SLASH;
				},
				(BACKSLASH, false) => {
					esc = true;
				},
				(NULL, _) => {
					break;
				},
				_ => {
					esc = false;
				},
			}
		}
		if is_dir {
			libc::mkdir(target.as_ptr(), 0o755);
		} else {
			libc::creat(target.as_ptr(), 0o777);
		}
	}
}
