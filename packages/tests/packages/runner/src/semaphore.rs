use {
	crate::Result,
	std::{
		collections::BTreeSet,
		ffi::CString,
		io,
		os::unix::fs::MetadataExt as _,
		path::{Path, PathBuf},
		process::ExitCode,
	},
};

/// The alphabet that LMDB uses to encode its default semaphore names.
const ALPHABET: &[u8] =
	b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#$%&()*+-;<=>?@^_`{|}~";

/// The name of the manifest of semaphore names in a test's temporary directory.
pub const MANIFEST: &str = "posix_semaphores";

/// Manage the POSIX semaphores that LMDB creates.
#[derive(clap::Subcommand)]
pub enum Command {
	/// Remove the semaphores recorded or configured under a test's temporary directory.
	Cleanup { path: PathBuf },

	/// Create semaphores, as a crashed server would leave them.
	Create { names: Vec<String> },

	/// Exit successfully if every semaphore exists.
	Exists { names: Vec<String> },

	/// Print the default semaphore names for LMDB lock files.
	Names { paths: Vec<PathBuf> },

	/// Unlink semaphores.
	Unlink { names: Vec<String> },
}

pub fn command(command: &Command) -> Result<ExitCode> {
	match command {
		Command::Cleanup { path } => cleanup(path)?,
		Command::Create { names } => {
			for name in names {
				open(name, true)?;
			}
		},
		Command::Exists { names } => {
			for name in names {
				open(name, false)?;
			}
		},
		Command::Names { paths } => {
			for path in paths {
				for name in lock_file_names(path)? {
					println!("{name}");
				}
			}
		},
		Command::Unlink { names } => {
			for name in names {
				unlink(name)?;
			}
		},
	}
	Ok(ExitCode::SUCCESS)
}

/// Remove the semaphores that a test's servers may have left behind. The names come from the manifest, from the prefixes in config files, and from the device and inode of each LMDB lock file.
pub fn cleanup(path: &Path) -> Result<()> {
	// Read the manifest.
	let mut names = BTreeSet::new();
	if let Ok(manifest) = std::fs::read_to_string(path.join(MANIFEST)) {
		names.extend(
			manifest
				.lines()
				.filter(|line| !line.is_empty())
				.map(ToOwned::to_owned),
		);
	}

	// Find the config files and lock files.
	let mut configs = Vec::new();
	let mut lock_files = Vec::new();
	crate::temp::walk(path, &mut |path| {
		let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
			return;
		};
		if name == "config.json" {
			configs.push(path.to_owned());
		} else if name.ends_with("-lock") {
			lock_files.push(path.to_owned());
		}
	});

	// Add the names configured with explicit prefixes.
	for config in configs {
		let Ok(contents) = std::fs::read(&config) else {
			continue;
		};
		let Ok(config) = serde_json::from_slice::<serde_json::Value>(&contents) else {
			continue;
		};
		for key in ["cache", "index"] {
			if let Some(prefix) = config
				.get(key)
				.and_then(|value| value.get("posix_sem_prefix"))
				.and_then(|value| value.as_str())
			{
				names.insert(format!("{prefix}r"));
				names.insert(format!("{prefix}w"));
			}
		}
	}

	// Add LMDB's default names while the device and inode are still available.
	for lock_file in lock_files {
		names.extend(lock_file_names(&lock_file)?);
	}

	// Unlink the semaphores.
	for name in names {
		unlink(&name)?;
	}

	Ok(())
}

/// Compute LMDB's default semaphore names for a lock file: the FNV-1a hash of its zero-padded device and inode pair.
pub fn lock_file_names(path: &Path) -> Result<[String; 2]> {
	#[repr(C)]
	struct Identity {
		device: libc::dev_t,
		inode: libc::ino_t,
	}

	let metadata = std::fs::metadata(path)
		.map_err(|error| format!("failed to read the lock file {}: {error}", path.display()))?;
	let mut identity = std::mem::MaybeUninit::<Identity>::zeroed();
	let pointer = identity.as_mut_ptr();
	// SAFETY: The pointer is valid for writes of each field, and the remaining bytes are zeroed padding.
	let bytes = unsafe {
		(&raw mut (*pointer).device).write(libc::dev_t::try_from(metadata.dev())?);
		(&raw mut (*pointer).inode).write(metadata.ino());
		std::slice::from_raw_parts(pointer.cast::<u8>(), std::mem::size_of::<Identity>())
	};
	let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
	for byte in bytes {
		hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
	}
	let mut suffix = String::new();
	while hash != 0 && suffix.len() < 10 {
		suffix.push(char::from(ALPHABET[usize::try_from(hash % 85).unwrap()]));
		hash /= 85;
	}
	let names = [format!("/MDBr{suffix}"), format!("/MDBw{suffix}")];
	Ok(names)
}

fn open(name: &str, create: bool) -> Result<()> {
	let c_name = CString::new(name)?;
	// SAFETY: The name is a valid C string, and the mode and value are passed as the variadic arguments that O_CREAT requires.
	let semaphore = unsafe {
		if create {
			libc::sem_open(
				c_name.as_ptr(),
				libc::O_CREAT | libc::O_EXCL,
				0o600 as libc::c_uint,
				1 as libc::c_uint,
			)
		} else {
			libc::sem_open(c_name.as_ptr(), 0)
		}
	};
	if semaphore == libc::SEM_FAILED {
		let error = io::Error::last_os_error();
		return Err(format!("{name}: {error}").into());
	}
	// SAFETY: The semaphore was opened successfully.
	unsafe {
		libc::sem_close(semaphore);
	}
	Ok(())
}

fn unlink(name: &str) -> Result<()> {
	let c_name = CString::new(name)?;
	// SAFETY: The name is a valid C string.
	let result = unsafe { libc::sem_unlink(c_name.as_ptr()) };
	if result != 0 {
		let error = io::Error::last_os_error();
		if error.raw_os_error() != Some(libc::ENOENT) {
			return Err(format!("{name}: {error}").into());
		}
	}
	Ok(())
}
