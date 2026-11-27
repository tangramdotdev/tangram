use {
	crate::{Cli, config::BoolOr},
	std::{
		io::{Read, Seek, Write as _},
		os::{fd::AsRawFd as _, unix::fs::PermissionsExt as _},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

mod watcher;

pub use self::watcher::Watcher;

/// The default maximum number of log files.
pub const DEFAULT_MAX_FILES: usize = 8;

/// The default size in bytes at which the server starts a new log file.
pub const DEFAULT_MAX_SIZE: u64 = 256 * 1024 * 1024;

/// The length of each chunk that is read when searching backwards for the start of the last lines.
const TAIL_CHUNK_LENGTH: u64 = 64 * 1024;

/// The format of the time in the name of a log file.
const TIME_FORMAT: &str = "%Y-%m-%dT%H-%M-%SZ";

/// The limits of the server's log.
#[derive(Clone, Copy, Debug)]
pub struct Options {
	pub max_files: usize,
	pub max_size: u64,
}

/// The server's current log file, which the server's stdout and stderr refer to.
struct Rotation {
	access: tg::server::Access,
	directory: PathBuf,
	file: std::fs::File,
	options: Options,
}

/// List the names of the log files in the directory, oldest first. Names that are not those of log files are ignored.
pub fn list(directory: &Path) -> std::io::Result<Vec<String>> {
	let mut names = Vec::new();
	for entry in std::fs::read_dir(directory)? {
		let entry = entry?;
		let Ok(name) = entry.file_name().into_string() else {
			continue;
		};
		if parse_name(&name).is_some() {
			names.push(name);
		}
	}
	names.sort_unstable();
	Ok(names)
}

/// Find the position at which the last lines of the files begin, as the index of a file and an offset in it, as if the files were one stream. A final newline does not begin a line.
pub fn tail<R: Read + Seek>(files: &mut [R], lines: usize) -> std::io::Result<(usize, u64)> {
	// Return the end of the files if no lines are requested.
	if lines == 0 {
		let Some(index) = files.len().checked_sub(1) else {
			return Ok((0, 0));
		};
		let length = files[index].seek(std::io::SeekFrom::End(0))?;
		return Ok((index, length));
	}

	// Search the files backwards for the newline that precedes the first of the lines.
	let mut remaining = lines;
	let mut last = true;
	for index in (0..files.len()).rev() {
		let file = &mut files[index];
		let mut end = file.seek(std::io::SeekFrom::End(0))?;
		while end > 0 {
			let start = end.saturating_sub(TAIL_CHUNK_LENGTH);
			file.seek(std::io::SeekFrom::Start(start))?;
			let length = usize::try_from(end - start).map_err(std::io::Error::other)?;
			let mut buffer = vec![0; length];
			file.read_exact(&mut buffer)?;
			let mut chunk = buffer.as_slice();
			if last {
				last = false;
				if let Some(rest) = chunk.strip_suffix(b"\n") {
					chunk = rest;
				}
			}
			if let Some(offset) = find_line_start(chunk, &mut remaining) {
				return Ok((index, start + offset as u64));
			}
			end = start;
		}
	}

	Ok((0, 0))
}

impl Cli {
	/// Redirect the server's stdout and stderr to a new file in its log directory if its log is enabled, and start a thread that rotates the log. `log` says that the output has nowhere else to go, which enables the log unless the config disables it, in which case the output is discarded.
	pub fn initialize_server_log(&self, log: bool, access: tg::server::Access) -> tg::Result<()> {
		// Resolve the options.
		let options = match self.config.as_ref().and_then(|config| config.log) {
			None if log => Some(Options::default()),
			None | Some(BoolOr::Bool(false)) => None,
			Some(BoolOr::Bool(true)) => Some(Options::default()),
			Some(BoolOr::Value(config)) => Some(Options::new(config)?),
		};

		// Discard the output if the log is disabled and the output has nowhere else to go.
		let Some(options) = options else {
			if log {
				let file = std::fs::OpenOptions::new()
					.write(true)
					.open("/dev/null")
					.map_err(|error| tg::error!(!error, "failed to open /dev/null"))?;
				redirect(&file)
					.map_err(|error| tg::error!(!error, "failed to redirect the output"))?;
			}
			return Ok(());
		};

		// Create the log directory, and let members of the server's group read it if the server's access is `group`.
		let directory = self.server_log_directory_path()?;
		std::fs::create_dir_all(&directory).map_err(
			|error| tg::error!(!error, path = %directory.display(), "failed to create the log directory"),
		)?;
		if access == tg::server::Access::Group {
			let permissions = std::fs::Permissions::from_mode(0o750);
			std::fs::set_permissions(&directory, permissions).map_err(|error| {
				tg::error!(!error, path = %directory.display(), "failed to set permissions on the log directory")
			})?;
		}

		// Create the first file and redirect the output to it.
		let file = create_file(&directory, access).map_err(
			|error| tg::error!(!error, path = %directory.display(), "failed to create the log file"),
		)?;
		redirect(&file).map_err(|error| tg::error!(!error, "failed to redirect the output"))?;
		remove_old_files(&directory, options.max_files).map_err(
			|error| tg::error!(!error, path = %directory.display(), "failed to remove the old log files"),
		)?;

		// Spawn the thread that rotates the log.
		let rotation = Rotation {
			access,
			directory,
			file,
			options,
		};
		std::thread::Builder::new()
			.name("log".to_owned())
			.spawn(move || rotation.run())
			.map_err(|error| tg::error!(!error, "failed to spawn the log thread"))?;

		Ok(())
	}

	/// Get the path of the server's log directory.
	pub fn server_log_directory_path(&self) -> tg::Result<PathBuf> {
		let path = self.server_directory_path()?.join("log");
		Ok(path)
	}
}

impl Options {
	/// Resolve the options from the config.
	pub fn new(config: crate::config::Log) -> tg::Result<Self> {
		let max_files = config.max_files.unwrap_or(DEFAULT_MAX_FILES);
		if max_files == 0 {
			return Err(tg::error!("the log's max_files must be at least 1"));
		}
		let max_size = config.max_size.unwrap_or(DEFAULT_MAX_SIZE);
		if max_size == 0 {
			return Err(tg::error!("the log's max_size must be at least 1"));
		}
		let options = Self {
			max_files,
			max_size,
		};
		Ok(options)
	}
}

impl Rotation {
	/// Start a new file when the current file reaches the maximum size, and remove the oldest files so that at most the maximum number remain.
	fn run(mut self) {
		// Create the watcher.
		let watcher = Watcher::new(&self.directory).and_then(|mut watcher| {
			watcher.watch_file(&self.file)?;
			Ok(watcher)
		});
		let mut watcher = match watcher {
			Ok(watcher) => watcher,
			Err(error) => {
				tracing::error!(%error, "failed to watch the log directory");
				return;
			},
		};

		// Rotate the log whenever the current file reaches the threshold, which moves past a file that could not be replaced, so that a failure is not retried on every write.
		let mut threshold = self.options.max_size;
		loop {
			let size = match self.file.metadata() {
				Ok(metadata) => metadata.len(),
				Err(error) => {
					tracing::error!(%error, "failed to get the log file metadata");
					return;
				},
			};
			if size >= threshold {
				match self.rotate(&mut watcher) {
					Ok(()) => {
						threshold = self.options.max_size;
					},
					Err(error) => {
						tracing::error!(%error, "failed to rotate the log");
						threshold = size.saturating_add(self.options.max_size);
					},
				}
				continue;
			}
			if let Err(error) = watcher.wait() {
				tracing::error!(%error, "failed to watch the log directory");
				return;
			}
		}
	}

	/// Create a new file, redirect the output to it, and remove the oldest files.
	fn rotate(&mut self, watcher: &mut Watcher) -> std::io::Result<()> {
		let file = create_file(&self.directory, self.access)?;
		redirect(&file)?;
		watcher.watch_file(&file)?;
		self.file = file;
		remove_old_files(&self.directory, self.options.max_files)?;
		Ok(())
	}
}

impl Default for Options {
	fn default() -> Self {
		Self {
			max_files: DEFAULT_MAX_FILES,
			max_size: DEFAULT_MAX_SIZE,
		}
	}
}

/// Create a new log file whose name sorts after every existing one.
fn create_file(directory: &Path, access: tg::server::Access) -> std::io::Result<std::fs::File> {
	loop {
		let names = list(directory)?;
		let now = jiff::Timestamp::now().strftime(TIME_FORMAT).to_string();
		let name = create_name(&now, names.last().map(String::as_str));
		let path = directory.join(name);
		let result = std::fs::OpenOptions::new()
			.append(true)
			.create_new(true)
			.open(&path);
		match result {
			Ok(file) => {
				if access == tg::server::Access::Group {
					let permissions = std::fs::Permissions::from_mode(0o640);
					file.set_permissions(permissions)?;
				}
				return Ok(file);
			},
			Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
			Err(error) => {
				return Err(error);
			},
		}
	}
}

/// Redirect stdout and stderr to the file.
fn redirect(file: &std::fs::File) -> std::io::Result<()> {
	std::io::stdout().flush().ok();
	for target in [libc::STDOUT_FILENO, libc::STDERR_FILENO] {
		// SAFETY: Both descriptors are open, and `dup2` replaces the target atomically, so that a concurrent write goes to either the old file or the new one.
		let ret = unsafe { libc::dup2(file.as_raw_fd(), target) };
		if ret < 0 {
			return Err(std::io::Error::last_os_error());
		}
	}
	Ok(())
}

/// Remove the oldest log files so that at most the maximum number remain.
fn remove_old_files(directory: &Path, max_files: usize) -> std::io::Result<()> {
	let names = list(directory)?;
	for name in old_names(&names, max_files) {
		match std::fs::remove_file(directory.join(name)) {
			Ok(()) => (),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
			Err(error) => {
				return Err(error);
			},
		}
	}
	Ok(())
}

/// Get the names of the oldest files to remove from the names sorted oldest first, so that at most the maximum number remain.
fn old_names(names: &[String], max_files: usize) -> &[String] {
	let count = names.len().saturating_sub(max_files);
	&names[..count]
}

/// Create the name of a new log file from the current time, which sorts after the newest existing name. A name that would not sort after it takes the newest name's time and the next counter.
fn create_name(now: &str, newest: Option<&str>) -> String {
	match newest.and_then(parse_name) {
		Some((time, counter)) if time >= now => format_name(time, counter + 1),
		_ => format_name(now, 0),
	}
}

/// Format the name of a log file from its time and counter. The counter distinguishes files whose times are equal, and is omitted when it is zero.
fn format_name(time: &str, counter: u32) -> String {
	if counter == 0 {
		format!("{time}.log")
	} else {
		format!("{time}_{counter:06}.log")
	}
}

/// Parse the name of a log file into its time and counter.
fn parse_name(name: &str) -> Option<(&str, u32)> {
	let stem = name.strip_suffix(".log")?;
	let (time, counter) = match stem.split_once('_') {
		Some((time, counter)) => {
			if counter.len() != 6 || !counter.bytes().all(|byte| byte.is_ascii_digit()) {
				return None;
			}
			let counter = counter.parse::<u32>().ok().filter(|counter| *counter > 0)?;
			(time, counter)
		},
		None => (stem, 0),
	};
	let pattern = b"0000-00-00T00-00-00Z";
	let valid = time.len() == pattern.len()
		&& time
			.bytes()
			.zip(pattern)
			.all(|(byte, expected)| match expected {
				b'0' => byte.is_ascii_digit(),
				expected => byte == *expected,
			});
	if !valid {
		return None;
	}
	Some((time, counter))
}

/// Search a chunk backwards for newlines, counting each against the remaining lines, and return the offset after the newline that precedes the first of the lines.
fn find_line_start(chunk: &[u8], remaining: &mut usize) -> Option<usize> {
	for (index, byte) in chunk.iter().enumerate().rev() {
		if *byte == b'\n' {
			*remaining -= 1;
			if *remaining == 0 {
				return Some(index + 1);
			}
		}
	}
	None
}

#[cfg(test)]
mod tests {
	use {super::*, std::io::Cursor};

	// A new name takes the current time when it sorts after the newest name, and the newest name's time with the next counter otherwise.
	#[test]
	fn name() {
		assert_eq!(
			create_name("2026-11-27T18-04-47Z", None),
			"2026-11-27T18-04-47Z.log"
		);
		assert_eq!(
			create_name("2026-11-27T18-04-48Z", Some("2026-11-27T18-04-47Z.log")),
			"2026-11-27T18-04-48Z.log"
		);
		assert_eq!(
			create_name("2026-11-27T18-04-47Z", Some("2026-11-27T18-04-47Z.log")),
			"2026-11-27T18-04-47Z_000001.log"
		);
		assert_eq!(
			create_name(
				"2026-11-27T18-04-47Z",
				Some("2026-11-27T18-04-47Z_000009.log")
			),
			"2026-11-27T18-04-47Z_000010.log"
		);
		assert_eq!(
			create_name("2026-11-27T18-04-46Z", Some("2026-11-27T18-04-47Z.log")),
			"2026-11-27T18-04-47Z_000001.log"
		);
	}

	// Names sort in the order in which they were created.
	#[test]
	fn name_order() {
		let mut names = Vec::new();
		let mut newest: Option<String> = None;
		for now in [
			"2026-11-27T18-04-47Z",
			"2026-11-27T18-04-47Z",
			"2026-11-27T18-04-47Z",
			"2026-11-27T18-04-48Z",
			"2026-11-27T18-04-48Z",
			"2026-11-28T00-00-00Z",
		] {
			let name = create_name(now, newest.as_deref());
			names.push(name.clone());
			newest = Some(name);
		}
		for _ in 0..20 {
			let name = create_name("2026-11-28T00-00-00Z", newest.as_deref());
			names.push(name.clone());
			newest = Some(name);
		}
		let mut sorted = names.clone();
		sorted.sort();
		assert_eq!(names, sorted);
		let mut unique = names.clone();
		unique.dedup();
		assert_eq!(names, unique);
	}

	// Only the names of log files parse.
	#[test]
	fn name_parse() {
		assert_eq!(
			parse_name("2026-11-27T18-04-47Z.log"),
			Some(("2026-11-27T18-04-47Z", 0))
		);
		assert_eq!(
			parse_name("2026-11-27T18-04-47Z_000012.log"),
			Some(("2026-11-27T18-04-47Z", 12))
		);
		assert_eq!(parse_name("2026-11-27T18-04-47Z_000000.log"), None);
		assert_eq!(parse_name("2026-11-27T18-04-47Z_12.log"), None);
		assert_eq!(parse_name("2026-11-27T18-04-47Z"), None);
		assert_eq!(parse_name("2026-11-27T18:04:47Z.log"), None);
		assert_eq!(parse_name("log"), None);
		assert_eq!(parse_name(".log"), None);
	}

	// The oldest names are removed so that at most the maximum number remain.
	#[test]
	fn old() {
		let names = ["a", "b", "c", "d"].map(ToOwned::to_owned);
		assert_eq!(old_names(&names, 2), ["a", "b"]);
		assert_eq!(old_names(&names, 1), ["a", "b", "c"]);
		assert_eq!(old_names(&names, 4), [] as [String; 0]);
		assert_eq!(old_names(&names, 8), [] as [String; 0]);
		assert_eq!(old_names(&[], 1), [] as [String; 0]);
	}

	// The last lines begin after the newline that precedes them, across files, and a final newline does not begin a line.
	#[test]
	fn tail_lines() {
		let tail = |files: &[&str], lines: usize| {
			let mut files = files
				.iter()
				.map(|file| Cursor::new(file.as_bytes().to_vec()))
				.collect::<Vec<_>>();
			super::tail(&mut files, lines).unwrap()
		};
		assert_eq!(tail(&["a\nb\nc\n"], 2), (0, 2));
		assert_eq!(tail(&["a\nb\nc"], 2), (0, 2));
		assert_eq!(tail(&["a\nb\nc\n"], 3), (0, 0));
		assert_eq!(tail(&["a\nb\nc\n"], 10), (0, 0));
		assert_eq!(tail(&["a\nb\nc\n"], 0), (0, 6));
		assert_eq!(tail(&["a\nb\n", "c\n"], 2), (0, 2));
		assert_eq!(tail(&["a\nb\n", "c\n"], 1), (0, 4));
		assert_eq!(tail(&["a\nb\n", "c\n", ""], 1), (0, 4));
		assert_eq!(tail(&["a\nb\n", "c\nd\n"], 3), (0, 2));
		assert_eq!(tail(&["a\nb", "c\nd\n"], 2), (0, 2));
		assert_eq!(tail(&[], 2), (0, 0));
		assert_eq!(tail(&["\n\n"], 1), (0, 1));
	}

	// The search for the last lines reads chunks backwards across a chunk boundary.
	#[test]
	fn tail_chunks() {
		let length = usize::try_from(TAIL_CHUNK_LENGTH).unwrap();
		let mut file = vec![b'x'; length * 2];
		file[10] = b'\n';
		file.push(b'\n');
		let mut files = vec![Cursor::new(file)];
		assert_eq!(super::tail(&mut files, 1).unwrap(), (0, 11));
	}
}
