use {
	std::{
		io::{Seek as _, Write},
		path::Path,
	},
	tangram_cli_core::{Cli, log::Watcher},
	tangram_client::prelude::*,
};

/// The number of lines that `--follow` prints before it follows the log, unless `--lines` is given.
const FOLLOW_LINES: usize = 10;

/// Print the server's log.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Print the last lines of the log, and then print what the server writes until interrupted.
	#[arg(long, short)]
	pub follow: bool,

	/// Print only the last lines of the log.
	#[arg(long, short = 'n')]
	pub lines: Option<usize>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	// Get the log directory, and fail with a hint for where to find the log if the server writes none.
	let directory = cli.server_log_directory_path()?;
	let is_directory = tokio::fs::metadata(&directory)
		.await
		.is_ok_and(|metadata| metadata.is_dir());
	if !is_directory {
		let manager = cli
			.try_read_server_lock_if_readable()
			.await
			.ok()
			.flatten()
			.and_then(|lock| lock.manager)
			.or_else(|| cli.server_manager());
		return Err(no_log_error(manager, &directory));
	}

	// Print the log.
	tokio::task::spawn_blocking(move || {
		let stdout = std::io::stdout();
		let mut stdout = stdout.lock();
		let result = if args.follow {
			let lines = args.lines.unwrap_or(FOLLOW_LINES);
			follow(&directory, lines, &mut stdout)
		} else {
			print(&directory, args.lines, &mut stdout)
		};
		match result {
			Ok(()) => Ok(()),
			Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
			Err(error) => {
				Err(tg::error!(!error, path = %directory.display(), "failed to print the log"))
			},
		}
	})
	.await
	.map_err(|error| tg::error!(!error, "the log task panicked"))??;

	Ok(())
}

/// Print the files in the log directory from the oldest to the newest, or only their last lines.
fn print(directory: &Path, lines: Option<usize>, writer: &mut impl Write) -> std::io::Result<()> {
	let (mut files, _) = open(directory)?;
	let (index, offset) = match lines {
		Some(lines) => tangram_cli_core::log::tail(&mut files, lines)?,
		None => (0, 0),
	};
	for (i, file) in files.iter_mut().enumerate().skip(index) {
		let offset = if i == index { offset } else { 0 };
		file.seek(std::io::SeekFrom::Start(offset))?;
		std::io::copy(file, writer)?;
	}
	writer.flush()?;
	Ok(())
}

/// Print the last lines of the log, and then print what the server writes, moving to the next file when the server starts one.
fn follow(directory: &Path, lines: usize, writer: &mut impl Write) -> std::io::Result<()> {
	// Create the watcher before reading, so that no change after the read is missed.
	let mut watcher = Watcher::new(directory)?;

	// Print the last lines.
	let (mut files, mut names) = open(directory)?;
	let (index, offset) = tangram_cli_core::log::tail(&mut files, lines)?;
	for (i, file) in files.iter_mut().enumerate().skip(index) {
		let offset = if i == index { offset } else { 0 };
		file.seek(std::io::SeekFrom::Start(offset))?;
		std::io::copy(file, writer)?;
	}
	writer.flush()?;

	// Follow the newest file, or the file before it if the newest is empty, because the server creates a file before it moves its output to it.
	let mut current = files.pop().zip(names.pop());
	if current
		.as_ref()
		.is_some_and(|(file, _)| file.metadata().is_ok_and(|metadata| metadata.len() == 0))
		&& let Some(previous) = files.pop().zip(names.pop())
	{
		current = Some(previous);
	}
	if let Some((file, _)) = &current {
		watcher.watch_file(file)?;
	}
	loop {
		// Print what was written to the current file.
		if let Some((file, _)) = &mut current {
			std::io::copy(file, writer)?;
			writer.flush()?;
		}

		// Find the next file.
		let names = tangram_cli_core::log::list(directory)?;
		let mut later = names
			.into_iter()
			.filter(|name| current.as_ref().is_none_or(|(_, current)| name > current));
		let Some(name) = later.next() else {
			watcher.wait()?;
			continue;
		};
		let file = match std::fs::File::open(directory.join(&name)) {
			Ok(file) => file,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				continue;
			},
			Err(error) => {
				return Err(error);
			},
		};

		// Wait for the server to move its output to the next file, which it has done once the file has data or a later file exists, and watch the next file meanwhile, because the server writes to it first.
		let started = current.is_none() || later.next().is_some() || file.metadata()?.len() > 0;
		if !started {
			watcher.watch_file(&file)?;
			watcher.wait()?;
			continue;
		}

		// Print the rest of the current file, and move to the next file.
		if let Some((file, _)) = &mut current {
			std::io::copy(file, writer)?;
			writer.flush()?;
		}
		watcher.watch_file(&file)?;
		current = Some((file, name));
	}
}

/// Open the files in the log directory, oldest first, skipping files that were removed after they were listed.
fn open(directory: &Path) -> std::io::Result<(Vec<std::fs::File>, Vec<String>)> {
	let mut files = Vec::new();
	let mut names = Vec::new();
	for name in tangram_cli_core::log::list(directory)? {
		match std::fs::File::open(directory.join(&name)) {
			Ok(file) => {
				files.push(file);
				names.push(name);
			},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
			Err(error) => {
				return Err(error);
			},
		}
	}
	Ok((files, names))
}

/// Create the error for a server that writes no log, with a hint for where its manager keeps its output.
fn no_log_error(manager: Option<tg::server::Manager>, directory: &Path) -> tg::Error {
	match manager {
		Some(tg::server::Manager::Openrc) => {
			let hint = "read `/var/log/tangram.log`";
			tg::error!(%hint, "the server is managed by OpenRC, which writes its output to `/var/log/tangram.log`")
		},
		Some(tg::server::Manager::Systemd) => {
			let hint = "run `journalctl -u tangram`";
			tg::error!(%hint, "the server is managed by systemd, which writes its output to the journal")
		},
		Some(tg::server::Manager::App | tg::server::Manager::Cli) | None => {
			let hint =
				"start the server with `tangram server start`, or set the `log` key in the config";
			tg::error!(%hint, path = %directory.display(), "the server has no log")
		},
	}
}
