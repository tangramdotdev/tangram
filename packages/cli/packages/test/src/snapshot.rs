use {
	crate::{Result, test::Test},
	std::{
		io::Write as _,
		path::{Path, PathBuf},
	},
};

/// An inline snapshot that did not match, recorded by the test library.
#[derive(serde::Deserialize)]
struct Entry {
	length: usize,
	new: String,
	old: String,
	position: usize,
}

/// Accept every pending file snapshot and inline snapshot.
pub fn accept(tests: &[Test]) -> Result<()> {
	for test in tests {
		for pending in files(&test.path, "pending") {
			std::fs::rename(&pending, pending.with_extension("snapshot"))?;
		}
		let inline = test.path.with_extension("inline");
		if let Ok(contents) = std::fs::read_to_string(&inline) {
			let entries = serde_json::from_str::<Vec<Entry>>(&contents)?;
			apply(&test.path, &entries)?;
			std::fs::remove_file(&inline)?;
		}
	}
	Ok(())
}

/// Remove the snapshot state that a previous run of a test left behind.
pub fn prepare(test: &Test) {
	for extension in ["pending", "touched"] {
		for path in files(&test.path, extension) {
			std::fs::remove_file(path).ok();
		}
	}
	std::fs::remove_file(test.path.with_extension("inline")).ok();
}

/// Remove the snapshots that a passing test did not touch, and remove the touch files.
pub fn prune(test: &Test) {
	for snapshot in files(&test.path, "snapshot") {
		if !snapshot.with_extension("touched").exists() {
			std::fs::remove_file(snapshot).ok();
		}
	}
	for touched in files(&test.path, "touched") {
		std::fs::remove_file(touched).ok();
	}
}

/// Interactively review every pending file snapshot and inline snapshot.
pub fn review(tests: &[Test]) -> Result<()> {
	for test in tests {
		// Review the file snapshots.
		let pending = files(&test.path, "pending");
		for pending in &pending {
			let snapshot = pending.with_extension("snapshot");
			clear();
			if snapshot.exists() {
				eprintln!("\x1b[33mchanged\x1b[0m {}", snapshot.display());
				eprintln!("{}", diff(&snapshot, pending));
			} else {
				eprintln!("\x1b[32madded\x1b[0m {}", snapshot.display());
				eprintln!();
				eprintln!("\x1b[32m{}\x1b[0m", std::fs::read_to_string(pending)?);
			}
			eprintln!();
			if prompt()? {
				std::fs::rename(pending, &snapshot)?;
			} else {
				std::fs::remove_file(pending)?;
			}
			eprintln!();
		}

		// Review the inline snapshots.
		let inline = test.path.with_extension("inline");
		let has_inline = inline.exists();
		if let Ok(contents) = std::fs::read_to_string(&inline) {
			let entries = serde_json::from_str::<Vec<Entry>>(&contents)?;
			clear();
			eprintln!("\x1b[33mchanged\x1b[0m {}", test.path.display());
			for entry in &entries {
				let directory = std::env::temp_dir();
				let old = directory.join(format!("tangram_review_old_{}", std::process::id()));
				let new = directory.join(format!("tangram_review_new_{}", std::process::id()));
				std::fs::write(&old, &entry.old)?;
				std::fs::write(&new, &entry.new)?;
				eprintln!("{}", diff(&old, &new));
				std::fs::remove_file(old).ok();
				std::fs::remove_file(new).ok();
			}
			eprintln!();
			if prompt()? {
				apply(&test.path, &entries)?;
			}
			std::fs::remove_file(&inline)?;
			eprintln!();
		}

		// Delete the snapshots which were not touched and remove the touch files.
		if !pending.is_empty() || has_inline {
			prune(test);
		}
	}
	Ok(())
}

/// Replace each inline snapshot literal in a test's source with its new value.
fn apply(path: &Path, entries: &[Entry]) -> Result<()> {
	let mut source = std::fs::read_to_string(path)?;
	let mut entries = entries.iter().collect::<Vec<_>>();
	entries.sort_by_key(|entry| std::cmp::Reverse(entry.position));
	for entry in entries {
		let indent = indent(&source, entry.position);
		let literal = literal(&entry.new, &indent);
		source.replace_range(entry.position..entry.position + entry.length, &literal);
	}
	std::fs::write(path, source)?;
	Ok(())
}

fn clear() {
	eprint!("\x1b[2J\x1b[3J\x1b[H");
}

fn diff(old: &Path, new: &Path) -> String {
	let output = std::process::Command::new("delta")
		.args([
			"--file-style=omit",
			"--hunk-header-style=omit",
			"--no-gitconfig",
		])
		.arg(old)
		.arg(new)
		.output();
	if let Ok(output) = output {
		return String::from_utf8_lossy(&output.stdout).into_owned();
	}
	let output = std::process::Command::new("diff")
		.arg("-u")
		.arg(old)
		.arg(new)
		.output();
	output
		.map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
		.unwrap_or_default()
}

/// Get the paths of a test's file snapshot state with an extension: `<stem>.<extension>` beside the test and `<stem>/*.<extension>`.
fn files(path: &Path, extension: &str) -> Vec<PathBuf> {
	let mut output = Vec::new();
	let file = path.with_extension(extension);
	if file.exists() {
		output.push(file);
	}
	if let Ok(read_dir) = std::fs::read_dir(path.with_extension("")) {
		for entry in read_dir.flatten() {
			let path = entry.path();
			if path.extension().is_some_and(|value| value == extension) {
				output.push(path);
			}
		}
	}
	output.sort();
	output
}

/// Get the leading whitespace of the line containing a position.
fn indent(source: &str, position: usize) -> String {
	let start = source[..position].rfind('\n').map_or(0, |index| index + 1);
	source[start..position]
		.chars()
		.take_while(|character| character.is_whitespace())
		.collect()
}

/// Format a value as a Nushell string literal, using a raw string if it contains a single quote, and indenting multiline values one level beyond the snapshot call.
fn literal(value: &str, indent: &str) -> String {
	let (open, close) = if value.contains('\'') {
		("r#'", "'#")
	} else {
		("'", "'")
	};
	if !value.contains('\n') {
		return format!("{open}{value}{close}");
	}
	let trailing_newline = value.ends_with('\n');
	let lines = value
		.trim_end_matches('\n')
		.split('\n')
		.map(|line| format!("{indent}\t{line}"))
		.collect::<Vec<_>>()
		.join("\n");
	if trailing_newline {
		format!("{open}\n{lines}\n\n{indent}{close}")
	} else {
		format!("{open}\n{lines}\n{indent}{close}")
	}
}

/// Prompt to accept or reject a change. Return true to accept it.
fn prompt() -> Result<bool> {
	eprint!("\x1b[32m[a]ccept\x1b[0m or \x1b[31m[r]eject\x1b[0m: ");
	std::io::stderr().flush()?;
	crossterm::terminal::enable_raw_mode()?;
	let accept = loop {
		let event = crossterm::event::read();
		let event = match event {
			Ok(event) => event,
			Err(error) => {
				crossterm::terminal::disable_raw_mode()?;
				return Err(error.into());
			},
		};
		if let crossterm::event::Event::Key(key) = event
			&& key.kind == crossterm::event::KeyEventKind::Press
		{
			match key.code {
				crossterm::event::KeyCode::Char('a') => break true,
				crossterm::event::KeyCode::Char('r') => break false,
				crossterm::event::KeyCode::Char('c')
					if key
						.modifiers
						.contains(crossterm::event::KeyModifiers::CONTROL) =>
				{
					crossterm::terminal::disable_raw_mode()?;
					return Err("the review was interrupted".into());
				},
				_ => (),
			}
		}
	};
	crossterm::terminal::disable_raw_mode()?;
	eprintln!();
	Ok(accept)
}
