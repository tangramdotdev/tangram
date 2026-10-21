use {
	crate::test::{Output, SKIPPED},
	std::{
		fmt::Write as _,
		io::{IsTerminal as _, Write as _},
		time::{Duration, Instant},
	},
};

/// The progress display, which prints results above a live region that lists the running tests and the aggregate progress.
pub struct Progress {
	details: bool,
	live: bool,
	lines: usize,
	print_passing_test_output: bool,
	start: Instant,
}

/// The counts that the progress bar shows.
pub struct Counts {
	pub completed: usize,
	pub failed: usize,
	pub passed: usize,
	pub skipped: usize,
	pub total: Option<usize>,
}

/// A test that is running.
pub struct Running<'a> {
	pub name: &'a str,
	pub start: Instant,
}

/// Describe an exit code.
#[must_use]
pub fn describe(code: i32) -> String {
	match code {
		124 => "timed out".to_owned(),
		130 => "interrupted".to_owned(),
		137 => "killed with SIGKILL".to_owned(),
		143 => "terminated with SIGTERM".to_owned(),
		code if code > 128 => format!("terminated by signal {}", code - 128),
		_ => "failed".to_owned(),
	}
}

/// Format a duration compactly.
#[must_use]
pub fn duration(duration: Duration) -> String {
	let seconds = duration.as_secs_f64();
	if seconds < 1.0 {
		format!("{}ms", duration.as_millis())
	} else if seconds < 60.0 {
		format!("{seconds:.2}s")
	} else {
		format!("{}m {:.1}s", duration.as_secs() / 60, seconds % 60.0)
	}
}

impl Progress {
	#[must_use]
	pub fn new(details: bool, live: bool, print_passing_test_output: bool) -> Self {
		let live = live && std::io::stderr().is_terminal();
		if live {
			eprint!("\x1b[?25l");
		}
		Self {
			details,
			live,
			lines: 0,
			print_passing_test_output,
			start: Instant::now(),
		}
	}

	/// Redraw the live region.
	pub fn draw(&mut self, running: &[Running], counts: &Counts) {
		if !self.live {
			return;
		}
		let mut output = String::from("\r\x1b[0J");
		let width = crossterm::terminal::size().map_or(80, |(columns, _)| usize::from(columns));
		let mut lines = 0;
		if self.details {
			for test in running {
				let text = format!("{} {}s", test.name, test.start.elapsed().as_secs());
				let text = truncate(&text, width.saturating_sub(2));
				writeln!(output, "\x1b[34m●\x1b[0m {text}").unwrap();
				lines += 1;
			}
		}
		let filled = match counts.total {
			Some(total) if total > 0 => (counts.completed * 10 / total).min(10),
			_ => 0,
		};
		let bar = format!("{}>{}", "=".repeat(filled), " ".repeat(10 - filled));
		let total = counts
			.total
			.map_or_else(|| "∞".to_owned(), |total| total.to_string());
		let text = format!(
			"[{bar}] {}/{total}: {} running, \x1b[32m{} passed\x1b[0m, \x1b[33m{} skipped\x1b[0m, \x1b[31m{} failed\x1b[0m, {}s",
			counts.completed,
			running.len(),
			counts.passed,
			counts.skipped,
			counts.failed,
			self.start.elapsed().as_secs(),
		);
		output.push_str(&text);
		if lines > 0 {
			write!(output, "\r\x1b[{lines}A").unwrap();
		} else {
			output.push('\r');
		}
		self.lines = lines;
		eprint!("{output}");
		std::io::stderr().flush().ok();
	}

	/// Clear the live region and restore the cursor.
	pub fn finish(&mut self) {
		if self.live {
			eprint!("\r\x1b[0J\x1b[?25h");
			std::io::stderr().flush().ok();
			self.live = false;
		}
	}

	/// Print a test's result above the live region.
	pub fn print(&mut self, output: &Output) {
		let mut text = String::new();
		if self.live {
			text.push_str("\r\x1b[0J");
		}
		let symbol = if output.code == 0 {
			"\x1b[32m✓\x1b[0m"
		} else if output.code == SKIPPED {
			"\x1b[33m⊘\x1b[0m"
		} else {
			"\x1b[31m✗\x1b[0m"
		};
		let status = if output.failed() {
			format!(" — exit {} ({})", output.code, describe(output.code))
		} else {
			String::new()
		};
		writeln!(
			text,
			"{symbol} {} {}{status}",
			output.name,
			duration(output.duration)
		)
		.unwrap();
		if output.code == SKIPPED {
			let reason = output.log.trim();
			if !reason.is_empty() {
				writeln!(text, "  {reason}").unwrap();
			}
		} else if self.print_passing_test_output || output.code != 0 {
			if output.log.is_empty() {
				text.push_str("  no output was captured\n");
			} else {
				text.push_str(&output.log);
				if !output.log.ends_with('\n') {
					text.push('\n');
				}
			}
			writeln!(
				text,
				"  phases: execution {}, cleanup {}",
				duration(output.execution_duration),
				duration(output.cleanup_duration)
			)
			.unwrap();
		}
		eprint!("{text}");
		std::io::stderr().flush().ok();
	}
}

impl Drop for Progress {
	fn drop(&mut self) {
		self.finish();
	}
}

fn truncate(text: &str, width: usize) -> String {
	if text.chars().count() <= width {
		return text.to_owned();
	}
	let mut output = text
		.chars()
		.take(width.saturating_sub(1))
		.collect::<String>();
	output.push('…');
	output
}
