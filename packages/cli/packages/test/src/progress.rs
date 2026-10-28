use {
	crate::test::{Output, SKIPPED, Stop},
	std::{
		fmt::Write as _,
		io::{IsTerminal as _, Write as _},
		time::{Duration, Instant},
	},
};

/// The interval between frames.
pub const FRAME: Duration = Duration::from_millis(100);

/// The progress display. In live mode, it renders frames at a fixed rate, each of which prints the messages queued since the previous frame above a region that lists the running tests and the aggregate progress.
pub struct Progress {
	details: bool,
	live: bool,
	pending: String,
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

/// Describe why a test failed. Only the runner's own interrupts and timeouts are described as such, because a test may exit with the same codes for other reasons.
#[must_use]
pub fn describe(output: &Output) -> String {
	match (output.stop, output.code) {
		(Some(Stop::Interrupted), _) => "interrupted".to_owned(),
		(Some(Stop::TimedOut), _) => "timed out".to_owned(),
		(None, 137) => "killed with SIGKILL".to_owned(),
		(None, 143) => "terminated with SIGTERM".to_owned(),
		(None, code) if code > 128 => format!("terminated by signal {}", code - 128),
		(None, _) => "failed".to_owned(),
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
			write("\x1b[?25l");
		}
		Self {
			details,
			live,
			pending: String::new(),
			print_passing_test_output,
			start: Instant::now(),
		}
	}

	/// Print the pending messages, clear the live region, and restore the cursor, in one write.
	pub fn finish(&mut self) {
		if !self.live {
			return;
		}
		let mut frame = String::from("\x1b[?2026h\r");
		push_lines(&mut frame, &std::mem::take(&mut self.pending));
		frame.push_str("\x1b[J\x1b[?25h\x1b[?2026l");
		write(&frame);
		self.live = false;
	}

	/// Print a message. In live mode, it is printed above the live region with the next frame.
	pub fn message(&mut self, text: &str) {
		if self.live {
			self.pending.push_str(text);
		} else {
			write(text);
		}
	}

	/// Render a frame: the pending messages followed by the live region, written and flushed once. The lines are overwritten in place within a synchronized update, so the terminal never shows a cleared or partially drawn region.
	pub fn render(&mut self, running: &[Running], counts: &Counts) {
		if !self.live {
			return;
		}
		let (columns, rows) = crossterm::terminal::size().unwrap_or((80, 24));
		let width = usize::from(columns).saturating_sub(1);

		// Begin the synchronized update at the start of the region, and print the pending messages over the previous region.
		let mut frame = String::from("\x1b[?2026h\r");
		push_lines(&mut frame, &std::mem::take(&mut self.pending));

		// List the running tests, leaving room for the progress bar so that the region fits on the screen.
		let mut lines = 0;
		if self.details {
			let capacity = usize::from(rows).saturating_sub(2);
			let shown = if running.len() > capacity {
				capacity.saturating_sub(1)
			} else {
				running.len()
			};
			for test in &running[..shown] {
				let text = format!("{} {}s", test.name, test.start.elapsed().as_secs());
				let text = truncate(&text, width.saturating_sub(2));
				writeln!(frame, "\x1b[34m●\x1b[0m {text}\x1b[K").unwrap();
				lines += 1;
			}
			if shown < running.len() {
				let text = format!("… and {} more", running.len() - shown);
				let text = truncate(&text, width.saturating_sub(2));
				writeln!(frame, "  {text}\x1b[K").unwrap();
				lines += 1;
			}
		}

		// Draw the progress bar, and clear whatever remains of the previous region below it.
		let filled = match counts.total {
			Some(total) if total > 0 => (counts.completed * 10 / total).min(10),
			_ => 0,
		};
		let bar = format!("{}>{}", "=".repeat(filled), " ".repeat(10 - filled));
		let total = counts
			.total
			.map_or_else(|| "∞".to_owned(), |total| total.to_string());
		let elapsed = self.start.elapsed().as_secs();
		write!(
			frame,
			"[{bar}] {}/{total}: {} running, \x1b[32m{} passed\x1b[0m, \x1b[33m{} skipped\x1b[0m, \x1b[31m{} failed\x1b[0m, {elapsed}s\x1b[K\x1b[J",
			counts.completed,
			running.len(),
			counts.passed,
			counts.skipped,
			counts.failed,
		)
		.unwrap();

		// Return the cursor to the start of the region and end the synchronized update.
		frame.push('\r');
		if lines > 0 {
			write!(frame, "\x1b[{lines}A").unwrap();
		}
		frame.push_str("\x1b[?2026l");

		write(&frame);
	}

	/// Print a test's result.
	pub fn result(&mut self, output: &Output) {
		let symbol = if output.code == 0 {
			"\x1b[32m✓\x1b[0m"
		} else if output.code == SKIPPED {
			"\x1b[33m⊘\x1b[0m"
		} else {
			"\x1b[31m✗\x1b[0m"
		};
		let status = if output.failed() {
			format!(" — exit {} ({})", output.code, describe(output))
		} else {
			String::new()
		};
		let mut text = String::new();
		writeln!(
			text,
			"{symbol} {} {}{status}",
			output.name,
			duration(output.duration)
		)
		.unwrap();
		if output.code == SKIPPED {
			let reason = output
				.skip_reason
				.as_deref()
				.unwrap_or_else(|| output.log.trim());
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
		self.message(&text);
	}
}

impl Drop for Progress {
	fn drop(&mut self) {
		self.finish();
	}
}

/// Append text to a frame, clearing the remainder of each line that it overwrites.
fn push_lines(frame: &mut String, text: &str) {
	for line in text.lines() {
		frame.push_str(line);
		frame.push_str("\x1b[K\n");
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

/// Write to stderr and flush once.
fn write(text: &str) {
	let mut stderr = std::io::stderr().lock();
	stderr.write_all(text.as_bytes()).ok();
	stderr.flush().ok();
}
