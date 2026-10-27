use {
	crate::{Result, process},
	std::{
		io,
		os::fd::{AsRawFd as _, OwnedFd, RawFd},
		sync::atomic::{AtomicI32, Ordering},
	},
};

static WRITE: AtomicI32 = AtomicI32::new(-1);

/// Signals delivered through a pipe, so that a poll loop can wait for them alongside other file descriptors.
pub struct Signals {
	read: OwnedFd,
	_write: OwnedFd,
}

impl Signals {
	pub fn new(signals: &[libc::c_int]) -> Result<Self> {
		// Create the pipe that the handler writes signal numbers to.
		let (read, write) = process::pipe()?;
		process::set_nonblocking(&read)?;
		process::set_nonblocking(&write)?;
		WRITE.store(write.as_raw_fd(), Ordering::SeqCst);

		// Install the handler for each signal.
		for signal in signals {
			// SAFETY: The action is zeroed and then initialized, and the handler only calls async-signal-safe functions.
			let result = unsafe {
				let mut action: libc::sigaction = std::mem::zeroed();
				action.sa_sigaction = handler as *const () as libc::sighandler_t;
				action.sa_flags = libc::SA_RESTART | libc::SA_NOCLDSTOP;
				libc::sigemptyset(&raw mut action.sa_mask);
				libc::sigaction(*signal, &raw const action, std::ptr::null_mut())
			};
			if result != 0 {
				return Err(io::Error::last_os_error().into());
			}
		}

		let signals = Self {
			read,
			_write: write,
		};

		Ok(signals)
	}

	#[must_use]
	pub fn fd(&self) -> RawFd {
		self.read.as_raw_fd()
	}

	/// Take the signals that have been delivered since the last call.
	pub fn take(&self) -> Vec<libc::c_int> {
		let mut signals = Vec::new();
		let mut buffer = [0u8; 64];
		loop {
			// SAFETY: The buffer is valid for writes of its length.
			let n = unsafe {
				libc::read(
					self.read.as_raw_fd(),
					buffer.as_mut_ptr().cast(),
					buffer.len(),
				)
			};
			let Ok(n) = usize::try_from(n) else {
				break;
			};
			if n == 0 {
				break;
			}
			signals.extend(buffer[..n].iter().map(|byte| libc::c_int::from(*byte)));
		}
		signals
	}

	/// Wait until a signal is delivered or the deadline passes.
	pub fn wait(&self, deadline: Option<std::time::Instant>) -> Result<Vec<libc::c_int>> {
		let mut fds = [libc::pollfd {
			events: libc::POLLIN,
			fd: self.fd(),
			revents: 0,
		}];
		process::poll(&mut fds, deadline)?;
		let signals = self.take();
		Ok(signals)
	}
}

extern "C" fn handler(signal: libc::c_int) {
	let errno = process::errno();
	let fd = WRITE.load(Ordering::Relaxed);
	let byte = u8::try_from(signal).unwrap_or(0);
	// SAFETY: write is async-signal-safe, and the byte is valid for reads of one byte.
	unsafe {
		libc::write(fd, (&raw const byte).cast(), 1);
	}
	process::set_errno(errno);
}
