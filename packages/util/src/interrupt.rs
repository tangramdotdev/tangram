use std::sync::Mutex;

static STATE: Mutex<Option<State>> = Mutex::new(None);

/// Defer the terminal's interrupt in a parent while its children handle it and finish cleanup.
pub struct Guard(());

struct State {
	count: usize,
	previous: libc::sigaction,
}

impl Guard {
	pub fn new() -> std::io::Result<Option<Self>> {
		let Some(tty) = crate::tty::open_controlling_tty() else {
			return Ok(None);
		};
		use std::os::fd::AsRawFd as _;
		if !crate::tty::is_foreground_controlling_tty(tty.as_raw_fd()) {
			return Ok(None);
		}

		let mut state = STATE.lock().unwrap();
		if let Some(state) = state.as_mut() {
			state.count += 1;
		} else {
			// Use a caught signal so exec restores the default handler in the children.
			// SAFETY: Both actions are initialized before they are read, and the handler does not access any shared state.
			let previous = unsafe {
				let mut action = std::mem::zeroed::<libc::sigaction>();
				action.sa_sigaction = defer as *const () as usize;
				action.sa_flags = libc::SA_RESTART;
				libc::sigemptyset(&raw mut action.sa_mask);
				let mut previous = std::mem::MaybeUninit::uninit();
				if libc::sigaction(libc::SIGINT, &raw const action, previous.as_mut_ptr()) != 0 {
					return Err(std::io::Error::last_os_error());
				}
				previous.assume_init()
			};
			*state = Some(State { count: 1, previous });
		}

		Ok(Some(Self(())))
	}
}

impl Drop for Guard {
	fn drop(&mut self) {
		let mut state = STATE.lock().unwrap();
		let Some(current) = state.as_mut() else {
			return;
		};
		current.count -= 1;
		if current.count != 0 {
			return;
		}
		let previous = state.take().unwrap().previous;

		// Restore the previous handler unless another subsystem has installed a new handler.
		// SAFETY: The action is initialized by sigaction before it is read, and the previous action came from sigaction.
		unsafe {
			let mut action = std::mem::MaybeUninit::<libc::sigaction>::uninit();
			if libc::sigaction(libc::SIGINT, std::ptr::null(), action.as_mut_ptr()) == 0
				&& action.assume_init().sa_sigaction == defer as *const () as usize
			{
				libc::sigaction(libc::SIGINT, &raw const previous, std::ptr::null_mut());
			}
		}
	}
}

extern "C" fn defer(_signal: libc::c_int) {}
