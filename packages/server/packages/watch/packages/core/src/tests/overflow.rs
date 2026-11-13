use {
	crate::{Graph, Message, Shared, Solutions, State, Watch},
	futures::FutureExt as _,
	std::{
		collections::HashSet,
		pin::pin,
		sync::{
			Arc, Mutex,
			atomic::{AtomicBool, Ordering},
		},
		task::{Context, Poll, Wake, Waker},
	},
};

#[derive(Default)]
struct WakeFlag(AtomicBool);

#[tokio::test]
async fn overflow_wakes_a_drained_channel_and_ignores_a_retired_watcher() {
	// Fill the channel and pause an overflowing send before recording the overflow.
	let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
	let overflow_notify = Arc::new(tokio::sync::Notify::new());
	let overflowed = Arc::new(AtomicBool::new(false));
	let state = State {
		affected_paths: HashSet::default(),

		graph: Graph::default(),

		index_task: Shared::spawn(|_| async { Ok(()) }),

		invalidated_paths: HashSet::default(),

		lock: None,

		overflow_notify: overflow_notify.clone(),

		overflowed: overflowed.clone(),

		#[cfg(target_os = "macos")]
		paths: HashSet::default(),

		pending_lock_write_version: None,

		revalidation_version: 0,

		sender: sender.clone(),

		solutions: Solutions::default(),

		timeout_task: None,

		version: 0,

		watcher: None,
	};
	let state = Mutex::new(state);
	sender.try_send(message()).unwrap();
	let result = sender.try_send(message());
	assert!(matches!(
		result,
		Err(tokio::sync::mpsc::error::TrySendError::Full(_))
	));

	// Drain the channel and wait for another event before the callback resumes.
	assert!(
		Watch::recv(&state, &mut receiver, &overflow_notify)
			.now_or_never()
			.unwrap()
			.is_some()
	);
	assert!(!overflowed.swap(false, Ordering::SeqCst));
	let wake = Arc::new(WakeFlag::default());
	let waker = Waker::from(wake.clone());
	let mut context = Context::from_waker(&waker);
	let mut receive = pin!(Watch::recv(&state, &mut receiver, &overflow_notify));
	assert!(matches!(receive.as_mut().poll(&mut context), Poll::Pending));

	// Resume the callback and invalidate the cached version without another event.
	overflowed.store(true, Ordering::SeqCst);
	overflow_notify.notify_one();
	assert!(wake.0.swap(false, Ordering::SeqCst));
	assert!(matches!(receive.as_mut().poll(&mut context), Poll::Pending));
	let version = state.lock().unwrap().version;
	assert_eq!(version, 1);
	assert!(!state.lock().unwrap().can_publish(&Graph::default(), 0));

	// Ignore an overflow recorded by the retired watcher after the reset.
	overflowed.store(true, Ordering::SeqCst);
	overflow_notify.notify_one();
	assert!(wake.0.load(Ordering::SeqCst));
	assert!(matches!(receive.as_mut().poll(&mut context), Poll::Pending));
	assert_eq!(state.lock().unwrap().version, version);
}

impl Wake for WakeFlag {
	fn wake(self: Arc<Self>) {
		self.0.store(true, Ordering::SeqCst);
	}
}

fn message() -> Message {
	Message {
		event: notify::Event::new(notify::EventKind::Any),
		sender: None,
	}
}
