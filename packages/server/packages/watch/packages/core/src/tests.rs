use {
	super::{Message, State},
	std::sync::atomic::{AtomicBool, Ordering},
	tokio::sync::mpsc::{self, error::TrySendError},
};

#[cfg(target_os = "linux")]
mod linux;

#[test]
fn overflow_after_queue_drains_notifies_receiver() {
	// Fill the channel and fail to send another event.
	let (sender, mut receiver) = mpsc::channel(1);
	let overflowed = AtomicBool::new(false);
	assert!(sender.try_send(message()).is_ok());
	let Err(TrySendError::Full(message)) = sender.try_send(message()) else {
		panic!("expected a full channel");
	};

	// Drain the channel before the callback records the overflow.
	assert!(receiver.try_recv().is_ok());
	assert!(!overflowed.swap(false, Ordering::SeqCst));
	assert!(receiver.try_recv().is_err());

	// Finish the callback and require another message to observe the overflow.
	State::resend_after_overflow(&overflowed, &sender, message);
	assert!(receiver.try_recv().is_ok());
	assert!(overflowed.swap(false, Ordering::SeqCst));
}

#[test]
fn overflow_while_queue_is_full_does_not_block() {
	// Fill the channel and fail to send another event.
	let (sender, mut receiver) = mpsc::channel(1);
	let overflowed = AtomicBool::new(false);
	assert!(sender.try_send(message()).is_ok());
	let Err(TrySendError::Full(message)) = sender.try_send(message()) else {
		panic!("expected a full channel");
	};

	// Finish the callback while the channel is full and observe the overflow with the queued message.
	State::resend_after_overflow(&overflowed, &sender, message);
	assert!(receiver.try_recv().is_ok());
	assert!(overflowed.swap(false, Ordering::SeqCst));
	assert!(receiver.try_recv().is_err());
}

fn message() -> Message {
	Message {
		event: notify::Event::new(notify::EventKind::Any),
		sender: None,
	}
}
