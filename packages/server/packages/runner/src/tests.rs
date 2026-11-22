use super::*;

#[test]
fn a_report_waits_for_a_sender_other_than_the_lost_one() {
	// A sender that replaced the lost sender before the wait began is not missed.
	let (sender, mut receiver) = tokio::sync::watch::channel(Some(1));
	sender.send_replace(Some(2));
	receiver.borrow_and_update();
	let mut wait = pin!(wait_for_other_sender(&mut receiver, |current| *current == 1));
	assert!(poll(&mut wait).is_ready());

	// The lost sender, or no sender, is waited past.
	let (sender, mut receiver) = tokio::sync::watch::channel(Some(1));
	let mut wait = pin!(wait_for_other_sender(&mut receiver, |current| *current == 1));
	assert!(poll(&mut wait).is_pending());
	sender.send_replace(None);
	assert!(poll(&mut wait).is_pending());
	sender.send_replace(Some(2));
	assert!(poll(&mut wait).is_ready());
}

fn poll<F: Future + Unpin>(future: &mut F) -> std::task::Poll<F::Output> {
	let waker = std::task::Waker::noop();
	let mut context = std::task::Context::from_waker(waker);
	std::pin::Pin::new(future).poll(&mut context)
}
