use super::*;

#[tokio::test]
async fn local_control_failure_does_not_wait_forever() {
	let response = future::err(tg::error!("the control channel failed")).boxed();
	let wait = future::pending().boxed().shared();
	let future = Component::finish_write_process_stdio(response, wait, 0, Duration::from_secs(10));
	let error = tokio::time::timeout(Duration::from_secs(15), future)
		.await
		.expect("completion recovery must be bounded")
		.unwrap_err();
	assert_eq!(error.to_string(), "the control channel failed");
}

#[tokio::test]
async fn local_control_failure_observes_pending_completion() {
	let response = future::err(tg::error!("the control channel failed")).boxed();
	let (sender, receiver) = tokio::sync::oneshot::channel();
	let wait = async move {
		receiver.await.unwrap();
		Ok(())
	}
	.boxed()
	.shared();
	let future = Component::finish_write_process_stdio(response, wait, 0, Duration::from_secs(10));
	let mut future = std::pin::pin!(future);
	assert!(future.as_mut().now_or_never().is_none());
	sender.send(()).unwrap();
	let output = future.await.unwrap();
	assert!(output.closed);
	assert_eq!(output.position, 0);
}
