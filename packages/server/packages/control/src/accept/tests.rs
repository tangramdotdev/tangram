use {super::*, futures::future};

#[tokio::test]
async fn the_first_success_accepts() {
	let handled = future::pending();
	let queued = async { Ok(()) };
	let result = accept(queued, handled).await;
	assert!(result.is_ok());
}

#[tokio::test]
async fn a_failure_waits_for_the_other_path() {
	let handled = async {
		tokio::task::yield_now().await;
		Ok(())
	};
	let queued = async { Err(tg::error!("failed to write the queue")) };
	let result = accept(queued, handled).await;
	assert!(result.is_ok());
}

#[tokio::test]
async fn both_failures_fail() {
	let handled = async { Err(tg::error!("failed to handle the request")) };
	let queued = async { Err(tg::error!("failed to write the queue")) };
	let result = accept(queued, handled).await;
	assert!(result.is_err());
}

#[test]
fn the_acceptance_is_decided_once() {
	let mut accept = Accept::default();
	assert!(accept.finish(Path::Handled, Ok(())).unwrap().is_ok());
	assert!(accept.finish(Path::Queued, Ok(())).is_none());
}
