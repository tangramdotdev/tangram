use super::*;

#[tokio::test]
async fn send_request_enqueues_before_waiting_and_preserves_response_errors() {
	let (local, mut receiver) = Local::new();
	let arg =
		tg::sandbox::control::ServerRequestArg::Get(tg::sandbox::control::GetServerRequestArg {});
	let response = local.send_request(arg).await.unwrap();
	let message = receiver.try_recv().unwrap();
	message
		.sender
		.send(Err(tg::error!("the request failed")))
		.await
		.unwrap();
	assert!(response.await.unwrap().is_err());
}

#[tokio::test]
async fn send_request_distinguishes_transport_errors_and_abandoned_callers() {
	let (local, mut receiver) = Local::new();
	let arg =
		tg::sandbox::control::ServerRequestArg::Get(tg::sandbox::control::GetServerRequestArg {});
	let response = local.send_request(arg.clone()).await.unwrap();
	drop(receiver.try_recv().unwrap());
	assert!(response.await.is_err());
	let response = local.send_request(arg.clone()).await.unwrap();
	drop(response);
	receiver
		.try_recv()
		.unwrap()
		.sender
		.send(Err(tg::error!("the request failed")))
		.await
		.unwrap();
	drop(receiver);
	assert!(local.send_request(arg).await.is_err());
}
