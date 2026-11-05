use {super::Server, std::time::Duration, tokio::sync::oneshot};

#[tokio::test]
async fn cancelling_startup_cleans_up_after_mounting_finishes() {
	let (started_sender, started_receiver) = oneshot::channel();
	let (mounted_sender, mounted_receiver) = oneshot::channel();
	let (cleaned_sender, cleaned_receiver) = oneshot::channel();
	let start = tokio::spawn(Server::start_inner(async move {
		started_sender.send(()).unwrap();
		mounted_receiver.await.unwrap();
		Ok(async move {
			cleaned_sender.send(()).unwrap();
		})
	}));

	started_receiver.await.unwrap();
	start.abort();
	assert!(start.await.is_err_and(|error| error.is_cancelled()));
	mounted_sender.send(()).unwrap();
	tokio::time::timeout(Duration::from_secs(5), cleaned_receiver)
		.await
		.unwrap()
		.unwrap();
}

#[tokio::test]
async fn dropping_a_mounted_server_starts_cleanup() {
	let (cleaned_sender, cleaned_receiver) = oneshot::channel();
	let server = Server::start_inner(async move {
		Ok(async move {
			cleaned_sender.send(()).unwrap();
		})
	})
	.await
	.unwrap();

	drop(server);
	tokio::time::timeout(Duration::from_secs(5), cleaned_receiver)
		.await
		.unwrap()
		.unwrap();
}

#[tokio::test]
async fn cancelling_wait_does_not_cancel_cleanup() {
	let (started_sender, started_receiver) = oneshot::channel();
	let (finish_sender, finish_receiver) = oneshot::channel();
	let (cleaned_sender, cleaned_receiver) = oneshot::channel();
	let server = Server::start_inner(async move {
		Ok(async move {
			started_sender.send(()).unwrap();
			finish_receiver.await.unwrap();
			cleaned_sender.send(()).unwrap();
		})
	})
	.await
	.unwrap();

	let wait = tokio::spawn(server.wait());
	started_receiver.await.unwrap();
	wait.abort();
	assert!(wait.await.is_err_and(|error| error.is_cancelled()));
	finish_sender.send(()).unwrap();
	tokio::time::timeout(Duration::from_secs(5), cleaned_receiver)
		.await
		.unwrap()
		.unwrap();
}
