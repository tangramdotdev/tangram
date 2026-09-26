use super::*;

#[tokio::test]
async fn command_sync_drains_the_full_process_window() {
	let (sender, receiver) = mpsc::channel(64);
	let mut input = ReceiverStream::new(receiver).boxed();
	let mut task = Some(Task::spawn(move |_| async move {
		for id in 0..MAX_PENDING as u64 {
			let request = tg::process::connect::ClientRequest {
				arg: tg::process::connect::ClientRequestArg::Close(0),
				id,
			};
			sender
				.send(Ok(tg::process::connect::ClientMessage::Request(request)))
				.await
				.unwrap();
		}
		Ok(())
	}));
	let mut pending = VecDeque::new();
	tokio::time::timeout(
		std::time::Duration::from_secs(1),
		Session::connect_process_await_command_sync(&mut task, &mut input, &mut pending),
	)
	.await
	.unwrap()
	.unwrap();
	// Messages still queued when sync finishes remain available to the spawn path.
	while let Some(message) = input.try_next().await.unwrap() {
		Session::connect_process_buffer_message(&mut pending, message).unwrap();
	}
	assert_eq!(pending.len(), MAX_PENDING);
}

#[tokio::test]
async fn excess_process_traffic_fails_instead_of_blocking_sync() {
	let mut input = stream::iter((0..=MAX_PENDING as u64).map(|id| {
		let request = tg::process::connect::ClientRequest {
			arg: tg::process::connect::ClientRequestArg::Close(0),
			id,
		};
		Ok(tg::process::connect::ClientMessage::Request(request))
	}))
	.boxed();
	let mut task = Some(Task::spawn(|_| {
		futures::future::pending::<tg::Result<()>>()
	}));
	let mut pending = VecDeque::new();
	let error = tokio::time::timeout(
		std::time::Duration::from_secs(1),
		Session::connect_process_await_command_sync(&mut task, &mut input, &mut pending),
	)
	.await
	.unwrap()
	.unwrap_err();
	assert!(error.to_string().contains("request window was exceeded"));
}
