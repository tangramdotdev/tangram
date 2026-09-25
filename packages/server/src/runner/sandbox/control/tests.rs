use {
	super::*, crate::sandbox::control::local::Local, futures::FutureExt as _,
	tokio_stream::wrappers::ReceiverStream,
};

#[tokio::test]
async fn local_requests_do_not_cancel_remote_acknowledgments() {
	let (input, receiver) = tokio::sync::mpsc::channel(1);
	let (output, mut responses) = tokio::sync::mpsc::channel(1);
	let ack = tg::sandbox::control::ClientAck {
		id: "occupied".into(),
	};
	output
		.send(tg::sandbox::control::ClientMessage::Ack(ack))
		.await
		.unwrap();
	let stream = crate::control::Stream::new(
		ReceiverStream::new(receiver).boxed(),
		output,
		crate::control::stream_options(),
	);
	let (local, receiver) = Local::new();
	let mut control = Control::new(stream, receiver);
	let request = tg::sandbox::control::ServerRequest {
		arg: tg::sandbox::control::ServerRequestArg::Get(
			tg::sandbox::control::GetServerRequestArg {},
		),
		id: "remote".into(),
	};
	input
		.send(Ok(tg::sandbox::control::ServerMessage::Request(request)))
		.await
		.unwrap();
	assert!(control.recv().now_or_never().is_none());

	// A local request completes while the remote request waits to send its receipt.
	let arg = tg::sandbox::control::ServerRequestArg::Destroy(
		tg::sandbox::control::DestroyServerRequestArg { error: None },
	);
	let mut response = local.request(arg).boxed();
	assert!(response.as_mut().now_or_never().is_none());
	let message = control.recv().await.unwrap().unwrap();
	assert!(matches!(
		message.arg,
		tg::sandbox::control::ServerRequestArg::Destroy(_)
	));
	let output = tg::sandbox::control::DestroyClientResponseOutput { destroyed: false };
	message
		.sender
		.send(Ok(tg::sandbox::control::ClientResponseOutput::Destroy(
			output,
		)))
		.await
		.unwrap();
	assert!(
		!response
			.await
			.unwrap()
			.try_unwrap_destroy()
			.unwrap()
			.destroyed
	);

	// Resuming the remote transport must deliver the request that was already consumed.
	responses.recv().await.unwrap();
	let message = control.recv().await.unwrap().unwrap();
	assert!(matches!(
		message.arg,
		tg::sandbox::control::ServerRequestArg::Get(_)
	));
	assert!(
		matches!(responses.recv().await, Some(tg::sandbox::control::ClientMessage::Ack(ack)) if ack.id == "remote")
	);
	message
		.sender
		.send(Err(tg::error!("test error")))
		.await
		.unwrap();
	assert!(
		matches!(responses.recv().await, Some(tg::sandbox::control::ClientMessage::Response(response)) if response.id == "remote" && response.error.is_some())
	);
}

#[tokio::test]
async fn local_requests_survive_remote_end_and_abandoned_callers() {
	let (output, _responses) = tokio::sync::mpsc::channel(1);
	let stream = crate::control::Stream::new(
		futures::stream::empty().boxed(),
		output,
		crate::control::stream_options(),
	);
	let (local, receiver) = Local::new();
	let mut control = Control::new(stream, receiver);
	assert!(control.recv().await.unwrap().is_none());
	let arg = tg::sandbox::control::ServerRequestArg::Destroy(
		tg::sandbox::control::DestroyServerRequestArg { error: None },
	);
	let mut response = local.request(arg).boxed();
	assert!(response.as_mut().now_or_never().is_none());
	let message = control.recv().await.unwrap().unwrap();
	drop(response);
	message
		.sender
		.send(Err(tg::error!("test error")))
		.await
		.unwrap();
	assert!(!local.is_closed());
	drop(control);
	assert!(local.is_closed());
}
