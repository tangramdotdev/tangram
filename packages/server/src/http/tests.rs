use {super::*, futures::TryStreamExt as _, tokio::io::AsyncReadExt as _};

#[tokio::test]
async fn request_body_error_survives_reading_and_context() {
	let request = http::Request::new(failing_body());
	let mut reader = pin!(request.reader());
	let error = reader.read_to_end(&mut Vec::new()).await.unwrap_err();
	let error = tg::error!(!error, "failed to read the input");
	assert!(is_request_body_error(&error));
	assert_eq!(error.kind(), None);
	assert!(
		error
			.trace()
			.to_string()
			.contains("the client disconnected")
	);
}

#[tokio::test]
async fn request_body_failure_does_not_classify_an_unrelated_error() {
	let request = http::Request::new(failing_body());
	let mut stream =
		frame::decode::<tg::process::stdio::write::Data>(request, frame::Encoding::Tangram, 1024);
	let error = stream.try_next().await.unwrap_err();
	assert!(is_request_body_error(&error));
	let error = tg::error!(internal, "failed to query the database");
	assert!(!is_request_body_error(&error));
}

fn failing_body() -> BoxBody {
	let error = std::io::Error::new(
		std::io::ErrorKind::ConnectionReset,
		"the client disconnected",
	);
	let stream = futures::stream::iter([Err::<bytes::Bytes, _>(error)]);
	let body = BoxBody::with_data_stream(stream);
	BoxBody::new(body.map_err(request_body_error))
}
