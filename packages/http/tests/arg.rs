use {
	bytes::Bytes,
	futures::{TryStreamExt as _, stream},
	std::collections::BTreeMap,
	tangram_http::{body, body::Ext as _, request, request::Ext as _, request::builder::Ext as _},
};

#[tokio::test]
async fn small_args_use_the_query_string() {
	let arg = BTreeMap::from([("tokens".to_owned(), vec!["proof".to_owned()])]);
	let request = http::Request::builder()
		.uri("/objects/id")
		.bytes("payload")
		.unwrap();
	let request = request::with_query_params(request, &arg).unwrap();
	assert!(!request.headers().contains_key(body::arg::HEADER));
	assert_eq!(
		request
			.query_params::<BTreeMap<String, Vec<String>>>()
			.unwrap()
			.unwrap(),
		arg
	);
	assert_eq!(request.bytes().await.unwrap(), "payload");
}

#[tokio::test]
async fn large_args_preserve_the_body_and_retries() {
	let arg = BTreeMap::from([("tokens".to_owned(), vec!["proof".repeat(2000)])]);
	let request = http::Request::builder()
		.uri("/objects/id")
		.bytes("payload")
		.unwrap();
	let request = request::with_query_params(request, &arg).unwrap();
	assert!(request.headers().contains_key(body::arg::HEADER));
	assert_eq!(request.headers()[http::header::CACHE_CONTROL], "no-store");
	assert!(!request.headers().contains_key(http::header::CONTENT_LENGTH));
	assert!(request.uri().query().is_none());
	for request in [request.clone(), request] {
		let request = request::read_query_params(request.boxed_body(), 20000)
			.await
			.unwrap();
		assert_eq!(
			request
				.query_params::<BTreeMap<String, Vec<String>>>()
				.unwrap()
				.unwrap(),
			arg
		);
		assert_eq!(request.bytes().await.unwrap(), "payload");
	}
}

#[tokio::test]
async fn fragmented_args_preserve_trailers() {
	let arg = BTreeMap::from([("tokens".to_owned(), vec!["proof".repeat(2000)])]);
	let request = http::Request::builder()
		.uri("/objects/id")
		.bytes("payload")
		.unwrap();
	let request = request::with_query_params(request, &arg).unwrap();
	let (parts, body) = request.into_parts();
	let bytes = body.collect().await.unwrap().to_bytes();
	let mut frames = bytes
		.chunks(7)
		.map(|chunk| {
			Ok::<_, tangram_http::Error>(http_body::Frame::data(Bytes::copy_from_slice(chunk)))
		})
		.collect::<Vec<_>>();
	let mut trailers = http::HeaderMap::new();
	trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
	frames.push(Ok(http_body::Frame::trailers(trailers.clone())));
	let body = body::Boxed::with_stream(stream::iter(frames));
	let request = http::Request::from_parts(parts, body);
	let request = request::read_query_params(request, 20000).await.unwrap();
	assert_eq!(
		request
			.query_params::<BTreeMap<String, Vec<String>>>()
			.unwrap()
			.unwrap(),
		arg
	);
	let frames = request
		.into_body()
		.into_stream()
		.try_collect::<Vec<_>>()
		.await
		.unwrap();
	let mut bytes = Vec::new();
	for frame in &frames {
		if let Some(data) = frame.data_ref() {
			bytes.extend_from_slice(data);
		}
	}
	assert_eq!(bytes, b"payload");
	assert_eq!(frames.last().unwrap().trailers_ref(), Some(&trailers));
}

#[tokio::test]
async fn oversized_args_are_rejected() {
	let arg = BTreeMap::from([("tokens".to_owned(), vec!["proof".repeat(2000)])]);
	let request = http::Request::builder().uri("/objects/id").empty().unwrap();
	let request = request::with_query_params(request, &arg).unwrap();
	assert!(
		request::read_query_params(request.boxed_body(), 64)
			.await
			.is_err()
	);
}
