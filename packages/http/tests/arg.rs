use {
	bytes::Bytes,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream},
	std::collections::BTreeMap,
	tangram_http::{body, body::Ext as _, request::Ext as _, request::builder::Ext as _},
};

#[tokio::test]
async fn deeply_nested_args_use_the_body() {
	let mut arg = serde_json::json!("input");
	for _ in 0..8 {
		arg = serde_json::json!({"node": arg});
	}
	let request = http::Request::builder()
		.uri("/processes/control")
		.arg(&arg, body::Bytes::new("payload"))
		.unwrap()
		.unwrap();
	assert!(request.headers().contains_key(body::arg::HEADER));
	assert!(request.uri().query().is_none());
	let (output, request) = request.arg::<serde_json::Value>().await.unwrap();
	assert_eq!(output, Some(arg));
	assert_eq!(request.bytes().await.unwrap(), "payload");
}

#[tokio::test]
async fn small_args_use_the_query_string() {
	let arg = BTreeMap::from([("tokens".to_owned(), vec!["proof".to_owned()])]);
	let request = http::Request::builder()
		.uri("/objects/id")
		.arg(&arg, body::Bytes::new("payload"))
		.unwrap()
		.unwrap();
	assert!(!request.headers().contains_key(body::arg::HEADER));
	assert_eq!(
		request
			.query_params::<BTreeMap<String, Vec<String>>>()
			.unwrap()
			.unwrap(),
		arg
	);
	let (output, request) = request
		.arg::<BTreeMap<String, Vec<String>>>()
		.await
		.unwrap();
	assert_eq!(output, Some(arg));
	assert_eq!(request.bytes().await.unwrap(), "payload");
}

#[tokio::test]
async fn large_args_preserve_the_body_and_retries() {
	let arg = BTreeMap::from([("tokens".to_owned(), vec!["proof".repeat(2000)])]);
	let request = http::Request::builder()
		.uri("/objects/id")
		.header(http::header::CACHE_CONTROL, "public")
		.header(http::header::CONTENT_LENGTH, "7")
		.arg(&arg, body::Bytes::new("payload"))
		.unwrap()
		.unwrap();
	assert!(request.headers().contains_key(body::arg::HEADER));
	assert_eq!(request.headers()[http::header::CACHE_CONTROL], "no-store");
	assert!(!request.headers().contains_key(http::header::CONTENT_LENGTH));
	assert!(request.uri().query().is_none());
	for request in [request.clone(), request] {
		let (output, request) = request
			.arg::<BTreeMap<String, Vec<String>>>()
			.await
			.unwrap();
		assert_eq!(output.as_ref(), Some(&arg));
		assert!(!request.headers().contains_key(body::arg::HEADER));
		assert_eq!(request.bytes().await.unwrap(), "payload");
	}
}

#[tokio::test]
async fn fragmented_args_preserve_trailers() {
	let arg = BTreeMap::from([("tokens".to_owned(), vec!["proof".repeat(2000)])]);
	let request = http::Request::builder()
		.uri("/objects/id")
		.arg(&arg, body::Bytes::new("payload"))
		.unwrap()
		.unwrap();
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
	let (output, request) = request
		.arg::<BTreeMap<String, Vec<String>>>()
		.await
		.unwrap();
	assert_eq!(output, Some(arg));
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
async fn oversized_args_are_rejected_before_reading_the_arg() {
	let length = tangram_util::varint::encode_uvarint(body::arg::MAX_LENGTH + 1);
	let body = body::Boxed::with_data_stream(
		stream::iter([Ok::<_, tangram_http::Error>(Bytes::from(length))]).chain(stream::pending()),
	);
	let request = http::Request::builder()
		.header(body::arg::HEADER, "true")
		.body(body)
		.unwrap();
	let result = request.arg::<serde_json::Value>().now_or_never().unwrap();
	let error = result.err().unwrap();
	assert_eq!(error.to_string(), "arg too large");
}

#[tokio::test]
async fn query_threshold_and_existing_query() {
	for length in [body::arg::THRESHOLD - 6, body::arg::THRESHOLD - 5] {
		let arg = BTreeMap::from([("value".to_owned(), "x".repeat(length))]);
		let request = http::Request::builder()
			.uri("http://example.com/objects/id?old=value")
			.arg(&arg, body::Empty::new())
			.unwrap()
			.unwrap();
		assert_eq!(request.uri().scheme_str(), Some("http"));
		assert_eq!(request.uri().authority().unwrap().as_str(), "example.com");
		assert_eq!(request.uri().path(), "/objects/id");
		assert_eq!(
			request.headers().contains_key(body::arg::HEADER),
			length + 6 > body::arg::THRESHOLD
		);
		let (output, _) = request.arg::<BTreeMap<String, String>>().await.unwrap();
		assert_eq!(output, Some(arg));
	}
}

#[tokio::test]
async fn missing_args_do_not_read_the_body() {
	let body =
		body::Boxed::with_data_stream(stream::pending::<Result<Bytes, tangram_http::Error>>());
	let request = http::Request::builder()
		.uri("/objects/id")
		.body(body)
		.unwrap();
	let (output, _) = request
		.arg::<BTreeMap<String, String>>()
		.now_or_never()
		.unwrap()
		.unwrap();
	assert!(output.is_none());
}

#[tokio::test]
async fn empty_args_clear_the_existing_query() {
	#[derive(serde::Deserialize, serde::Serialize)]
	struct Arg {}
	let arg = Arg {};
	for force in [false, true] {
		let mut builder = http::Request::builder().uri("/objects/id?old=value");
		if force {
			builder = builder.header(body::arg::HEADER, "true");
		}
		let request = builder
			.arg(&arg, body::Bytes::new("payload"))
			.unwrap()
			.unwrap();
		assert!(request.uri().query().is_none());
		assert_eq!(request.headers().contains_key(body::arg::HEADER), force);
		let (output, request) = request.arg::<Arg>().await.unwrap();
		assert_eq!(output.is_some(), force);
		assert_eq!(request.bytes().await.unwrap(), "payload");
	}
}

#[tokio::test]
async fn large_args_deserialize_directly_into_the_requested_type() {
	#[derive(Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
	struct Arg {
		padding: String,
		value: u128,
	}
	let arg = Arg {
		padding: "x".repeat(body::arg::THRESHOLD),
		value: u128::MAX,
	};
	// Query serialization does not support u128, so construct the framed body directly.
	let body = body::arg::Body::with_arg(body::Empty::new(), &arg).unwrap();
	let request = http::Request::builder()
		.uri("/objects/id")
		.header(body::arg::HEADER, "true")
		.body(body)
		.unwrap();
	let (output, _) = request.arg::<Arg>().await.unwrap();
	assert_eq!(output, Some(arg));
}

#[tokio::test]
async fn malformed_args_are_rejected() {
	for (header, bytes) in [
		("false", b"".as_slice()),
		("true", b"\x80".as_slice()),
		("true", b"\x04{}".as_slice()),
		("true", b"\x01{".as_slice()),
		("true", b"\x01\x00".as_slice()),
	] {
		let request = http::Request::builder()
			.header(body::arg::HEADER, header)
			.bytes(Bytes::copy_from_slice(bytes))
			.unwrap();
		assert!(request.arg::<BTreeMap<String, String>>().await.is_err());
	}
}

#[test]
fn invalid_builders_return_the_http_error() {
	for length in [1, body::arg::THRESHOLD] {
		let arg = BTreeMap::from([("value", "x".repeat(length))]);
		let result = http::Request::builder()
			.header("invalid\nheader", "value")
			.arg(&arg, body::Empty::new())
			.unwrap();
		assert!(result.is_err());
	}
}

#[tokio::test]
async fn prefixed_args_follow_the_content_type() {
	let arg = BTreeMap::from([("tokens".to_owned(), vec!["token".to_owned()])]);
	for (content_type, encoding) in [
		("application/json", body::encoding::Encoding::Json),
		("text/event-stream", body::encoding::Encoding::Json),
		("application/octet-stream", body::encoding::Encoding::Json),
		(
			"application/vnd.tangram.process-connect",
			body::encoding::Encoding::Tangram,
		),
		(
			"application/vnd.tangram.sync; version=1",
			body::encoding::Encoding::Tangram,
		),
	] {
		let request = http::Request::builder()
			.uri("/sync")
			.header(http::header::CONTENT_TYPE, content_type)
			.header(body::arg::HEADER, "true")
			.arg_with_tangram(&arg, body::Bytes::new("payload"))
			.unwrap()
			.unwrap();
		let bytes = request
			.clone()
			.into_body()
			.collect()
			.await
			.unwrap()
			.to_bytes();
		let prefix = body::arg::get(&mut bytes.as_ref(), body::arg::MAX_LENGTH)
			.await
			.unwrap();
		assert_eq!(prefix, encoding.serialize(&arg).unwrap());
		let (output, request) = request
			.arg_with_tangram::<BTreeMap<String, Vec<String>>>()
			.await
			.unwrap();
		assert_eq!(output, Some(arg.clone()));
		assert_eq!(request.bytes().await.unwrap(), "payload");
	}
}

#[tokio::test]
async fn json_only_args_reject_native_body_prefixes() {
	let arg = serde_json::json!({"value": "input"});
	assert!(
		http::Request::builder()
			.uri("/sync")
			.header(http::header::CONTENT_TYPE, "application/vnd.tangram.sync")
			.header(body::arg::HEADER, "true")
			.arg(&arg, body::Empty::new())
			.is_err()
	);
	let request = http::Request::builder()
		.uri("/sync")
		.header(http::header::CONTENT_TYPE, "application/vnd.tangram.sync")
		.header(body::arg::HEADER, "true")
		.body(body::arg::Body::with_arg(body::Empty::new(), &arg).unwrap())
		.unwrap();
	assert!(request.arg::<serde_json::Value>().await.is_err());
}
