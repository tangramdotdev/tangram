use super::*;

#[tokio::test]
async fn decode_tangram_preserves_error_trailers() {
	let stream =
		futures::stream::iter([Ok(7_u64), Err(tg::error!("the test stream failed"))]).boxed();
	let body = encode_tangram(stream, 1024);
	let request = http::Request::new(body);
	let mut stream = decode_tangram::<u64>(request, 1024);

	assert_eq!(stream.try_next().await.unwrap(), Some(7));
	assert!(stream.try_next().await.is_err());
}
