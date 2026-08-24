use {
	bytes::Bytes,
	futures::{channel::mpsc, stream},
	http_body::Frame,
	http_body_util::{BodyExt as _, StreamBody},
	std::convert::Infallible,
	tangram_http::body::Coalesce,
};

#[tokio::test]
async fn coalesces_ready_data_frames() {
	let frames = ["a", "b", "c", "d", "e"]
		.map(|value| Ok::<_, Infallible>(Frame::data(Bytes::from_static(value.as_bytes()))));
	let mut body = Coalesce::with_target_size(StreamBody::new(stream::iter(frames)), 4);

	let frame = body.frame().await.unwrap().unwrap();
	assert_eq!(frame.into_data().unwrap(), "abcd");
	let frame = body.frame().await.unwrap().unwrap();
	assert_eq!(frame.into_data().unwrap(), "e");
	assert!(body.frame().await.is_none());
}

#[tokio::test]
async fn flushes_data_when_the_stream_is_pending() {
	let (mut sender, receiver) = mpsc::channel(1);
	sender
		.try_send(Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"a"))))
		.unwrap();
	let mut body = Coalesce::with_target_size(StreamBody::new(receiver), 4);

	let frame = body.frame().await.unwrap().unwrap();
	assert_eq!(frame.into_data().unwrap(), "a");
}

#[tokio::test]
async fn preserves_trailers_after_buffered_data() {
	let mut trailers = http::HeaderMap::new();
	trailers.insert("x-test", http::HeaderValue::from_static("value"));
	let frames = [
		Frame::data(Bytes::from_static(b"a")),
		Frame::trailers(trailers),
	]
	.map(Ok::<_, Infallible>);
	let mut body = Coalesce::with_target_size(StreamBody::new(stream::iter(frames)), 4);

	let frame = body.frame().await.unwrap().unwrap();
	assert_eq!(frame.into_data().unwrap(), "a");
	let frame = body.frame().await.unwrap().unwrap();
	assert_eq!(frame.into_trailers().unwrap()["x-test"], "value");
	assert!(body.frame().await.is_none());
}
