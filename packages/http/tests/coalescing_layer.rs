use {
	bytes::Bytes,
	futures::stream,
	http_body_util::BodyExt as _,
	std::convert::Infallible,
	tangram_http::{
		body::Boxed,
		layer::coalescing::{RequestCoalescingLayer, ResponseCoalescingLayer},
	},
	tower::{Layer as _, ServiceExt as _},
};

#[tokio::test]
async fn coalesces_request_bodies() {
	let service = tower::service_fn(|request: http::Request<Boxed>| async move {
		let mut body = request.into_body();
		assert_frame_lengths(&mut body).await;
		Ok::<_, Infallible>(http::Response::new(Boxed::empty()))
	});
	let service = RequestCoalescingLayer::new(16 * 1024).layer(service);
	let request = http::Request::new(body());

	service.oneshot(request).await.unwrap();
}

#[tokio::test]
async fn coalesces_response_bodies() {
	let service = tower::service_fn(|_: http::Request<Boxed>| async move {
		Ok::<_, Infallible>(http::Response::new(body()))
	});
	let service = ResponseCoalescingLayer::new(16 * 1024).layer(service);
	let request = http::Request::new(Boxed::empty());

	let response = service.oneshot(request).await.unwrap();
	let mut body = response.into_body();
	assert_frame_lengths(&mut body).await;
}

fn body() -> Boxed {
	let chunks = (0..16_385).map(|_| Ok::<_, Infallible>(Bytes::from_static(b"a")));
	Boxed::with_data_stream(stream::iter(chunks))
}

async fn assert_frame_lengths(body: &mut Boxed) {
	let frame = body.frame().await.unwrap().unwrap();
	assert_eq!(frame.into_data().unwrap().len(), 16 * 1024);
	let frame = body.frame().await.unwrap().unwrap();
	assert_eq!(frame.into_data().unwrap().len(), 1);
	assert!(body.frame().await.is_none());
}
