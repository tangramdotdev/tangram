use {super::*, futures::TryStreamExt as _};

#[tokio::test]
async fn decode_with_header_preserves_error_trailers() {
	let stream =
		futures::stream::iter([Ok(7_u64), Err(tg::error!("the test stream failed"))]).boxed();
	let body = encode(stream, 1024);
	let body = tangram_http::body::header::set(
		body,
		&42_u64,
		tangram_http::body::encoding::Encoding::Tangram,
	)
	.unwrap();
	let (header, mut stream) = decode_with_header::<u64, u64>(body, 1_048_576, 1024)
		.await
		.unwrap()
		.unwrap();

	assert_eq!(header, 42);
	assert_eq!(stream.try_next().await.unwrap(), Some(7));
	assert!(stream.try_next().await.is_err());
}

#[tokio::test]
async fn decode_transport_failure_ends_the_attempt() {
	for tail in [
		Bytes::new(),
		Bytes::from_static(&[0x80]),
		Bytes::from_static(&[0x10, 0]),
	] {
		let bytes = tangram_serialize::to_vec(&7_u64).unwrap();
		let mut frame = vec![u8::try_from(bytes.len()).unwrap()];
		frame.extend(bytes);
		let error = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
		let stream = futures::stream::iter([Ok(Bytes::from(frame)), Ok(tail), Err(error)]);
		let body = Boxed::with_data_stream(stream);
		let mut stream = decode::<u64>(body, 1024);

		assert_eq!(stream.try_next().await.unwrap(), Some(7));
		assert_eq!(stream.try_next().await.unwrap(), None);
	}
}

#[tokio::test]
async fn decode_with_header_ends_before_the_header() {
	let error = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
	let stream = futures::stream::iter([Ok(Bytes::from_static(&[0x10])), Err(error)]);
	let body = Boxed::with_data_stream(stream);
	let output = decode_with_header::<u64, u64>(body, 1_048_576, 1024)
		.await
		.unwrap();

	assert!(output.is_none());
}
