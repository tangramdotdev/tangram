use {
	futures::stream,
	tangram_http::body::{self, Ext as _, encoding::Encoding},
	tokio::io::AsyncReadExt as _,
};

#[derive(
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
struct Header {
	#[tangram_serialize(id = 0)]
	value: String,
}

#[tokio::test]
async fn encodings_preserve_the_header_and_following_body() {
	let header = Header {
		value: "value".repeat(100),
	};
	for encoding in [Encoding::Json, Encoding::Tangram] {
		let body = body::Boxed::new(body::Bytes::new("payload"));
		let body = body::header::set(body, &header, encoding).unwrap();
		let bytes = body.collect().await.unwrap().to_bytes();
		let payload = encoding.serialize(&header).unwrap();
		assert!(bytes.ends_with(&[payload.as_slice(), b"payload"].concat()));
		let frames = bytes
			.iter()
			.map(|byte| Ok::<_, std::io::Error>(bytes::Bytes::from(vec![*byte])))
			.collect::<Vec<_>>();
		let mut reader = tokio_util::io::StreamReader::new(stream::iter(frames));
		let output: Header = body::header::get(&mut reader, body::header::MAX_LENGTH, encoding)
			.await
			.unwrap();
		assert_eq!(output, header);
		let mut remaining = String::new();
		reader.read_to_string(&mut remaining).await.unwrap();
		assert_eq!(remaining, "payload");
	}
}

#[tokio::test]
async fn wrong_encoding_and_truncated_headers_fail() {
	let header = Header {
		value: "value".into(),
	};
	for encoding in [Encoding::Json, Encoding::Tangram] {
		let body = body::Boxed::new(body::Empty::new());
		let bytes = body::header::set(body, &header, encoding)
			.unwrap()
			.collect()
			.await
			.unwrap()
			.to_bytes();
		let other = match encoding {
			Encoding::Json => Encoding::Tangram,
			Encoding::Tangram => Encoding::Json,
		};
		assert!(
			body::header::get::<Header, _>(&mut bytes.as_ref(), body::header::MAX_LENGTH, other)
				.await
				.is_err()
		);
		assert!(
			body::header::get::<Header, _>(
				&mut &bytes[..bytes.len() - 1],
				body::header::MAX_LENGTH,
				encoding
			)
			.await
			.is_err()
		);
		assert!(
			body::header::get::<Header, _>(&mut bytes.as_ref(), 1, encoding)
				.await
				.is_err()
		);
	}
}
