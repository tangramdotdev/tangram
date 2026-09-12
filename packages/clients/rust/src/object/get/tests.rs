use {super::Output, crate::prelude::*, bytes::Bytes, std::collections::BTreeMap};

#[test]
fn binary_round_trip_preserves_buffers() {
	let bytes = Bytes::from(vec![42; 1024 * 1024]);
	let output = output(bytes.clone());
	let expected = output.clone();
	let [header, payload] = output.serialize().unwrap();
	assert_eq!(payload.as_ptr(), bytes.as_ptr());
	assert_eq!(payload, bytes);

	let response = Bytes::from([header.as_ref(), payload.as_ref()].concat());
	let payload = response.slice(header.len()..);
	let output = Output::deserialize(response).unwrap();
	assert_eq!(output.bytes.as_ptr(), payload.as_ptr());
	assert_eq!(output.bytes, expected.bytes);
	assert_eq!(output.availability, expected.availability);
	assert_eq!(output.metadata, expected.metadata);
	assert_eq!(output.tokens, expected.tokens);
	assert_eq!(output.children.len(), expected.children.len());
	for (id, child) in expected.children {
		assert_eq!(output.children[&id].tokens, child.tokens);
	}
}

#[test]
fn binary_round_trip_accepts_empty_payload() {
	let [header, payload] = output(Bytes::new()).serialize().unwrap();
	assert!(payload.is_empty());
	let output = Output::deserialize(header).unwrap();
	assert!(output.bytes.is_empty());
}

#[test]
fn binary_rejects_incomplete_headers_and_incorrect_payload_sizes() {
	let [header, payload] = output(Bytes::from_static(b"object")).serialize().unwrap();
	for len in 0..header.len() {
		assert!(Output::deserialize(header.slice(..len)).is_err());
	}
	let response = Bytes::from([header.as_ref(), payload.as_ref()].concat());
	assert!(Output::deserialize(response.slice(..response.len() - 1)).is_err());
	let response = Bytes::from([response.as_ref(), &[0]].concat());
	assert!(Output::deserialize(response).is_err());
}

fn output(bytes: Bytes) -> Output {
	let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
	let key =
		tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
			.unwrap();
	let body = tg::authorization::Body {
		expires_at: 100,
		permissions: vec![tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		)],
		resource: id.clone().into(),
	};
	let token = tg::authorization::Token::sign(body, &key).unwrap();
	let tokens = tg::Tokens::with_authorization(Some(token));
	let child = tg::object::get::Child {
		tokens: tokens.clone(),
	};
	let children = BTreeMap::from([(id, child)]);
	let availability = tg::object::Availability { subtree: true };
	Output {
		availability: Some(availability),
		bytes,
		children,
		metadata: Some(tg::object::Metadata::default()),
		tokens,
	}
}
