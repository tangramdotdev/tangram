use {super::*, serde_json::json};

#[test]
fn webhook_authentication() {
	let billing = billing();
	let body = br#"{"id":"evt_1","type":"customer.updated","data":{"object":{"id":"cus_1"}}}"#;
	let headers = headers(body, 1000);
	assert!(billing.try_parse_webhook(&headers, body, 1000).is_ok());
	assert!(billing.try_parse_webhook(&headers, body, 1301).is_err());
	assert!(billing.try_parse_webhook(&headers, body, 699).is_err());
	assert!(
		billing
			.try_parse_webhook(&http::HeaderMap::new(), body, 1000)
			.is_err()
	);
	let mut tampered = body.to_vec();
	tampered.push(b' ');
	assert!(
		billing
			.try_parse_webhook(&headers, &tampered, 1000)
			.is_err()
	);
}

#[test]
fn webhook_events() {
	let billing = billing();
	for (kind, object, previous_attributes, customer) in [
		(
			"customer.updated",
			json!({"id": "cus_1"}),
			json!({}),
			Some("cus_1"),
		),
		(
			"payment_method.attached",
			json!({"customer": "cus_2"}),
			json!({}),
			Some("cus_2"),
		),
		(
			"payment_method.detached",
			json!({"customer": null}),
			json!({"customer": "cus_3"}),
			Some("cus_3"),
		),
		("payment_method.detached", json!({}), json!({}), None),
	] {
		let body = serde_json::to_vec(&json!({
			"data": {"object": object, "previous_attributes": previous_attributes},
			"id": "evt_1",
			"type": kind,
		}))
		.unwrap();
		let event = billing
			.try_parse_webhook(&headers(&body, 1000), &body, 1000)
			.unwrap()
			.unwrap();
		assert_eq!(event.id, "evt_1");
		assert_eq!(event.customer.as_deref(), customer);
	}
	let body = br#"{"id":"evt_2","type":"customer.deleted","data":{"object":{"id":"cus_1"}}}"#;
	assert!(
		billing
			.try_parse_webhook(&headers(body, 1000), body, 1000)
			.unwrap()
			.is_none()
	);
	let body = b"invalid json";
	assert!(
		billing
			.try_parse_webhook(&headers(body, 1000), body, 1000)
			.is_err()
	);
}

fn billing() -> Billing {
	let config = Config {
		secret_key: "sk_test".into(),
		url: "https://example.invalid".parse().unwrap(),
		webhook_secret: "whsec_test".into(),
	};
	Billing::new(&config)
}

fn headers(body: &[u8], timestamp: i64) -> http::HeaderMap {
	let mut payload = format!("{timestamp}.").into_bytes();
	payload.extend_from_slice(body);
	let key = hmac::Key::new(hmac::HMAC_SHA256, b"whsec_test");
	let signature = HEXLOWER.encode(hmac::sign(&key, &payload).as_ref());
	let mut headers = http::HeaderMap::new();
	headers.insert(
		"stripe-signature",
		format!("t={timestamp},v1={signature}").parse().unwrap(),
	);
	headers
}
