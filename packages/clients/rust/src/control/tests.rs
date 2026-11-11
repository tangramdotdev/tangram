use {
	super::{Response, Retry},
	crate::prelude::*,
	std::ops::ControlFlow,
};

#[test]
fn a_response_decodes_to_its_output_retry_or_error() {
	let response = Response::<u64, Retry>::with_output("a".to_owned(), 1);
	assert!(matches!(response.into_result(), Ok(ControlFlow::Break(1))));
	let response = Response::<u64, Retry>::with_retry("a".to_owned(), Retry::NotReady);
	assert!(matches!(
		response.into_result(),
		Ok(ControlFlow::Continue(Retry::NotReady))
	));
	let error = tg::error::Data {
		message: Some("failed".to_owned()),
		..Default::default()
	};
	let response = Response::<u64, Retry>::with_error("a".to_owned(), error.clone());
	let error = response.into_result().unwrap_err();
	assert_eq!(error.to_string(), "failed");

	// A response that sets none, or more than one, is malformed.
	let response = Response::<u64, Retry> {
		error: None,
		id: "a".to_owned(),
		output: None,
		retry: None,
	};
	assert!(response.into_result().is_err());
	let response = Response::<u64, Retry> {
		error: None,
		id: "a".to_owned(),
		output: Some(1),
		retry: Some(Retry::NotHolder),
	};
	assert!(response.into_result().is_err());
}

#[test]
fn a_response_round_trips_in_both_formats() {
	let responses = [
		Response::<u64, Retry>::with_output("a".to_owned(), 1),
		Response::<u64, Retry>::with_retry("b".to_owned(), Retry::NotHolder),
		Response::<u64, Retry>::with_retry("c".to_owned(), Retry::NotReady),
	];
	for response in responses {
		let bytes = tangram_serialize::to_vec(&response).unwrap();
		let decoded: Response<u64, Retry> = tangram_serialize::from_slice(&bytes).unwrap();
		assert_eq!(decoded.id, response.id);
		assert_eq!(decoded.output, response.output);
		assert_eq!(decoded.retry, response.retry);
		let json = serde_json::to_string(&response).unwrap();
		let decoded: Response<u64, Retry> = serde_json::from_str(&json).unwrap();
		assert_eq!(decoded.id, response.id);
		assert_eq!(decoded.output, response.output);
		assert_eq!(decoded.retry, response.retry);
	}
	let json = serde_json::to_string(&Response::<u64, Retry>::with_retry(
		"b".to_owned(),
		Retry::NotHolder,
	))
	.unwrap();
	assert_eq!(json, r#"{"id":"b","retry":"not_holder"}"#);
}
