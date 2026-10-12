use {super::*, futures::StreamExt as _, serde::Deserialize};

#[derive(Deserialize)]
struct Case {
	events: Vec<serde_json::Value>,
	input: String,
}

#[tokio::test]
async fn shared_decoding_cases() {
	let cases: Vec<Case> = serde_json::from_str(include_str!("../../fixtures/sse.json")).unwrap();
	for case in cases {
		for capacity in [1, 2, 128] {
			let reader = tokio::io::BufReader::with_capacity(
				capacity,
				std::io::Cursor::new(case.input.as_bytes().to_vec()),
			);
			let events = decode(reader)
				.map(|event| {
					let event = event.unwrap();
					let mut value = serde_json::json!({"data": event.data});
					if let Some(name) = event.event {
						value["event"] = name.into();
					}
					if let Some(id) = event.id {
						value["id"] = id.into();
					}
					if let Some(retry) = event.retry {
						value["retry"] = retry.into();
					}
					value
				})
				.collect::<Vec<_>>()
				.await;
			assert_eq!(events, case.events, "{}", case.input);
		}
	}
}
