use {super::*, serde_json::json};

#[test]
fn json_preserves_binary_node_data() {
	let bytes = Bytes::from_static(&[0, 255, 128, 1]);
	let object = PutNodeObjectMessage {
		bytes: bytes.clone(),
		id: tg::blob::Id::new(&bytes).into(),
		metadata: None,
	};
	let process = PutNodeProcessMessage {
		bytes,
		id: tg::process::Id::new(),
		metadata: None,
	};
	let messages = [
		Message::Put(PutMessage::Node(PutNodeMessage::Object(object))),
		Message::Put(PutMessage::Node(PutNodeMessage::Process(process))),
	];
	for message in messages {
		let json = serde_json::to_value(&message).unwrap();
		assert_eq!(json["value"]["value"]["value"]["bytes"], "AP+AAQ==");
		let decoded: Message = serde_json::from_value(json).unwrap();
		assert_eq!(
			tangram_serialize::to_vec(&decoded).unwrap(),
			tangram_serialize::to_vec(&message).unwrap(),
		);
	}
}

#[test]
fn json_preserves_defaults_and_variants() {
	let id = tg::blob::Id::new(b"test");
	let messages = [
		json!({ "kind": "end" }),
		json!({ "kind": "get", "value": { "kind": "end" } }),
		json!({ "kind": "put", "value": { "kind": "end" } }),
		json!({ "kind": "get", "value": { "kind": "node", "value": { "selector": id.to_string() } } }),
		json!({ "kind": "put", "value": { "kind": "progress", "value": {} } }),
	];
	for json in messages {
		let message: Message = serde_json::from_value(json.clone()).unwrap();
		assert_eq!(serde_json::to_value(&message).unwrap(), json);
		let bytes = tangram_serialize::to_vec(&message).unwrap();
		let decoded: Message = tangram_serialize::from_slice(&bytes).unwrap();
		assert_eq!(serde_json::to_value(decoded).unwrap(), json);
	}
}

#[test]
fn sse_preserves_messages_and_errors() {
	let messages = [
		Message::End,
		Message::Get(GetMessage::End),
		Message::Put(PutMessage::End),
	];
	for message in messages {
		let bytes = tangram_serialize::to_vec(&message).unwrap();
		let event = tangram_http::sse::Event::try_from(message).unwrap();
		let decoded = Message::try_from(event).unwrap();
		assert_eq!(tangram_serialize::to_vec(&decoded).unwrap(), bytes);
	}
	let error = tg::error!("the sync failed");
	let event = tangram_http::sse::Event::try_from(error).unwrap();
	assert!(Message::try_from(event).is_err());
}
