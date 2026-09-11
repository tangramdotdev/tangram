use {
	super::*,
	std::{collections::BTreeMap, io::SeekFrom},
};

#[test]
fn opening_metadata_preserves_read_options() {
	let read = tg::process::stdio::read::Arg {
		length: Some(-7),
		position: Some(SeekFrom::End(-3)),
		streams: vec![tg::process::stdio::Stream::Stdout],
		..Default::default()
	};
	let arg = Arg {
		reads: BTreeMap::from([(1, read)]),
		target: Target::Existing {
			id: tg::process::Id::new(),
			options: tg::process::wait::Arg::default().into(),
		},
	};
	let request = ClientRequest {
		arg: ClientRequestArg::Connect(arg),
		id: 0,
	};
	let message = ClientMessage::Request(request);
	let bytes = tangram_serialize::to_vec(&message).unwrap();
	let decoded: ClientMessage = tangram_serialize::from_slice(&bytes).unwrap();
	assert_eq!(
		serde_json::to_value(message).unwrap(),
		serde_json::to_value(decoded).unwrap()
	);
}

#[test]
fn stdio_keeps_binary_bytes_and_eof_positions() {
	let chunk = tg::process::stdio::Chunk {
		bytes: bytes::Bytes::from_static(&[0, 255, 42]),
		combined_position: 42,
		stream: tg::process::stdio::Stream::Stdin,
		stream_position: 42,
		timestamp: None,
	};
	let messages = [
		tg::process::stdio::write::ClientRequest::Chunk(chunk),
		tg::process::stdio::write::ClientRequest::End { position: 45 },
	];
	for message in messages {
		let notification = WriteClientNotification {
			id: 9,
			message: tg::process::stdio::write::ClientMessage::Request(message),
		};
		let message = ClientMessage::Notification(ClientNotification::Write(notification));
		let bytes = tangram_serialize::to_vec(&message).unwrap();
		let decoded: ClientMessage = tangram_serialize::from_slice(&bytes).unwrap();
		assert_eq!(
			serde_json::to_value(message).unwrap(),
			serde_json::to_value(decoded).unwrap()
		);
	}
}
