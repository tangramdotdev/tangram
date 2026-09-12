use {
	super::*,
	std::{collections::BTreeMap, io::SeekFrom, time::Duration},
};

#[test]
fn opening_metadata_preserves_read_options() {
	let read = tg::process::stdio::read::Arg {
		length: Some(-7),
		position: Some(SeekFrom::End(-3)),
		size: Some(1024),
		streams: vec![tg::process::stdio::Stream::Stdout],
		timeout: Some(Duration::new(2, 1)),
		..Default::default()
	};
	let arg = Arg {
		reads: BTreeMap::from([(1, read)]),
		target: Target::Existing {
			id: tg::process::Id::new(),
			options: tg::process::wait::Arg {
				lease: Some("lease".to_owned()),
				location: Some("remote:test".parse().unwrap()),
				..Default::default()
			},
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
fn spawn_metadata_uses_native_types() {
	let arg = serde_json::json!({
		"cache_location": "local",
		"cached": false,
		"command": {"node": {
			"args": [
				{"kind": "string", "value": "hello"},
				{"kind": "value", "value": null}
			],
			"cwd": "/tmp",
			"env": {"EXAMPLE": {"kind": "value", "value": true}},
			"executable": {"node": {"path": "/bin/echo"}},
			"host": "aarch64-darwin",
			"user": "test"
		}},
		"debug": {"addr": "127.0.0.1:9229", "mode": "wait"},
		"location": "remote:test",
		"parent": tg::process::Id::new(),
		"public": true,
		"retry": true,
		"sandbox": {
			"cpu": 2,
			"host": "aarch64-darwin",
			"hostname": "test",
			"isolation": {"kind": "seatbelt"},
			"location": "local",
			"memory": 1_048_576,
			"network": {"kind": "host"},
			"ttl": 60.125
		},
		"stderr": "log",
		"stdin": "pipe",
		"stdout": "pipe",
		"tty": {"size": {"cols": 80, "rows": 24}}
	});
	let mut arg: tg::process::spawn::Arg = serde_json::from_value(arg).unwrap();
	for mode in [Mode::Run, Mode::Spawn] {
		let request = Arg {
			reads: BTreeMap::new(),
			target: Target::Spawn {
				arg: Box::new(arg.clone()),
				mode,
			},
		};
		assert_roundtrip(&ClientRequestArg::Connect(request));
		arg.command.node = tg::Either::Right(tg::command::Id::new(b"command"));
		arg.sandbox = Some(tg::Either::Right(tg::sandbox::Id::new()));
		arg.tty = Some(tg::Either::Left(true));
	}
}

#[test]
fn operation_payloads_are_native_structs() {
	let args = [
		serde_json::json!({"kind": "cancel", "value": {"lease": "lease", "location": "local"}}),
		serde_json::json!({"kind": "read", "value": {"position": "current.5", "streams": "stdout,stderr"}}),
		serde_json::json!({"kind": "signal", "value": {"signal": "TERM"}}),
		serde_json::json!({"kind": "tty", "value": {"size": {"cols": 80, "rows": 24}}}),
		serde_json::json!({"kind": "write", "value": {"streams": "stdin"}}),
	];
	for arg in args {
		let arg: ClientRequestArg = serde_json::from_value(arg).unwrap();
		assert_roundtrip(&arg);
		let bytes = tangram_serialize::to_vec(&arg).unwrap();
		let tangram_serialize::Value::Enum(value) = tangram_serialize::from_slice(&bytes).unwrap()
		else {
			panic!("expected a native enum");
		};
		assert!(matches!(*value.value, tangram_serialize::Value::Struct(_)));
	}
}

#[test]
fn progress_preserves_variants() {
	let events = [
		serde_json::json!({"kind": "diagnostic", "value": {"message": "diagnostic", "severity": "warning"}}),
		serde_json::json!({"kind": "indicators", "value": [
			{"current": 1, "format": "bytes", "name": "download", "title": "Downloading", "total": null},
			{"current": null, "format": "normal", "name": "build", "title": "Building", "total": 2}
		]}),
		serde_json::json!({"kind": "log", "value": {"level": null, "message": "log"}}),
		serde_json::json!({"kind": "log", "value": {"level": "error", "message": "error"}}),
		serde_json::json!({"kind": "log", "value": {"level": "info", "message": "info"}}),
		serde_json::json!({"kind": "log", "value": {"level": "success", "message": "success"}}),
		serde_json::json!({"kind": "log", "value": {"level": "warning", "message": "warning"}}),
		serde_json::json!({"kind": "output", "value": null}),
	];
	for event in events {
		let event: tg::progress::Event<()> = serde_json::from_value(event).unwrap();
		assert_roundtrip(&ServerNotification::Progress(event));
	}
}

#[test]
fn responses_preserve_errors_and_optional_null_outputs() {
	let error = tg::error::Data {
		message: Some("failed".to_owned()),
		..Default::default()
	};
	let errors = [
		None,
		Some(tg::Either::Left(error)),
		Some(tg::Either::Right(tg::Referent::with_node(
			tg::error::Id::new(b"error"),
		))),
	];
	for error in errors {
		for value in [None, Some(tg::value::Data::Null)] {
			let wait = tg::process::wait::Output {
				error: error.clone(),
				exit: 1,
				output: value,
			};
			let decoded = assert_roundtrip(&wait);
			assert_eq!(decoded.output.is_some(), wait.output.is_some());
			assert_roundtrip(&ServerNotification::Wait(wait.clone()));
			let output = tg::process::spawn::Output {
				cached: true,
				lease: Some("lease".to_owned()),
				location: Some("remote:test".parse().unwrap()),
				process: tg::Either::Right(tg::process::Id::new()),
				tokens: tg::authorization::Tokens::default(),
				wait: Some(wait),
			};
			assert_roundtrip(&ServerResponseOutput::Connect(output));
		}
	}
	let output = tg::process::cancel::Output { released: true };
	assert_roundtrip(&ServerResponseOutput::Cancel(output));
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

fn assert_roundtrip<T>(value: &T) -> T
where
	T: serde::Serialize
		+ tangram_serialize::Serialize
		+ for<'de> tangram_serialize::Deserialize<'de>,
{
	let bytes = tangram_serialize::to_vec(value).unwrap();
	let decoded: T = tangram_serialize::from_slice(&bytes).unwrap();
	assert_eq!(
		serde_json::to_value(value).unwrap(),
		serde_json::to_value(&decoded).unwrap()
	);
	assert_eq!(bytes, tangram_serialize::to_vec(&decoded).unwrap());
	decoded
}
