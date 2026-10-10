use super::*;

#[test]
fn bytes_and_messages_bound_independently() {
	let limits = Limits {
		bytes: 8,
		messages: 2,
	};
	let mut sender = Sender::new(limits);
	sender.send(1).unwrap();
	sender.send(1).unwrap();
	assert!(!sender.available(1));
	let consumption = Consumption {
		bytes: 1,
		messages: 1,
	};
	sender.update(consumption).unwrap();
	sender.update(consumption).unwrap();
	sender.send(7).unwrap();
	assert!(!sender.available(1));
	assert!(
		sender
			.update(Consumption {
				bytes: 10,
				messages: 4
			})
			.is_err()
	);
	assert!(sender.update(Consumption::default()).is_err());
}

#[test]
fn receiver_reports_either_threshold_and_flushes() {
	let limits = Limits {
		bytes: 8,
		messages: 4,
	};
	let mut receiver = Receiver::new(limits);
	receiver.receive(1).unwrap();
	assert!(receiver.consume(1).unwrap().is_none());
	receiver.receive(1).unwrap();
	assert_eq!(
		receiver.consume(1).unwrap(),
		Some(Consumption {
			bytes: 2,
			messages: 2
		})
	);
	receiver.receive(4).unwrap();
	assert_eq!(
		receiver.consume(4).unwrap(),
		Some(Consumption {
			bytes: 6,
			messages: 3
		})
	);
	receiver.receive(1).unwrap();
	assert!(receiver.consume(1).unwrap().is_none());
	assert_eq!(
		receiver.flush(),
		Some(Consumption {
			bytes: 7,
			messages: 4
		})
	);
	assert!(receiver.flush().is_none());
	assert!(receiver.consume(1).is_err());
}

#[test]
fn zero_byte_messages_consume_message_credit() {
	let limits = Limits {
		bytes: 8,
		messages: 1,
	};
	let mut receiver = Receiver::new(limits);
	receiver.receive(0).unwrap();
	assert!(receiver.receive(0).is_err());
	assert_eq!(
		receiver.consume(0).unwrap(),
		Some(Consumption {
			bytes: 0,
			messages: 1
		})
	);
	receiver.receive(0).unwrap();
}

#[test]
fn consumption_preserves_both_counts_in_json_and_tangram() {
	let consumption = Consumption {
		bytes: 1234,
		messages: 17,
	};
	let json = serde_json::to_vec(&consumption).unwrap();
	assert_eq!(
		serde_json::from_slice::<Consumption>(&json).unwrap(),
		consumption
	);
	let bytes = tangram_serialize::to_vec(&consumption).unwrap();
	assert_eq!(
		tangram_serialize::from_slice::<Consumption>(&bytes).unwrap(),
		consumption
	);
}

#[test]
fn http_windows_leave_application_and_connection_headroom() {
	let config = crate::http2::Config::default();
	let limits = Limits {
		bytes: 2 * 1024 * 1024,
		messages: 64,
	};
	config.validate_flow(limits, limits, 4).unwrap();
	let config = crate::http2::Config {
		connection_window_size: config.stream_window_size,
		..config
	};
	assert!(config.validate().is_err());
	let config = crate::http2::Config {
		stream_window_size: 1024,
		..crate::http2::Config::default()
	};
	assert!(config.validate_flow(limits, limits, 4).is_err());
	assert!(config.validate_flow(limits, limits, usize::MAX).is_err());
}

#[test]
fn http_window_validation_is_independent_of_request_concurrency() {
	let config = crate::http2::Config::default();
	for max_concurrent_streams in [None, Some(1), Some(u32::MAX)] {
		let config = crate::http2::Config {
			max_concurrent_streams,
			..config
		};
		config.validate().unwrap();
	}
	let config = crate::http2::Config {
		max_concurrent_streams: Some(0),
		..config
	};
	assert!(config.validate().is_err());
}

#[test]
fn http_windows_reserve_metadata_for_tiny_stdio_messages() {
	let config = crate::http2::Config {
		connection_window_size: 1024 * 1024,
		max_concurrent_streams: None,
		stream_window_size: 1024,
	};
	let limits = Limits {
		bytes: 64,
		messages: 64,
	};
	assert!(config.validate_flow(limits, limits, 1).is_err());
}

#[test]
fn cumulative_counters_reject_overflow() {
	let limits = Limits {
		bytes: 8,
		messages: 8,
	};
	let maximum = Consumption {
		bytes: u64::MAX,
		messages: u64::MAX,
	};
	let mut sender = Sender {
		consumption: maximum,
		limits,
		sent: maximum,
	};
	assert!(sender.send(1).is_err());
	assert_eq!(sender.sent, maximum);
	let mut receiver = Receiver {
		consumption: maximum,
		limits,
		received: maximum,
		reported: maximum,
	};
	assert!(receiver.receive(1).is_err());
	assert!(receiver.consume(1).is_err());
	assert_eq!(receiver.received, maximum);
	assert_eq!(receiver.consumption, maximum);
}
