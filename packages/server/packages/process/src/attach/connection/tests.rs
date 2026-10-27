use super::*;

#[test]
fn excess_process_traffic_fails_instead_of_buffering() {
	let max_pending_messages = 64;
	let mut pending = VecDeque::new();
	for id in 0..=max_pending_messages as u64 {
		let request = tg::process::attach::ClientRequest {
			arg: tg::process::attach::ClientRequestArg::Detach,
			id,
		};
		let message = tg::process::attach::ClientMessage::Request(request);
		let result =
			Component::attach_process_buffer_message(&mut pending, message, max_pending_messages);
		if id < max_pending_messages as u64 {
			result.unwrap();
		} else {
			assert!(
				result
					.unwrap_err()
					.to_string()
					.contains("too many buffered process messages")
			);
		}
	}
	assert_eq!(pending.len(), max_pending_messages);
}
