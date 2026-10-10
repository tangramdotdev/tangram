use {super::*, futures::FutureExt as _};

#[tokio::test]
async fn sync_waits_for_configuration_and_consumption() {
	let updates = Updates::new();
	let input = stream::iter((0..3).map(|_| Ok(tg::sync::Message::End))).boxed();
	let mut output = updates.send_sync_messages(input);
	assert!(output.next().now_or_never().is_none());
	let config = tg::sync::Config {
		limits: tangram_http::flow::Limits {
			bytes: 1024,
			messages: 2,
		},
		max_frame_size: 512,
		max_message_size: 512,
		max_object_size: 128,
	};
	updates.set_sync_config(config).unwrap();
	assert!(output.try_next().await.unwrap().is_some());
	assert!(output.try_next().await.unwrap().is_some());
	assert!(output.next().now_or_never().is_none());
	updates
		.update_sync_consumption(Consumption {
			bytes: 64,
			messages: 1,
		})
		.unwrap();
	assert!(output.try_next().await.unwrap().is_some());
	assert!(output.try_next().await.unwrap().is_none());
}

#[tokio::test]
async fn credit_is_returned_after_the_sync_consumer_advances() {
	let config = tg::sync::Config {
		limits: tangram_http::flow::Limits {
			bytes: 1024,
			messages: 2,
		},
		max_frame_size: 512,
		max_message_size: 512,
		max_object_size: 128,
	};
	let (sender, mut output, mut consumption) = Input::new(config).unwrap();
	sender.receive_sync_message(tg::sync::Message::End).unwrap();
	sender.receive_sync_message(tg::sync::Message::End).unwrap();
	assert!(sender.receive_sync_message(tg::sync::Message::End).is_err());
	output.try_next().await.unwrap().unwrap();
	assert!(consumption.next().now_or_never().is_none());
	output.try_next().await.unwrap().unwrap();
	assert_eq!(
		consumption.next().await,
		Some(Consumption {
			bytes: 64,
			messages: 1
		})
	);
	sender.receive_sync_message(tg::sync::Message::End).unwrap();
	drop(sender);
	output.try_next().await.unwrap().unwrap();
	assert!(output.try_next().await.unwrap().is_none());
	assert_eq!(
		consumption.next().await,
		Some(Consumption {
			bytes: 192,
			messages: 3
		})
	);
	assert!(consumption.next().await.is_none());
}

#[test]
fn a_message_cannot_use_the_credit_reserved_for_consumption_batching() {
	let config = tg::sync::Config {
		limits: tangram_http::flow::Limits {
			bytes: 512,
			messages: 16,
		},
		max_frame_size: 512,
		max_message_size: 512,
		max_object_size: 128,
	};
	assert!(config.validate().is_err());
	let config = tg::sync::Config {
		limits: tangram_http::flow::Limits {
			bytes: 1024,
			..config.limits
		},
		..config
	};
	config.validate().unwrap();
}

#[tokio::test]
async fn small_messages_do_not_prevent_large_messages_from_returning_credit() {
	let config = tg::sync::Config {
		limits: tangram_http::flow::Limits {
			bytes: 1024,
			messages: 16,
		},
		max_frame_size: 512,
		max_message_size: 512,
		max_object_size: 128,
	};
	let large = tg::sync::GetOutputMessage { nodes: Vec::new() };
	let large = tg::sync::Message::Get(tg::sync::GetMessage::Output(large));
	let messages = stream::iter([Ok(tg::sync::Message::End), Ok(large.clone()), Ok(large)]).boxed();
	let updates = Updates::new();
	updates.set_sync_config(config).unwrap();
	let mut messages = updates.send_sync_messages(messages);
	let (sender, mut input, mut consumption) = Input::new(config).unwrap();
	for _ in 0..2 {
		let message = messages
			.try_next()
			.now_or_never()
			.unwrap()
			.unwrap()
			.unwrap();
		sender.receive_sync_message(message).unwrap();
		input.try_next().await.unwrap().unwrap();
	}
	assert!(messages.try_next().now_or_never().is_none());
	assert!(input.try_next().now_or_never().is_none());
	let consumption = consumption.next().await.unwrap();
	assert_eq!(consumption.bytes, 576);
	assert_eq!(consumption.messages, 2);
	updates.update_sync_consumption(consumption).unwrap();
	assert!(
		messages
			.try_next()
			.now_or_never()
			.unwrap()
			.unwrap()
			.is_some()
	);
}
