use {super::Messenger, futures::TryStreamExt as _, tangram_messenger::Messenger as _};

#[tokio::test]
async fn subject_prefixes() {
	for (instance, region, expected) in [
		(None, None, "syncs.test.control.server"),
		(None, Some("region"), "region.syncs.test.control.server"),
		(Some("instance"), None, "instance.syncs.test.control.server"),
		(
			Some("instance"),
			Some("region"),
			"instance.region.syncs.test.control.server",
		),
		(Some(""), Some(""), "syncs.test.control.server"),
	] {
		let messenger = Messenger::memory(instance.map(str::to_owned), region.map(str::to_owned));
		let messages = messenger
			.subscribe::<()>("syncs.test.control.*".into())
			.await
			.unwrap();
		let mut messages = std::pin::pin!(messages);
		messenger
			.publish("syncs.test.control.server".into(), ())
			.await
			.unwrap();
		let message = tokio::time::timeout(std::time::Duration::from_secs(1), messages.try_next())
			.await
			.unwrap()
			.unwrap()
			.unwrap();
		assert_eq!(message.subject, expected);
		assert!(messenger.matches_subject(&message.subject, "syncs.test.control.server".into()));
		assert!(!messenger.matches_subject(&message.subject, "syncs.other.control.server".into()));
		assert!(!messenger.matches_subject(
			"foreign.syncs.test.control.server",
			"syncs.test.control.server".into()
		));
	}
}
