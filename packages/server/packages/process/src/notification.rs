use {
	crate::Component, tangram_client::prelude::*, tangram_messenger::Messenger as _,
	tangram_server_core::Server,
};

impl Component {
	pub(crate) fn notify_process_log(server: &Server, id: &tg::process::Id) {
		let subject = format!("processes.{id}.log");
		server.notifications.notify(subject);
	}

	pub(crate) async fn publish_process_child_spawned(
		server: &Server,
		id: &tg::process::Id,
	) -> tg::Result<()> {
		let subject = format!("processes.{id}.children");
		server
			.messenger
			.publish(subject, ())
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to publish the child spawned notification")
			})?;

		Ok(())
	}

	pub(crate) fn spawn_publish_process_status_task(server: &Server, id: &tg::process::Id) {
		let subject = format!("processes.{id}.status");
		server.notifications.notify(subject);
	}

	pub(crate) fn spawn_publish_process_stdio_close_message_task(
		server: &Server,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) {
		let subject = format!("processes.{id}.{stream}.close");
		server.notifications.spawn_publish(subject);
	}
}
