use {
	crate::Component, tangram_client::prelude::*, tangram_messenger::Messenger as _,
	tangram_server_core::Server,
};

impl Component {
	pub(crate) async fn publish_sandbox_control_connected(
		server: &Server,
		id: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let subject = tangram_server_sandbox_common::control::connected_subject(id);
		server
			.messenger
			.publish(subject, ())
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to publish the sandbox control connection")
			})?;

		Ok(())
	}

	pub(crate) async fn publish_sandbox_process_spawned(
		server: &Server,
		id: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let subject = format!("sandboxes.{id}.processes");
		server
			.messenger
			.publish(subject, ())
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to publish the sandbox process spawned notification"
				)
			})?;

		Ok(())
	}

	pub(crate) fn spawn_publish_sandbox_status_task(server: &Server, id: &tg::sandbox::Id) {
		let subject = format!("sandboxes.{id}.status");
		server.notifications.notify(subject);
	}
}
