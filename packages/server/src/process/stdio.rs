use {crate::Server, tangram_client::prelude::*, tangram_messenger::prelude::*};

pub mod read;
pub mod write;

impl Server {
	pub(crate) fn spawn_publish_process_stdio_close_message_task(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) {
		self.spawn_publish_process_stdio_message_task(id, stream, "close");
	}

	fn spawn_publish_process_stdio_message_task(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		action: &str,
	) {
		let id = id.clone();
		let action = action.to_owned();
		let subject = format!("processes.{id}.{stream}.{action}");
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish(subject, ())
					.await
					.inspect_err(|error| {
						tracing::error!(
							%error,
							%id,
							%stream,
							%action,
							"failed to publish the process stdio message"
						);
					})
					.ok();
			}
		});
	}
}
