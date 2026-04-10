use {crate::Server, tangram_client::prelude::*, tangram_messenger::prelude::*};

pub(crate) mod read;
pub(crate) mod write;

pub(crate) const MAX_UNREAD_PROCESS_STDIO_BYTES: u64 = 1024 * 1024;

impl Server {
	pub(crate) fn spawn_publish_process_stdio_close_message_task(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) {
		self.spawn_publish_process_stdio_message_task(id, stream, "close");
	}

	pub(crate) fn spawn_publish_process_stdio_read_message_task(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) {
		self.spawn_publish_process_stdio_message_task(id, stream, "read");
	}

	pub(crate) fn spawn_publish_process_stdio_write_message_task(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) {
		self.spawn_publish_process_stdio_message_task(id, stream, "write");
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
