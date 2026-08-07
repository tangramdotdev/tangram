use {
	crate::{Session, sync::put::State},
	futures::{StreamExt as _, stream::BoxStream},
	tangram_client::prelude::*,
};

impl Session {
	#[tracing::instrument(level = "trace", name = "input", ret, skip_all)]
	pub(super) async fn sync_put_input_task(
		&self,
		state: &State,
		mut stream: BoxStream<'static, tg::sync::GetMessage>,
	) -> tg::Result<()> {
		while let Some(message) = stream.next().await {
			match message {
				tg::sync::GetMessage::Item(message) => {
					tracing::trace!(id = %message.id, "received get item");
					state
						.graph
						.lock()
						.unwrap()
						.insert_remote_root(message.id.clone());
					state
						.queue
						.enqueue(message.eager, message.id, message.token)?;
				},

				tg::sync::GetMessage::Stored(tg::sync::GetStoredMessage::Object(message)) => {
					tracing::trace!(id = %message.id, "received stored object");
					state.graph.lock().unwrap().update_object_remote(
						&message.id,
						None,
						None,
						Some(&tangram_index::object::Stored { subtree: true }),
					);
					if state.graph.lock().unwrap().end_remote() {
						state.queue.close();
					}
				},

				tg::sync::GetMessage::Stored(tg::sync::GetStoredMessage::Process(message)) => {
					tracing::trace!(id = %message.id, "received stored process");
					let id = message.id;
					let stored = tangram_index::process::Stored {
						subtree: message.subtree_stored,
						subtree_command: message.subtree_command_stored,
						subtree_error: message.subtree_error_stored,
						subtree_log: message.subtree_log_stored,
						subtree_output: message.subtree_output_stored,
						node_command: message.node_command_stored,
						node_error: message.node_error_stored,
						node_log: message.node_log_stored,
						node_output: message.node_output_stored,
					};
					state
						.graph
						.lock()
						.unwrap()
						.update_process_remote(&id, None, Some(&stored));
					if state.graph.lock().unwrap().end_remote() {
						state.queue.close();
					}
				},

				tg::sync::GetMessage::Progress(_) => (),

				tg::sync::GetMessage::End => {
					tracing::trace!("received end");
					state.graph.lock().unwrap().mark_get_end_received();
					if state.graph.lock().unwrap().end_remote() {
						state.queue.close();
					}
					return Ok(());
				},
			}
		}

		Err(tg::error!("failed to receive the get end message"))
	}
}
