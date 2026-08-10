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
					tracing::trace!(selector = %message.selector, "received get item");
					match message.selector {
						tg::Selector::Id(id) => {
							state.queue.enqueue_root_with_descendants(
								message.descendants,
								message.eager,
								id,
								message.token,
							)?;
						},
						tg::Selector::Specifier(specifier) => {
							let inserted = state.graph.lock().unwrap().insert_remote_selector(
								message.descendants,
								message.eager,
								specifier.clone(),
								message.token,
							);
							if inserted {
								let item = super::resolve::Item { specifier };
								state.resolve_sender.send(item).await.map_err(|_| {
									tg::error!("failed to send the specifier to the resolve task")
								})?;
							}
						},
					}
				},

				tg::sync::GetMessage::Stored(tg::sync::GetStoredMessage::Object(message)) => {
					tracing::trace!(id = %message.id, "received stored object");
					state.graph.lock().unwrap().update_object_remote(
						false,
						&message.id,
						None,
						None,
						Some(&tangram_index::object::Stored { subtree: true }),
					);
					state.queue.close_if_end();
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
					state.graph.lock().unwrap().update_process_remote(
						false,
						&id,
						None,
						Some(&stored),
					);
					state.queue.close_if_end();
				},

				tg::sync::GetMessage::Progress(_) => (),

				tg::sync::GetMessage::End => {
					tracing::trace!("received end");
					state.graph.lock().unwrap().mark_get_end_received();
					state.resolve_sender.close();
					let end = state.queue.close_if_end();
					crate::checkpoint!(self.server, "sync.put.input.end", end).await;
					return Ok(());
				},
			}
		}

		Err(tg::error!("failed to receive the get end message"))
	}
}
