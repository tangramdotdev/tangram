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
				tg::sync::GetMessage::Node(message) => {
					crate::checkpoint!(self.server, "sync.put.input.node").await;
					tracing::trace!(selector = %message.selector, "received get node");
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
								let node = super::resolve::Node { specifier };
								state.resolve_sender.send(node).await.map_err(|_| {
									tg::error!("failed to send the specifier to the resolve task")
								})?;
							}
						},
					}
				},

				tg::sync::GetMessage::Available(tg::sync::GetAvailableMessage::Object(message)) => {
					tracing::trace!(id = %message.id, "received available object");
					state.graph.lock().unwrap().update_object_remote(
						false,
						&message.id,
						None,
						None,
						Some(&tg::object::Availability { subtree: true }),
					);
					state.queue.close_if_end();
				},

				tg::sync::GetMessage::Available(tg::sync::GetAvailableMessage::Process(
					message,
				)) => {
					tracing::trace!(id = %message.id, "received available process");
					let id = message.id;
					let availability = tg::process::Availability {
						node_command: message.node_command_available,
						node_error: message.node_error_available,
						node_log: message.node_log_available,
						node_output: message.node_output_available,
						subtree: message.subtree_available,
						subtree_command: message.subtree_command_available,
						subtree_error: message.subtree_error_available,
						subtree_log: message.subtree_log_available,
						subtree_output: message.subtree_output_available,
					};
					state.graph.lock().unwrap().update_process_remote(
						false,
						&id,
						None,
						Some(&availability),
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
