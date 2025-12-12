use {
	crate::{
		Server,
		sync::{
			put::State,
			queue::{ObjectItem, ProcessItem},
		},
	},
	futures::{StreamExt as _, stream::BoxStream},
	tangram_client::prelude::*,
};

impl Server {
	#[tracing::instrument(level = "debug", name = "input", ret, skip_all)]
	pub(super) async fn sync_put_input_task(
		&self,
		state: &State,
		mut stream: BoxStream<'static, tg::sync::GetMessage>,
	) {
		while let Some(message) = stream.next().await {
			match message {
				tg::sync::GetMessage::Item(tg::sync::GetItemMessage::Object(message)) => {
					tracing::trace!(id = %message.id, "received get object");
					let item = ObjectItem {
						parent: None,
						id: message.id,
						kind: None,
						eager: message.eager,
					};
					state.queue.enqueue_object(item);
				},

				tg::sync::GetMessage::Item(tg::sync::GetItemMessage::Process(message)) => {
					tracing::trace!(id = %message.id, "received get process");
					let item = ProcessItem {
						parent: None,
						id: message.id,
						eager: message.eager,
					};
					state.queue.enqueue_process(item);
				},

				tg::sync::GetMessage::Stored(tg::sync::GetStoredMessage::Object(message)) => {
					tracing::trace!(id = %message.id, "received stored object");
					state.graph.lock().unwrap().update_object(
						&message.id,
						None,
						None,
						Some(crate::object::stored::Output { subtree: true }),
					);
				},

				tg::sync::GetMessage::Stored(tg::sync::GetStoredMessage::Process(message)) => {
					tracing::trace!(id = %message.id, "received stored process");
					let id = message.id;
					let stored = crate::process::stored::Output {
						subtree: message.subtree_stored,
						subtree_command: message.subtree_command_stored,
						subtree_output: message.subtree_output_stored,
						node_command: message.node_command_stored,
						node_output: message.node_output_stored,
					};
					state
						.graph
						.lock()
						.unwrap()
						.update_process(&id, None, Some(&stored));
				},

				tg::sync::GetMessage::Progress(_) => (),

				tg::sync::GetMessage::End => {
					tracing::trace!("received end");
					state.queue.close();
				},
			}
		}
	}
}
