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
		let mut end = false;
		while let Some(message) = stream.next().await {
			match message {
				tg::sync::GetMessage::Item(tg::sync::GetItemMessage::Process(message)) => {
					tracing::trace!(id = %message.id, "received get process");
					let item = ProcessItem {
						parent: None,
						id: message.id,
						eager: message.eager,
					};
					state.queue.enqueue_process(item);
				},

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

				tg::sync::GetMessage::Complete(tg::sync::GetCompleteMessage::Process(message)) => {
					tracing::trace!(id = %message.id, "received complete process");
					let id = message.id;
					let complete = crate::process::complete::Output {
						children: message.children_complete,
						children_commands: message.children_commands_complete,
						children_outputs: message.children_outputs_complete,
						command: message.command_complete,
						output: message.output_complete,
					};
					state
						.graph
						.lock()
						.unwrap()
						.update_process(&id, None, Some(&complete));
				},

				tg::sync::GetMessage::Complete(tg::sync::GetCompleteMessage::Object(message)) => {
					tracing::trace!(id = %message.id, "received complete object");
					state
						.graph
						.lock()
						.unwrap()
						.update_object(&message.id, None, None, Some(true));
				},

				tg::sync::GetMessage::Progress(_) => (),

				tg::sync::GetMessage::End => {
					tracing::trace!("received end");
					state.queue.decrement(1);
				},
			}
		}
	}
}
