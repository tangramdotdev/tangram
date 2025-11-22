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
	pub(super) async fn sync_put_input_task(
		&self,
		state: &State,
		mut stream: BoxStream<'static, tg::sync::GetMessage>,
	) -> tg::Result<()> {
		while let Some(message) = stream.next().await {
			match message {
				tg::sync::GetMessage::Item(tg::sync::GetItemMessage::Process(message)) => {
					let item = ProcessItem {
						parent: None,
						id: message.id,
						eager: message.eager,
					};
					state.queue.enqueue_process(item);
				},

				tg::sync::GetMessage::Item(tg::sync::GetItemMessage::Object(message)) => {
					let item = ObjectItem {
						parent: None,
						id: message.id,
						eager: message.eager,
					};
					state.queue.enqueue_object(item);
				},

				tg::sync::GetMessage::Complete(tg::sync::GetCompleteMessage::Process(complete)) => {
					let complete_output = crate::process::complete::Output {
						children: complete.children_complete,
						children_commands: complete.children_commands_complete,
						children_outputs: complete.children_outputs_complete,
						command: complete.command_complete,
						output: complete.output_complete,
					};
					state
						.graph
						.lock()
						.unwrap()
						.update_process(&complete.id, None, Some(complete_output));
				},

				tg::sync::GetMessage::Complete(tg::sync::GetCompleteMessage::Object(complete)) => {
					state
						.graph
						.lock()
						.unwrap()
						.update_object(&complete.id, None, true);
				},

				tg::sync::GetMessage::Progress(_) => (),

				tg::sync::GetMessage::End => {
					state.queue.decrement(1);
				},
			}
		}
		Ok(())
	}
}
