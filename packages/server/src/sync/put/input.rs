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
		let mut end = false;
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
						kind: None,
						eager: message.eager,
					};
					state.queue.enqueue_object(item);
				},

				tg::sync::GetMessage::Complete(tg::sync::GetCompleteMessage::Process(complete)) => {
					let id = complete.id;
					let complete = crate::process::complete::Output {
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
						.update_process(&id, None, Some(&complete));
				},

				tg::sync::GetMessage::Complete(tg::sync::GetCompleteMessage::Object(complete)) => {
					state
						.graph
						.lock()
						.unwrap()
						.update_object(&complete.id, None, None, Some(true));
				},

				tg::sync::GetMessage::Progress(_) => (),

				tg::sync::GetMessage::End => {
					end = true;
					state.queue.decrement(1);
				},
			}
		}
		if !end {
			return Err(tg::error!("failed to receive the get end message"));
		}
		Ok(())
	}
}
