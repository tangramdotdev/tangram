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
	tangram_either::Either,
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
					let id = Either::Left(complete.id.clone());
					let complete = if state.arg.recursive {
						complete.children_complete
							&& (!state.arg.commands || complete.children_commands_complete)
							&& (!state.arg.outputs || complete.children_outputs_complete)
					} else {
						(!state.arg.commands || complete.command_complete)
							&& (!state.arg.outputs || complete.output_complete)
					};
					state.graph.lock().unwrap().update(None, id, complete);
				},

				tg::sync::GetMessage::Complete(tg::sync::GetCompleteMessage::Object(complete)) => {
					let id = Either::Right(complete.id.clone());
					let complete = true;
					state.graph.lock().unwrap().update(None, id, complete);
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
