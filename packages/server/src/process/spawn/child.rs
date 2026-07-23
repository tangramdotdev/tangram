use {crate::Session, tangram_client::prelude::*};

pub(super) struct AddProcessChildArg<'a> {
	pub cached: bool,
	pub child: &'a tg::process::Id,
	pub command: &'a tg::command::Id,
	pub lease: Option<&'a str>,
	pub location: Option<&'a tg::Location>,
	pub options: &'a tg::referent::Options,
	pub parent: &'a tg::process::Id,
	pub sandbox: Option<&'a tg::sandbox::Id>,
	pub token: Option<&'a tg::grant::Token>,
}

impl Session {
	pub(super) async fn add_process_child(&self, arg: AddProcessChildArg<'_>) -> tg::Result<()> {
		let child = arg.child.clone();
		let command = arg.command.clone();
		let parent = arg.parent.clone();
		let sandbox = arg.sandbox.cloned();
		let parent_sandbox = self
			.server
			.runner
			.state
			.try_get_process_sandbox(&parent)
			.ok_or_else(|| tg::error!("the parent process was not found"))?;
		let mut parent_sandbox = self
			.server
			.runner
			.state
			.sandboxes
			.get_mut(&parent_sandbox)
			.ok_or_else(|| tg::error!("the parent sandbox was not found"))?;
		let parent_process = parent_sandbox
			.processes
			.get_mut(&parent)
			.ok_or_else(|| tg::error!("the parent process was not found"))?;
		if parent_process.data.status.is_finished() {
			return Err(tg::error!("the parent process was finished"));
		}
		let mut options = arg.options.clone();
		options.token = arg.token.cloned();
		parent_process
			.data
			.children
			.get_or_insert_default()
			.push(tg::process::data::Child {
				cached: arg.cached,
				process: tg::Referent::new(child.clone(), options),
			});
		if let Some(lease) = arg.lease {
			parent_process.children.push(crate::process::Child {
				lease: lease.to_owned(),
				location: arg.location.cloned().map(Into::into),
				process: child.clone(),
			});
		}
		let parent_data = parent_process.data.clone();
		let control = parent_process.control.clone();
		drop(parent_sandbox);

		// Notify the server that the child was spawned.
		control
			.send(tg::process::control::ClientMessage::Notification(
				tg::process::control::ClientNotification::ChildSpawned,
			))
			.await
			.map_err(
				|error| tg::error!(!error, %parent, "failed to send the child spawned notification"),
			)?;

		// Index the child.
		self.index_process_child(&parent, &child, &command, sandbox.as_ref(), parent_data)
			.await?;

		Ok(())
	}

	async fn index_process_child(
		&self,
		parent: &tg::process::Id,
		child: &tg::process::Id,
		command: &tg::command::Id,
		sandbox: Option<&tg::sandbox::Id>,
		parent_data: tg::process::Data,
	) -> tg::Result<()> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let parent_data = parent_data.without_tokens();
		let parent_arg = tangram_index::process::put::Arg {
			children: None,
			command: parent_data.command.clone().into(),
			data: Some(parent_data.clone()),
			error: None,
			id: parent.clone(),
			log: None,
			metadata: tg::process::Metadata::default(),
			output: None,
			parent: None,
			sandbox: Some(parent_data.sandbox),
			stored: tangram_index::process::Stored::default(),
			time_to_touch: self.server.config.process.time_to_touch,
			touched_at: now,
		};
		let child_arg = tangram_index::process::put::Arg {
			children: None,
			command: command.clone().into(),
			data: None,
			error: None,
			id: child.clone(),
			log: None,
			metadata: tg::process::Metadata::default(),
			output: None,
			parent: Some(parent.clone()),
			sandbox: sandbox.cloned(),
			stored: tangram_index::process::Stored::default(),
			time_to_touch: self.server.config.process.time_to_touch,
			touched_at: now,
		};
		let arg = tangram_index::batch::Arg {
			items: vec![
				tangram_index::batch::Item::PutProcess(parent_arg),
				tangram_index::batch::Item::PutProcess(child_arg),
			],
		};
		self.server
			.index_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to index the process child"))?;

		Ok(())
	}
}
