use {crate::Session, std::collections::BTreeSet, tangram_client::prelude::*};

pub(super) struct AddProcessChildArg<'a> {
	pub cached: bool,
	pub child: &'a tg::process::Id,
	pub command: &'a tg::command::Id,
	pub lease: Option<&'a str>,
	pub location: Option<&'a tg::Location>,
	pub options: &'a tg::referent::Options,
	pub parent: &'a tg::process::Id,
	pub sandbox: Option<&'a tg::sandbox::Id>,
	pub tokens: &'a tg::authorization::Tokens,
	pub wait: Option<&'a tg::process::wait::Output>,
}

impl Session {
	pub(super) async fn add_process_child(&self, arg: AddProcessChildArg<'_>) -> tg::Result<()> {
		let child = arg.child.clone();
		let command = arg.command.clone();
		let parent = arg.parent.clone();
		let sandbox = arg.sandbox.cloned();
		let mut options = arg.options.clone();
		options.tokens = arg.tokens.clone();
		let data = tg::process::data::Child {
			cached: arg.cached,
			process: tg::Referent::new(child.clone(), options),
		};
		let Some(parent_sandbox) = self.server.runner.state().try_get_process_sandbox(&parent)
		else {
			self.index_process_child(&parent, &data, &command, sandbox.as_ref(), None, arg.wait)
				.await?;
			return Ok(());
		};
		let parent_sandbox = self
			.server
			.runner
			.state()
			.sandboxes()
			.get_by_id(&parent_sandbox)
			.ok_or_else(|| tg::error!("the parent sandbox was not found"))?;
		let mut parent_process = parent_sandbox
			.processes
			.get_mut_by_id(&parent)
			.ok_or_else(|| tg::error!("the parent process was not found"))?;
		if parent_process.data.status.is_finished() {
			return Err(tg::error!("the parent process was finished"));
		}
		if parent_process.children.contains_key(&child) {
			let lease = arg.lease.map(ToOwned::to_owned);
			let location = arg.location.cloned().map(Into::into);
			drop(parent_process);
			drop(parent_sandbox);
			if let Some(lease) = lease {
				let arg = tg::process::cancel::Arg { lease, location };
				self.cancel_process(&child, arg).await.map_err(
					|error| tg::error!(!error, %child, "failed to release a duplicate child lease"),
				)?;
			}

			return Ok(());
		}
		let child_state = crate::process::Child {
			data: data.clone(),
			lease: arg.lease.map(ToOwned::to_owned),
			location: arg.location.cloned().map(Into::into),
		};
		parent_process.children.insert(child.clone(), child_state);
		let parent_data = parent_process.data.clone();
		let control = parent_process.control.clone();
		drop(parent_process);
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
		self.index_process_child(
			&parent,
			&data,
			&command,
			sandbox.as_ref(),
			Some(parent_data),
			arg.wait,
		)
		.await?;

		Ok(())
	}

	async fn index_process_child(
		&self,
		parent: &tg::process::Id,
		child: &tg::process::data::Child,
		command: &tg::command::Id,
		sandbox: Option<&tg::sandbox::Id>,
		parent_data: Option<tg::process::Data>,
		wait: Option<&tg::process::wait::Output>,
	) -> tg::Result<()> {
		let (error, output) = if child.cached {
			match wait {
				Some(wait) => (
					Some(Self::index_process_child_error(wait.error.as_ref())),
					Some(Self::index_process_child_output(wait.output.as_ref())),
				),
				None => (None, None),
			}
		} else {
			(None, None)
		};
		let now = self.server.clock.unix_timestamp()?;
		let child_id = &child.process.node;
		let parent_arg = parent_data.map(|parent_data| {
			let parent_data = parent_data.without_location_and_tokens();
			tangram_index::process::put::Arg {
				cached: false,
				children: None,
				command: parent_data.command.node.clone().into(),
				data: Some(parent_data.clone()),
				error: None,
				id: parent.clone(),
				log: None,
				metadata: tg::process::Metadata::default(),
				options: tg::referent::Options::default(),
				output: None,
				parent: None,
				sandbox: Some(parent_data.sandbox),
				storage: tangram_index::process::Storage::default(),
				time_to_touch: self.server.config.process.time_to_touch,
				touched_at: now,
			}
		});
		let child_arg = tangram_index::process::put::Arg {
			cached: child.cached,
			children: None,
			command: command.clone().into(),
			data: None,
			error,
			id: child_id.clone(),
			log: None,
			metadata: tg::process::Metadata::default(),
			options: child.process.options.clone(),
			output,
			parent: Some(parent.clone()),
			sandbox: sandbox.cloned(),
			storage: tangram_index::process::Storage::default(),
			time_to_touch: self.server.config.process.time_to_touch,
			touched_at: now,
		};
		let account = self.usage_account(&self.context.principal).await?;
		let mut items = Vec::with_capacity(3);
		if let Some(parent_arg) = parent_arg {
			items.push(tangram_index::batch::Item::PutProcess(parent_arg));
		}
		items.push(tangram_index::batch::Item::PutProcess(child_arg));
		if let Some(account) = account {
			items.push(tangram_index::batch::Item::PutAccountProcess(
				tangram_index::usage::storage::put::ProcessArg {
					account,
					process: child_id.clone(),
					touched_at: now,
				},
			));
		}
		let arg = tangram_index::batch::Arg { items };
		self.server
			.index_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to index the process child"))?;

		Ok(())
	}

	fn index_process_child_error(
		error: Option<&tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>>,
	) -> Option<Vec<tg::object::Id>> {
		error.map(|error| match error {
			tg::Either::Left(data) => {
				let mut objects = BTreeSet::new();
				data.children(&mut objects);
				objects.into_iter().collect()
			},
			tg::Either::Right(error) => vec![error.node.clone().into()],
		})
	}

	fn index_process_child_output(output: Option<&tg::value::Data>) -> Option<Vec<tg::object::Id>> {
		let output = output?;
		let mut objects = BTreeSet::new();
		output.children(&mut objects);
		let output = objects.into_iter().collect();

		Some(output)
	}
}
