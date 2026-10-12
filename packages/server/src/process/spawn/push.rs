use {crate::Session, tangram_client::prelude::*, tangram_futures::task::Task};

#[cfg(test)]
mod tests;

impl Session {
	pub(super) async fn spawn_process_push_command(
		&self,
		command: &mut tg::Referent<tg::Either<tg::process::spawn::CommandArg, tg::command::Id>>,
		location: &tg::Location,
	) -> tg::Result<()> {
		let nodes = Self::spawn_process_command_nodes(command)?;
		if nodes.is_empty() {
			return Ok(());
		}

		// Push the command objects, associating them with the destination sync before sending the spawn request.
		let arg = crate::process::PushProcessObjectsArg {
			location: location.clone(),
			nodes,
			process_command_objects: true,
			sync: None,
		};
		let output = self.push_process_objects(arg).await?;
		Self::set_spawn_process_command_sync(command, &output.sync);

		// Keep transferring after the spawn response and its progress stream have been dropped.
		let session = self.clone();
		let location = location.clone();
		let mut task = Task::spawn(move |_| async move {
			let result = output
				.task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the command push task panicked"))
				.and_then(|result| result);
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), "failed to push the process command");
			} else {
				crate::checkpoint!(session.server, "process.spawn.command.push.finished").await;
			}
			if let tg::Location::Remote(remote) = location {
				session.invalidate_remote_cache(&remote.name).await;
			}
		});
		task.detach();

		Ok(())
	}

	fn set_spawn_process_command_sync(
		command: &mut tg::Referent<tg::Either<tg::process::spawn::CommandArg, tg::command::Id>>,
		sync: &tg::Referent<tg::sync::Id>,
	) {
		Self::inherit_spawn_process_command_tokens(command, &sync.options.tokens);
	}

	pub(super) fn inherit_spawn_process_command_tokens(
		command: &mut tg::Referent<tg::Either<tg::process::spawn::CommandArg, tg::command::Id>>,
		tokens: &tg::authorization::Tokens,
	) {
		let options = tg::referent::Options {
			tokens: tokens.clone(),
			..Default::default()
		};
		command.options.tokens.inherit(tokens);
		if let tg::Either::Left(command) = &mut command.node {
			command.inherit_location_and_tokens(&options);
		}
	}
}
