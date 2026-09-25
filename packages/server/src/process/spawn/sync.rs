use {
	crate::Session,
	futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tokio::sync::mpsc,
	tokio_stream::wrappers::ReceiverStream,
};

#[cfg(test)]
mod tests;

impl Session {
	pub(super) async fn spawn_process_sync_command(
		&self,
		command: &mut tg::Referent<tg::Either<tg::process::spawn::CommandArg, tg::command::Id>>,
		location: &tg::Location,
	) -> tg::Result<()> {
		let nodes = Self::spawn_process_command_nodes(command)?;
		if nodes.is_empty() {
			return Ok(());
		}

		// Open the source sync before requesting the destination token.
		let local = tg::Location::Local(tg::location::Local::default());
		let put = nodes
			.iter()
			.cloned()
			.map(|mut node| {
				node.options.tokens = node.options.tokens.for_location(&local);
				node
			})
			.collect();
		let arg = tg::sync::Arg {
			eager: true,
			location: Some(local.into()),
			process_commands: true,
			put,
			..Default::default()
		};
		let (sender, receiver) = mpsc::channel(1024);
		let input = ReceiverStream::new(receiver).boxed();
		let (_, stream) = self.sync_for_process(arg, input).await?;

		// Associate the spawn objects with the destination sync before sending the spawn request.
		let get = nodes
			.into_iter()
			.map(|mut node| {
				node.options.tokens = node.options.tokens.for_location(location);
				node.map(tg::Selector::Id)
			})
			.collect();
		let arg = tg::sync::Arg {
			eager: true,
			get,
			location: Some(location.clone().into()),
			process_commands: true,
			..Default::default()
		};
		let (output, stream) = self.sync_for_process(arg, stream.boxed()).await?;
		let token = output
			.token
			.ok_or_else(|| tg::error!("the command sync did not produce a token"))?;
		Self::set_spawn_process_command_sync(command, location, token)?;

		// Keep transferring after the spawn response and its progress stream have been dropped.
		let session = self.clone();
		let location = location.clone();
		let mut task = Task::spawn(move |_| async move {
			if let Err(error) = Self::spawn_process_sync_command_task(stream.boxed(), sender).await
			{
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
		location: &tg::Location,
		token: tg::authorization::Token,
	) -> tg::Result<()> {
		let mut options = tg::referent::Options::default();
		options.tokens.insert_authorization(location.clone(), token);
		command.options.tokens.inherit(&options.tokens);
		if let tg::Either::Left(command) = &mut command.node {
			let host = command
				.host
				.clone()
				.ok_or_else(|| tg::error!("expected a resolved host"))?;
			let mut data = tg::process::data::Command::new(command.clone(), host);
			data.inherit_location_and_tokens(&options);
			*command = data.to_spawn_arg();
		}
		Ok(())
	}

	async fn spawn_process_sync_command_task(
		mut stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
		sender: mpsc::Sender<tg::Result<tg::sync::Message>>,
	) -> tg::Result<()> {
		while let Some(message) = stream.try_next().await? {
			if matches!(message, tg::sync::Message::End) {
				return Ok(());
			}
			sender
				.send(Ok(message))
				.await
				.map_err(|_| tg::error!("the command sync closed"))?;
		}
		Err(tg::error!("the command sync ended unexpectedly"))
	}
}
