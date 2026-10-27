use {
	crate::Component,
	futures::TryStreamExt as _,
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_server_core::{
		Session,
		process::{PushProcessObjectsArg, PushProcessObjectsOutput},
	},
};

impl Component {
	pub(crate) async fn push_process_objects(
		&self,
		session: &Session,
		arg: PushProcessObjectsArg,
	) -> tg::Result<PushProcessObjectsOutput> {
		let PushProcessObjectsArg {
			join,
			location,
			nodes,
			process_command_objects,
			sync,
		} = arg;

		// Give each node its local tokens for the source, and the destination's tokens with the given sync's tokens for the destination, keyed so that the push delivers each to its side.
		let local = tg::Location::Local(tg::location::Local::default());
		let nodes = nodes
			.into_iter()
			.map(|mut node| {
				let mut destination_tokens = node.options.tokens.for_location(&location);
				if let Some(sync) = &sync {
					for token in sync.options.tokens.local_authorization() {
						destination_tokens.insert_local_authorization(token.clone());
					}
				}
				let mut tokens = node.options.tokens.for_location(&local);
				for token in destination_tokens.local_authorization() {
					if location.is_remote() {
						tokens.insert_authorization(location.clone(), token.clone());
					} else {
						tokens.insert_local_authorization(token.clone());
					}
				}
				node.options.tokens = tokens;
				node
			})
			.collect();

		// Push only the nodes, with every option set explicitly because the push defaults include process outputs, errors, and tag targets, reusing the given sync's id at the destination when there is one, and retrying with the process push retry options.
		let arg = tg::push::Arg {
			ancestors: tg::node::AncestorsPull::default(),
			destination: Some(location),
			eager: true,
			force: false,
			group_children: false,
			metadata: false,
			nodes,
			organization_children: false,
			process_children: false,
			process_command_objects,
			process_error_objects: false,
			process_log_objects: false,
			process_output_objects: false,
			sandbox_processes: false,
			source: Some(local),
			tag_targets: false,
			user_children: false,
		};
		let retry = session.server.config.process.push_retry.clone().into();
		let (header, stream) = session
			.server
			.components
			.push
			.push_for_process(session, arg, sync, join, retry)
			.await?;
		let sync = header
			.sync
			.ok_or_else(|| tg::error!("the process object push did not produce a sync"))?;
		let task = Task::spawn(move |_| async move {
			let mut stream = std::pin::pin!(stream);
			while let Some(event) = stream.try_next().await? {
				if let tg::progress::Event::Output(_) = event {
					return Ok(());
				}
			}
			Err(tg::error!(
				"the process object push ended without an output"
			))
		});
		let output = PushProcessObjectsOutput { sync, task };

		Ok(output)
	}
}
