use {
	crate::Cli,
	std::io::{IsTerminal as _, Read as _},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

/// View a node.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Collapse process children when processes finish.
	#[arg(long)]
	pub collapse_process_children: bool,

	/// The maximum depth to render, in levels below the root. Zero renders only the root.
	#[arg(long)]
	pub depth: Option<u32>,

	/// Expand groups.
	#[arg(long)]
	pub expand_groups: bool,

	/// Expand metadata.
	#[arg(long)]
	pub expand_metadata: bool,

	/// Expand objects.
	#[arg(long)]
	pub expand_objects: bool,

	/// Expand organizations.
	#[arg(long)]
	pub expand_organizations: bool,

	/// Expand processes.
	#[arg(long)]
	pub expand_processes: bool,

	/// Expand sandboxes.
	#[arg(long)]
	pub expand_sandboxes: bool,

	/// Expand tags.
	#[arg(long)]
	pub expand_tags: bool,

	/// Expand users.
	#[arg(long)]
	pub expand_users: bool,

	/// Choose the mode, either inline or fullscreen.
	#[arg(default_value = "fullscreen", long)]
	pub mode: Mode,

	#[command(flatten)]
	pub alternate_screen: AlternateScreen,

	/// The reference to view.
	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
	Inline,
	#[default]
	Fullscreen,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct AlternateScreen {
	#[arg(
		default_missing_value = "true",
		hide = true,
		id = "view.alternate_screen.alternate_screen",
		long = "alternate-screen",
		num_args = 0..=1,
		overrides_with = "view.alternate_screen.no_alternate_screen",
		require_equals = true,
	)]
	alternate_screen: Option<bool>,

	#[arg(
		default_missing_value = "true",
		hide = true,
		id = "view.alternate_screen.no_alternate_screen",
		long = "no-alternate-screen",
		num_args = 0..=1,
		overrides_with = "view.alternate_screen.alternate_screen",
		require_equals = true,
	)]
	no_alternate_screen: Option<bool>,
}

impl AlternateScreen {
	pub fn new(flag: bool) -> Self {
		Self {
			alternate_screen: Some(flag),
			no_alternate_screen: None,
		}
	}

	pub fn get(&self) -> Option<bool> {
		self.alternate_screen
			.or(self.no_alternate_screen.map(|v| !v))
	}
}

impl Cli {
	pub async fn command_view(&mut self, args: Args) -> tg::Result<()> {
		// Get the node.
		let output = self.get(&args.reference).await?;
		let client = self.client().await?;
		let root = get_node(&client, output).await?;

		// Create a channel to send the exit signal when stdin finishes reading.
		let (exit_sender, exit_receiver) = tokio::sync::oneshot::channel();
		let _stdin = if std::io::stdin().is_terminal() {
			None
		} else {
			Some(Task::spawn_blocking(move |_| {
				let mut buf = vec![0u8; 1024];
				loop {
					match std::io::stdin().read(&mut buf) {
						Ok(0) | Err(_) => break,
						Ok(_) => {},
					}
				}
				exit_sender.send(()).ok();
			}))
		};

		let alternate_screen = args.alternate_screen.get().unwrap_or(true);
		let mode = args.mode;
		Task::spawn_blocking(move |stop| -> tg::Result<()> {
			let local_set = tokio::task::LocalSet::new();
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.map_err(|error| tg::error!(!error, "failed to create the tokio runtime"))?;
			local_set.block_on(&runtime, async move {
				let options = crate::viewer::Options {
					attached: false,
					collapse_process_children: args.collapse_process_children,
					depth: args.depth,
					expand_groups: args.expand_groups,
					expand_metadata: args.expand_metadata,
					expand_objects: args.expand_objects,
					expand_organizations: args.expand_organizations,
					expand_processes: args.expand_processes,
					expand_sandboxes: args.expand_sandboxes,
					expand_tags: args.expand_tags,
					expand_users: args.expand_users,
					expand_values: matches!(mode, Mode::Inline),
					show_process_commands: true,
				};
				let mut viewer = crate::viewer::Viewer::new(&client, root, exit_receiver, options);
				match mode {
					Mode::Inline => {
						viewer.run_inline(stop, true).await?;
					},
					Mode::Fullscreen => {
						viewer.run_fullscreen(stop, alternate_screen).await?;
					},
				}
				Ok::<_, tg::Error>(())
			})
		})
		.wait()
		.await
		.map_err(|error| tg::error!(!error, "the viewer task panicked"))??;

		Ok(())
	}
}

async fn get_node(
	client: &tg::Client,
	output: tg::get::Output,
) -> tg::Result<tg::Referent<crate::viewer::Item>> {
	let tg::get::Output { location, referent } = output;
	let tg::Referent { node, options } = referent;
	let location = location.map(Into::into);
	let node = match node {
		tg::get::Node::Id(id) if id.kind().is_object() => {
			let object = id.try_into()?;
			let referent = tg::Referent::new(object, options.clone());
			let object = tg::Object::with_referent(referent);
			crate::viewer::Item::Value(object.into())
		},
		tg::get::Node::Id(id) => match id.kind() {
			tg::id::Kind::Group => {
				let id = id.try_into()?;
				let arg = tg::group::get::Arg {
					location: location.clone(),
					..tg::group::get::Arg::default()
				};
				let group = client
					.try_get_group(&tg::group::Selector::Id(id), arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the group"))?;
				crate::viewer::Item::Group(group)
			},
			tg::id::Kind::Organization => {
				let id = id.try_into()?;
				let arg = tg::organization::get::Arg {
					location: location.clone(),
					..tg::organization::get::Arg::default()
				};
				let organization = client
					.try_get_organization(&tg::organization::Selector::Id(id), arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the organization"))?;
				crate::viewer::Item::Organization(organization)
			},
			tg::id::Kind::Process => {
				let id: tg::process::Id = id.try_into()?;
				let arg = tg::process::get::Arg {
					location: location.clone(),
					token: options.token.clone(),
					..tg::process::get::Arg::default()
				};
				let output = client
					.try_get_process(&id, arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				let location = output.location.map(Into::into);
				let process = tg::Process::new(
					id,
					tg::process::Options {
						location,
						token: options.token.clone(),
						..tg::process::Options::default()
					},
				);
				crate::viewer::Item::Process(process)
			},
			tg::id::Kind::Sandbox => {
				let id = id.try_into()?;
				let arg = tg::sandbox::get::Arg {
					location: location.clone(),
					..tg::sandbox::get::Arg::default()
				};
				let output = client
					.try_get_sandbox(&id, arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the sandbox"))?;
				let location = output.location.clone().map(Into::into);
				let token = options.token.clone().or_else(|| output.token.clone());
				let sandbox = tg::Sandbox::new(
					id,
					tg::sandbox::Options {
						location,
						state: Some(output),
						token,
					},
				);
				crate::viewer::Item::Sandbox(sandbox)
			},
			tg::id::Kind::Tag => {
				let id = id.try_into()?;
				let arg = tg::tag::get::Arg {
					location: location.clone(),
					..tg::tag::get::Arg::default()
				};
				let tag = client
					.try_get_tag(&tg::tag::Selector::Id(id), arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the tag"))?
					.into();
				crate::viewer::Item::Tag(tag)
			},
			tg::id::Kind::User => {
				let id = id.try_into()?;
				let arg = tg::user::get::Arg {
					location,
					..tg::user::get::Arg::default()
				};
				let user = client
					.try_get_user(&tg::user::Selector::Id(id), arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the user"))?;
				crate::viewer::Item::User(user)
			},
			tg::id::Kind::Blob
			| tg::id::Kind::Command
			| tg::id::Kind::Directory
			| tg::id::Kind::Error
			| tg::id::Kind::File
			| tg::id::Kind::Graph
			| tg::id::Kind::Symlink => unreachable!(),
			tg::id::Kind::Runner | tg::id::Kind::Scheduler => {
				return Err(tg::error!(%id, "cannot view the node"));
			},
			_ => {
				return Err(tg::error!(%id, "cannot view the node"));
			},
		},
		tg::get::Node::Pointer(pointer) => {
			let graph = pointer
				.graph
				.clone()
				.ok_or_else(|| tg::error!("expected a graph"))?;
			let referent = tg::Referent::new(graph, options.clone());
			let graph = tg::Graph::with_referent(referent);
			crate::viewer::Item::Value(tg::Object::from(graph).into())
		},
	};
	let referent = tg::Referent { node, options };

	Ok(referent)
}
