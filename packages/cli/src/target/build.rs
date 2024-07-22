use crate::Cli;
use crossterm::style::Stylize as _;
use either::Either;
use futures::TryStreamExt as _;
use itertools::Itertools as _;
use num::ToPrimitive;
use std::{
	fmt::Write as _,
	path::PathBuf,
	sync::{Arc, Mutex, Weak},
};
use tangram_client::{self as tg, handle::Ext as _, Handle as _};

/// Build a target.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Set the arguments.
	#[arg(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub arg: Vec<Vec<String>>,

	/// Whether to check out the output. The output must be an artifact. A path to check out to may be provided.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub checkout: Option<Option<PathBuf>>,

	/// If this flag is set, then the command will exit immediately instead of waiting for the build to finish.
	#[arg(short, long, conflicts_with = "checkout")]
	pub detach: bool,

	/// Set the environment variables.
	#[arg(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub env: Vec<Vec<String>>,

	/// Set the host.
	#[arg(long)]
	pub host: Option<String>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// Whether to suppress printing info and progress.
	#[arg(short, long)]
	pub quiet: bool,

	/// The reference to the target to build.
	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,

	/// Whether to build on a remote.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,

	/// The retry strategy to use.
	#[allow(clippy::option_option)]
	#[arg(long)]
	pub retry: Option<Option<tg::build::Retry>>,

	/// Create a tag for this build.
	#[arg(long)]
	pub tag: Option<tg::Tag>,
}

#[derive(Clone, Debug, derive_more::Unwrap)]
pub enum InnerOutput {
	Detached(tg::build::Id),
	Path(tg::Path),
	Value(tg::Value),
}

struct Progress<H> {
	root: Arc<Node<H>>,
}

struct Node<H> {
	build: tg::Build,
	handle: H,
	state: Mutex<State<H>>,
}

struct State<H> {
	children: Vec<Arc<Node<H>>>,
	log: Option<String>,
	parent: Option<Weak<Node<H>>>,
	status: Option<tg::build::Status>,
	title: String,
}

impl Cli {
	pub async fn command_target_build(&self, args: Args) -> tg::Result<()> {
		// Build.
		let output = self.command_target_build_inner(args).await?;

		// Print the output.
		match output {
			InnerOutput::Detached(build) => {
				println!("{build}");
			},
			InnerOutput::Path(path) => {
				println!("{path}");
			},
			InnerOutput::Value(value) => {
				println!("{value}");
			},
		}

		Ok(())
	}

	pub(crate) async fn command_target_build_inner(&self, args: Args) -> tg::Result<InnerOutput> {
		let handle = self.handle().await?;

		// Get the reference.
		let reference = args
			.reference
			.unwrap_or_else(|| ".?kind=package".parse().unwrap());

		// Get the remote.
		let remote = args
			.remote
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));

		// Get the reference.
		let item = self.get_reference(&reference).await?;

		// Create the target.
		let target = if let Either::Right(tg::Object::Target(target)) = item {
			// If the object is a target, then use it.
			target
		} else {
			// Otherwise, the object must be a file.
			let Either::Right(tg::Object::File(executable)) = item else {
				return Err(tg::error!("expected a file"));
			};

			// Get the target.
			let target = reference
				.uri()
				.fragment()
				.map_or("default", |fragment| fragment);

			// Get the args.
			let mut args_: Vec<tg::Value> = args
				.arg
				.into_iter()
				.map(|arg| {
					arg.into_iter()
						.map(|arg| arg.parse())
						.collect::<Result<tg::value::Array, tg::Error>>()
						.map(Into::into)
				})
				.try_collect()?;
			args_.insert(0, target.into());

			// Get the env.
			let mut env: tg::value::Map = args
				.env
				.into_iter()
				.flatten()
				.map(|env| {
					let map = env
						.parse::<tg::Value>()?
						.try_unwrap_map()
						.map_err(|_| tg::error!("expected a map"))?
						.into_iter();
					Ok::<_, tg::Error>(map)
				})
				.try_fold(tg::value::Map::new(), |mut map, item| {
					map.extend(item?);
					Ok::<_, tg::Error>(map)
				})?;

			// Set the TANGRAM_HOST environment variable if it is not set.
			if !env.contains_key("TANGRAM_HOST") {
				let host = if let Some(host) = args.host {
					host
				} else {
					Cli::host().to_owned()
				};
				env.insert("TANGRAM_HOST".to_owned(), host.to_string().into());
			}

			// Choose the host.
			let host = "js";

			// Create the target.
			tg::target::Builder::new(host)
				.executable(executable)
				.args(args_)
				.env(env)
				.build()
		};

		// Determine the retry.
		let retry = match args.retry {
			None => tg::build::Retry::default(),
			Some(None) => tg::build::Retry::Succeeded,
			Some(Some(retry)) => retry,
		};

		// Print the target.
		eprintln!(
			"{} target {}",
			"info".blue().bold(),
			target.id(&handle).await?
		);

		// Build the target.
		let id = target.id(&handle).await?;
		let arg = tg::target::build::Arg {
			create: true,
			parent: None,
			remote: remote.clone(),
			retry,
		};
		let output = handle.build_target(&id, arg).await?;
		let build = tg::Build::with_id(output.build);

		// Tag the build if requested.
		if let Some(tag) = args.tag {
			let item = Either::Left(build.id().clone());
			let arg = tg::tag::put::Arg {
				force: false,
				item,
				remote: remote.clone(),
			};
			handle.put_tag(&tag, arg).await?;
		}

		// If the detach flag is set, then return the build.
		if args.detach {
			return Ok(InnerOutput::Detached(build.id().clone()));
		}

		// Print the build.
		eprintln!("{} build {}", "info".blue().bold(), build.id());

		// Get the build's status.
		let status = build
			.status(&handle)
			.await?
			.try_next()
			.await?
			.ok_or_else(|| tg::error!("failed to get the status"))?;

		// If the build is finished, then get the build's outcome.
		let outcome = if status == tg::build::Status::Finished {
			let outcome = build
				.outcome(&handle)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the outcome"))?;
			Some(outcome)
		} else {
			None
		};

		// If the build is not finished, then wait for it to finish while showing the TUI if enabled.
		let outcome = if let Some(outcome) = outcome {
			outcome
		} else {
			// Start the progress.
			let progress_task = (!args.quiet).then(|| {
				tokio::spawn({
					let progress = Progress::new(build.clone(), &handle);
					async move {
						progress.run().await;
					}
				})
			});

			// Spawn a task to attempt to cancel the build on the first interrupt signal and exit the process on the second.
			let cancel_task = tokio::spawn({
				let handle = handle.clone();
				let build = build.clone();
				async move {
					tokio::signal::ctrl_c().await.unwrap();
					tokio::spawn(async move {
						let outcome = tg::build::outcome::Data::Canceled;
						let arg = tg::build::finish::Arg { outcome, remote };
						build.finish(&handle, arg).await.ok();
					});
					tokio::signal::ctrl_c().await.unwrap();
					std::process::exit(130);
				}
			});

			// Wait for the build's outcome.
			let outcome = build.outcome(&handle).await;

			// Abort the cancel task.
			cancel_task.abort();

			// Wait for the progress to finish.
			if let Some(progress) = progress_task {
				progress.await.ok();
			}

			outcome.map_err(|source| tg::error!(!source, "failed to get the build outcome"))?
		};

		// Handle a failed build.
		let output = outcome
			.into_result()
			.map_err(|source| tg::error!(!source, "the build failed"))?;

		// Check out the output if requested.
		if let Some(path) = args.checkout {
			// Get the artifact.
			let artifact = tg::Artifact::try_from(output.clone())
				.map_err(|source| tg::error!(!source, "expected the output to be an artifact"))?;

			// If a path was provided, then ensure its parent directory exists and canonicalize it.
			let path = if let Some(path) = path {
				let current = std::env::current_dir()
					.map_err(|source| tg::error!(!source, "failed to get the working directory"))?;
				let path = current.join(&path);
				let parent = path
					.parent()
					.ok_or_else(|| tg::error!("the path must have a parent directory"))?;
				let file_name = path
					.file_name()
					.ok_or_else(|| tg::error!("the path must have a file name"))?;
				tokio::fs::create_dir_all(parent).await.map_err(|source| {
					tg::error!(!source, "failed to create the parent directory")
				})?;
				let path = parent
					.canonicalize()
					.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
					.join(file_name);
				Some(path.try_into()?)
			} else {
				None
			};

			// Check out the artifact.
			let arg = tg::artifact::checkout::Arg {
				bundle: path.is_some(),
				force: false,
				path,
				dependencies: true,
			};
			let output = artifact
				.check_out(&handle, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

			return Ok(InnerOutput::Path(output));
		}

		Ok(InnerOutput::Value(output))
	}
}

impl<H> Progress<H>
where
	H: tg::Handle,
{
	pub fn new(root: tg::Build, handle: &H) -> Self {
		let root = Node::new(root, handle, None);
		Self { root }
	}

	pub async fn run(&self) {
		loop {
			// If the build is finished, then break.
			let status = self.root.state.lock().unwrap().status;
			if matches!(status, Some(tg::build::Status::Finished)) {
				break;
			}

			// Get the spinner
			const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
			let now = std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_millis();
			let position = (now / (1000 / 10)) % 10;
			let position = position.to_usize().unwrap();
			let spinner = SPINNER[position].to_string();

			// Create the tree.
			let tree = self.root.to_tree(&spinner);

			// Save the current position.
			crossterm::execute!(std::io::stdout(), crossterm::cursor::SavePosition,).unwrap();

			// Clear.
			crossterm::execute!(
				std::io::stdout(),
				crossterm::terminal::Clear(crossterm::terminal::ClearType::FromCursorDown),
			)
			.unwrap();

			// Print the tree.
			tree.print();

			// Restore the cursor position.
			crossterm::execute!(std::io::stdout(), crossterm::cursor::RestorePosition).unwrap();

			// Sleep.
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
		}

		// Clear.
		crossterm::execute!(
			std::io::stdout(),
			crossterm::terminal::Clear(crossterm::terminal::ClearType::FromCursorDown),
		)
		.unwrap();
	}
}

impl<H> Node<H>
where
	H: tg::Handle,
{
	fn new(build: tg::Build, handle: &H, parent: Option<&Arc<Self>>) -> Arc<Self> {
		let handle = handle.clone();
		let children = Vec::new();
		let log = None;
		let parent = parent.map(Arc::downgrade);
		let status = None;
		let title = build.id().to_string();

		let state = Mutex::new(State {
			children,
			log,
			parent,
			status,
			title,
		});
		let node: Arc<Node<_>> = Arc::new(Self {
			build,
			handle,
			state,
		});

		// Spawn tasks to update the node.
		tokio::spawn({
			let node = node.clone();
			async move {
				node.children().await;
			}
		});
		tokio::spawn({
			let node = node.clone();
			async move {
				node.log().await;
			}
		});
		tokio::spawn({
			let node = node.clone();
			async move {
				node.status().await;
			}
		});
		tokio::spawn({
			let node = node.clone();
			async move {
				node.title().await.ok();
			}
		});

		node
	}

	fn to_tree(&self, spinner: &str) -> crate::tree::Tree {
		let state = self.state.lock().unwrap();
		let children = state.children.clone();
		let status = state.status;
		let log = state.log.clone();
		let title = state.title.clone();
		drop(state);
		let indicator = match status {
			Some(tg::build::Status::Created) => "⟳".yellow(),
			Some(tg::build::Status::Dequeued) => "•".yellow(),
			Some(tg::build::Status::Started) => spinner.blue(),
			Some(tg::build::Status::Finished) => "✓".green(),
			None => "?".red(),
		};
		let children = log
			.map(|log| crate::tree::Tree {
				title: log,
				children: Vec::new(),
			})
			.into_iter()
			.chain(children.into_iter().map(|child| child.to_tree(spinner)))
			.collect();
		let title = format!("{indicator} {title}");
		crate::tree::Tree { title, children }
	}

	async fn children(self: &Arc<Self>) {
		let arg = tg::build::children::get::Arg::default();
		let Ok(mut children) = self.build.children(&self.handle, arg).await else {
			return;
		};
		while let Ok(Some(child)) = children.try_next().await {
			let node = Self::new(child, &self.handle, Some(self));
			self.state.lock().unwrap().children.push(node);
		}
	}

	async fn log(&self) {
		let arg = tg::build::log::get::Arg {
			position: Some(std::io::SeekFrom::Start(0)),
			..Default::default()
		};
		let Ok(mut log) = self.build.log(&self.handle, arg).await else {
			return;
		};
		let mut buf = String::new();
		while let Ok(Some(chunk)) = log.try_next().await {
			let Ok(string) = std::str::from_utf8(&chunk.bytes) else {
				return;
			};
			buf.push_str(string);
			let last_line = buf.lines().last().unwrap_or(buf.as_str());
			self.state.lock().unwrap().log.replace(last_line.to_owned());
		}
	}

	async fn status(self: &Arc<Self>) {
		// Get the status stream.
		let Ok(mut status) = self.build.status(&self.handle).await else {
			return;
		};

		// Wait for the build to be finished.
		while let Ok(Some(status)) = status.try_next().await {
			self.state.lock().unwrap().status.replace(status);
			if matches!(status, tg::build::Status::Finished) {
				break;
			}
		}

		// Remove the node from its parent.
		if let Some(parent) = self
			.state
			.lock()
			.unwrap()
			.parent
			.as_ref()
			.and_then(Weak::upgrade)
		{
			let mut parent = parent.state.lock().unwrap();
			let index = parent
				.children
				.iter()
				.position(|child| Arc::ptr_eq(self, child))
				.unwrap();
			parent.children.remove(index);
		}
	}

	async fn title(&self) -> tg::Result<()> {
		// let target = self.build.target(&self.handle).await?;
		// let host = target.host(&self.handle).await?;
		// let (package, repository, version) =
		// 	if let Some(executable) = target.executable(&self.handle).await?.as_ref() {
		// 		let object = executable.object(&self.handle).await?;
		// 		let metadata = &object.nodes[object.root].metadata;
		// 		let package = executable.id(&self.handle).await?;
		// 		let repository = metadata
		// 			.get("repository")
		// 			.and_then(|value| value.try_unwrap_string_ref().ok())
		// 			.cloned();
		// 		let version = metadata
		// 			.get("version")
		// 			.and_then(|value| value.try_unwrap_string_ref().ok())
		// 			.cloned();
		// 		(Some(package), repository, version)
		// 	} else {
		// 		(None, None, None)
		// 	};
		// let mut title = String::new();
		// if let (Some(repository), Some(version)) = (repository, version) {
		// 	write!(title, "{repository}@{version}").unwrap();
		// } else if let Some(package) = package {
		// 	write!(title, "{package}").unwrap();
		// } else {
		// 	write!(title, "<unknown>").unwrap();
		// }
		// if host.as_str() == "js" {
		// 	let name = target
		// 		.args(&self.handle)
		// 		.await?
		// 		.first()
		// 		.and_then(|arg| arg.try_unwrap_string_ref().ok())
		// 		.cloned();
		// 	if let Some(name) = name {
		// 		write!(title, ":{name}").unwrap();
		// 	}
		// }
		// self.state.lock().unwrap().title = title;
		// Ok(())
		todo!()
	}
}
