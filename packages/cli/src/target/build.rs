use crate::Cli;
use crossterm::style::Stylize as _;
use either::Either;
use futures::TryStreamExt as _;
use itertools::Itertools as _;
use num::ToPrimitive;
use std::{
	path::PathBuf,
	sync::{Arc, Mutex, Weak},
};
use tangram_client as tg;

/// Build a target.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, then the command will exit immediately instead of waiting for the build to finish.
	#[arg(short, long, conflicts_with = "checkout")]
	pub detach: bool,

	#[command(flatten)]
	pub inner: InnerArgs,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct InnerArgs {
	/// Set the arguments.
	#[arg(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub arg: Vec<Vec<String>>,

	/// Whether to check out the output. The output must be an artifact. A path to check out to may be provided.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub checkout: Option<Option<PathBuf>>,

	/// Set the environment variables.
	#[arg(short, long, num_args = 1.., action = clap::ArgAction::Append)]
	pub env: Vec<Vec<String>>,

	/// Set the host.
	#[arg(long)]
	pub host: Option<String>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// Quiet output.
	#[arg(short, long)]
	pub quiet: bool,

	/// Whether to build on a remote.
	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,

	/// The retry strategy to use.
	#[allow(clippy::option_option)]
	#[arg(long)]
	pub retry: Option<Option<tg::build::Retry>>,

	/// Create a root for this build. If a name is not provided, the package's name will be used.
	#[arg(long)]
	pub root: Option<String>,

	/// The specifier of the target to build.
	#[arg(long, conflicts_with_all = ["target", "arg_"])]
	pub specifier: Option<Specifier>,

	/// The target to build.
	#[arg(long, conflicts_with_all = ["specifier", "arg_"])]
	pub target: Option<tg::target::Id>,

	/// The target to build.
	#[arg(conflicts_with_all = ["specifier", "target"])]
	pub arg_: Option<Arg>,
}

#[derive(Clone, Debug)]
pub enum Arg {
	Specifier(Specifier),
	Target(tg::target::Id),
}

#[derive(Clone, Debug)]
pub struct Specifier {
	dependency: tg::Dependency,
	target: String,
}

#[derive(Clone, Debug, derive_more::Unwrap)]
pub enum InnerOutput {
	Path(tg::Path),
	Value(tg::Value),
}

struct ProgressTree<H> {
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
		let output = self
			.command_target_build_inner(args.inner, args.detach)
			.await?;

		// Handle the output.
		if let Some(output) = output {
			match output {
				InnerOutput::Path(path) => {
					println!("{path}");
				},
				InnerOutput::Value(value) => {
					println!("{value}");
				},
			}
		}

		Ok(())
	}

	pub(crate) async fn command_target_build_inner(
		&self,
		args: InnerArgs,
		detach: bool,
	) -> tg::Result<Option<InnerOutput>> {
		let client = self.client().await?;

		// Get the arg.
		let arg = if let Some(specifier) = args.specifier {
			Arg::Specifier(specifier)
		} else if let Some(target) = args.target {
			Arg::Target(target)
		} else {
			args.arg_.unwrap_or_default()
		};

		// Create the target.
		let target = match arg {
			Arg::Specifier(mut specifier) => {
				// Canonicalize the path.
				if let Some(path) = specifier.dependency.path.as_mut() {
					*path = tokio::fs::canonicalize(&path)
						.await
						.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
						.try_into()?;
				}

				// Create the package.
				let (package, lock) =
					tg::package::get_with_lock(&client, &specifier.dependency, args.locked).await?;

				// Create the target.
				let host = "js";
				let path = tg::package::get_root_module_path(&client, &package).await?;
				let executable = tg::Symlink::new(Some(package), Some(path));
				let mut args_: tg::value::Array = args
					.arg
					.into_iter()
					.map(|arg| {
						arg.into_iter()
							.map(|arg| arg.parse())
							.collect::<Result<tg::value::Array, tg::Error>>()
							.map(Into::into)
					})
					.try_collect()?;
				args_.insert(0, specifier.target.into());
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
				if !env.contains_key("TANGRAM_HOST") {
					let host = if let Some(host) = args.host {
						host
					} else {
						Cli::host().to_owned()
					};
					env.insert("TANGRAM_HOST".to_owned(), host.to_string().into());
				}
				tg::target::Builder::new(host)
					.executable(tg::Artifact::from(executable))
					.args(args_)
					.env(env)
					.lock(lock)
					.build()
			},

			Arg::Target(target) => tg::Target::with_id(target),
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
			target.id(&client).await?
		);

		// Build the target.
		let id = target.id(&client).await?;
		let remote = args
			.remote
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::target::build::Arg {
			create: true,
			parent: None,
			remote: remote.clone(),
			retry,
		};
		let output = client.build_target(&id, arg).await?;
		let build = tg::Build::with_id(output.build);

		// Add the root if requested.
		if let Some(name) = args.root {
			let item = Either::Left(build.id().clone());
			let arg = tg::root::put::Arg { item };
			client.put_root(&name, arg).await?;
		}

		// If the detach flag is set, then print the build and exit.
		if detach {
			println!("{}", build.id());
			return Ok(None);
		}

		// Print the build.
		eprintln!("{} build {}", "info".blue().bold(), build.id());

		// Get the build's status.
		let status = build
			.status(&client)
			.await?
			.try_next()
			.await?
			.ok_or_else(|| tg::error!("failed to get the status"))?;

		// If the build is finished, then get the build's outcome.
		let outcome = if status == tg::build::Status::Finished {
			let outcome = build
				.outcome(&client)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the outcome"))?;
			Some(outcome)
		} else {
			None
		};

		// If the outcome is not immediatey available, then wait for it while showing the TUI if enabled.
		let outcome = if let Some(outcome) = outcome {
			outcome
		} else {
			// Start the TUI.
			let progress = (!args.quiet).then(|| {
				tokio::spawn({
					let progress = ProgressTree::new(build.clone(), &client);
					async move {
						progress.display().await;
					}
				})
			});

			// Spawn a task to attempt to cancel the build on the first interrupt signal and exit the process on the second.
			tokio::spawn({
				let handle = client.clone();
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
			let outcome = build.outcome(&client).await;

			// Stop the TUI.
			if let Some(progress) = progress {
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
				references: true,
			};
			let output = artifact
				.check_out(&client, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;

			return Ok(Some(InnerOutput::Path(output)));
		}

		Ok(Some(InnerOutput::Value(output)))
	}
}

impl Default for InnerArgs {
	fn default() -> Self {
		Self {
			arg: vec![],
			checkout: None,
			env: vec![],
			host: None,
			locked: false,
			quiet: false,
			remote: None,
			retry: None,
			root: None,
			specifier: None,
			target: None,
			arg_: None,
		}
	}
}

impl Default for Arg {
	fn default() -> Self {
		Self::Specifier(Specifier::default())
	}
}

impl std::str::FromStr for Arg {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(target) = s.parse() {
			return Ok(Arg::Target(target));
		}
		if let Ok(specifier) = s.parse() {
			return Ok(Arg::Specifier(specifier));
		}
		Err(tg::error!(%s, "expected a target specifier or target ID"))
	}
}

impl Default for Specifier {
	fn default() -> Self {
		Self {
			dependency: ".".parse().unwrap(),
			target: "default".to_owned(),
		}
	}
}

impl std::str::FromStr for Specifier {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		// Split the string by the colon character.
		let (package, target) = s.split_once(':').unwrap_or((s, ""));

		// Get the dependency.
		let dependency = if package.is_empty() {
			".".parse().unwrap()
		} else {
			package.parse()?
		};

		// Get the target.
		let target = if target.is_empty() {
			"default".to_owned()
		} else {
			target.to_owned()
		};

		Ok(Self { dependency, target })
	}
}

impl<H> ProgressTree<H>
where
	H: tg::Handle,
{
	pub fn new(root: tg::Build, handle: &H) -> Self {
		let root = Node::new(root, handle, None);
		Self { root }
	}

	pub async fn display(&self) {
		const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
		loop {
			let status = self.root.state.lock().unwrap().status.clone();
			if matches!(status, Some(tg::build::Status::Finished)) {
				break;
			}
			// Save the current position.
			crossterm::execute!(std::io::stdout(), crossterm::cursor::SavePosition,).unwrap();

			// Get the spinner
			let now = std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_millis();
			let position = (now / (1000 / 10)) % 10;
			let position = position.to_usize().unwrap();
			let spinner = SPINNER[position].to_string();

			// Construct the tree.
			let tree = self.root.into_tree(&spinner);

			// Clear the terminal here to avoid flicker.
			crossterm::execute!(
				std::io::stdout(),
				crossterm::terminal::Clear(crossterm::terminal::ClearType::FromCursorDown),
			)
			.unwrap();
			tree.print();

			// Wait for the next frame and restore.
			tokio::time::sleep(std::time::Duration::from_millis(10)).await;
			crossterm::execute!(std::io::stdout(), crossterm::cursor::RestorePosition,).unwrap();
		}

		crossterm::execute!(
			std::io::stdout(),
			crossterm::cursor::RestorePosition,
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
		let parent = parent.map(|parent| Arc::downgrade(parent));
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

		// Spawn tasks to asynchronously update state.
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
				node.title().await;
			}
		});

		node
	}

	fn into_tree(&self, spinner: &str) -> crate::tree::Tree {
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
			.chain(children.into_iter().map(|child| child.into_tree(spinner)))
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
		// Create the status stream.
		let Ok(mut status) = self.build.status(&self.handle).await else {
			return;
		};

		// Drain the stream.
		while let Ok(Some(status)) = status.try_next().await {
			self.state.lock().unwrap().status.replace(status.clone());
			if matches!(status, tg::build::Status::Finished) {
				break;
			}
		}

		// If we reach the end of the stream it means the build has finished, and we can remove it from its parent.
		let Some(parent) = self
			.state
			.lock()
			.unwrap()
			.parent
			.as_ref()
			.and_then(Weak::upgrade)
		else {
			return;
		};
		let mut parent = parent.state.lock().unwrap();
		let index = parent
			.children
			.iter()
			.position(|child| Arc::ptr_eq(self, child))
			.unwrap();
		parent.children.remove(index);
	}

	async fn title(&self) {
		let Ok(target) = self.build.target(&self.handle).await else {
			return;
		};
		let Ok(Some(package)) = target.package(&self.handle).await else {
			return;
		};
		let Ok(metadata) = tg::package::get_metadata(&self.handle, &package).await else {
			return;
		};
		let Ok(host) = target.host(&self.handle).await else {
			return;
		};
		let package_name = metadata.name.as_deref().unwrap_or("<unknown>");
		let package_version = metadata.version.as_deref().unwrap_or("<unknown>");
		let target_name = if host.as_str() == "js" {
			let Some(name) = target.args(&self.handle).await.ok().and_then(|args| {
				args.get(0)
					.and_then(|arg| arg.clone().try_unwrap_string().ok())
			}) else {
				return;
			};
			name
		} else {
			let Ok(target) = target.id(&self.handle).await else {
				return;
			};
			target.to_string()
		};
		let title = format!("{package_name}@{package_version} {target_name}");
		self.state.lock().unwrap().title = title;
	}
}
