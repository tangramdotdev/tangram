use {
	clap::{CommandFactory as _, FromArgMatches as _},
	futures::FutureExt as _,
	num::ToPrimitive as _,
	std::{os::unix::process::CommandExt as _, path::PathBuf, time::Duration},
	tangram_client::{Client, prelude::*},
	tangram_server::Owned as Server,
	tangram_uri::Uri,
	tracing_subscriber::prelude::*,
};

mod archive;
mod build;
mod builtin;
mod bundle;
mod cache;
mod check;
mod checkin;
mod checkout;
mod checksum;
mod children;
mod clean;
mod completion;
mod compress;
mod decompress;
mod document;
mod download;
mod error;
mod extract;
mod format;
mod get;
mod health;
mod id;
mod index;
mod init;
#[cfg(feature = "js")]
mod js;
mod lsp;
mod metadata;
mod new;
mod object;
mod outdated;
mod print;
mod process;
mod progress;
mod publish;
mod pull;
mod push;
mod put;
mod read;
mod remote;
mod run;
mod sandbox;
mod server;
mod session;
mod tag;
mod tangram;
mod tree;
mod update;
mod util;
mod view;
mod viewer;
mod watch;
mod write;

pub use self::config::Config;

pub mod config;

pub struct Cli {
	args: Args,
	config: Option<Config>,
	exit: Option<u8>,
	handle: Option<tg::Either<Client, Server>>,
	matches: clap::ArgMatches,
	mode: Mode,
}

#[derive(Clone, Debug, clap::Parser)]
#[command(
	about = "Tangram is a build system and package manager.",
	arg_required_else_help = true,
	before_help = before_help(),
	disable_help_subcommand = true,
	name = "tangram",
	version = version(),
)]
struct Args {
	#[command(subcommand)]
	command: Command,

	/// The path to the config file.
	#[arg(env = "TANGRAM_CONFIG", long, short)]
	config: Option<PathBuf>,

	/// Override the `directory` key in the config.
	#[arg(env = "TANGRAM_DIRECTORY", long, short)]
	directory: Option<PathBuf>,

	/// The mode.
	#[arg(env = "TANGRAM_MODE", long, short)]
	mode: Option<Mode>,

	/// Override the `remotes` key in the config.
	#[arg(long, conflicts_with = "remotes")]
	no_remotes: bool,

	/// Override the `remotes` key in the config.
	#[arg(long, short, value_delimiter = ',', conflicts_with = "no_remotes")]
	remotes: Option<Vec<String>>,

	/// Whether to show progress and other helpful information.
	#[arg(long, short)]
	quiet: bool,

	#[arg(env = "TANGRAM_TOKEN")]
	token: Option<String>,

	/// Override the tracing filter.
	#[arg(env = "TANGRAM_TRACING", long)]
	tracing: Option<String>,

	/// Override the `url` key in the config.
	#[arg(env = "TANGRAM_URL", long, short)]
	url: Option<Uri>,
}

fn before_help() -> String {
	let version = version();
	let logo = include_str!("tangram.ascii").trim_end();
	format!("Tangram {version}\n\n{logo}")
}

fn version() -> String {
	let mut version = env!("CARGO_PKG_VERSION").to_owned();
	if let Some(commit) = option_env!("TANGRAM_CLI_COMMIT_HASH") {
		version.push('+');
		version.push_str(commit);
	}
	version
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
	#[default]
	Auto,
	Client,
	Server,
}

#[derive(Clone, Debug, clap::Subcommand)]
enum Command {
	Archive(self::archive::Args),

	#[command(alias = "b")]
	Build(self::build::Args),

	#[command(hide = true)]
	Builtin(self::builtin::Args),

	Bundle(self::bundle::Args),

	Cache(self::cache::Args),

	Cancel(self::process::cancel::Args),

	Check(self::check::Args),

	#[command(alias = "ci")]
	Checkin(self::checkin::Args),

	#[command(alias = "co")]
	Checkout(self::checkout::Args),

	Checksum(self::checksum::Args),

	Children(self::children::Args),

	Clean(self::clean::Args),

	Completion(self::completion::Args),

	Compress(self::compress::Args),

	Decompress(self::decompress::Args),

	#[command(alias = "doc")]
	Document(self::document::Args),

	Download(self::download::Args),

	Extract(self::extract::Args),

	Format(self::format::Args),

	Get(self::get::Args),

	Health(self::health::Args),

	#[command(hide = true)]
	Id(self::id::Args),

	Index(self::index::Args),

	Init(self::init::Args),

	#[cfg(feature = "js")]
	#[command(hide = true)]
	Js(self::js::Args),

	#[command(alias = "ls")]
	List(self::tag::list::Args),

	Log(self::process::log::Args),

	Lsp(self::lsp::Args),

	Metadata(self::metadata::Args),

	New(self::new::Args),

	Object(self::object::Args),

	Outdated(self::outdated::Args),

	Output(self::process::output::Args),

	Process(self::process::Args),

	#[command(alias = "ps")]
	Processes(self::process::list::Args),

	Publish(self::publish::Args),

	Pull(self::pull::Args),

	Push(self::push::Args),

	Put(self::put::Args),

	#[command(alias = "cat")]
	Read(self::read::Args),

	Remote(self::remote::Args),

	#[command(alias = "r")]
	Run(self::run::Args),

	#[command(hide = true)]
	Sandbox(self::sandbox::Args),

	#[command(name = "self")]
	Self_(self::tangram::Args),

	Serve(self::server::run::Args),

	Server(self::server::Args),

	#[command(hide = true)]
	Session(self::session::Args),

	#[command(alias = "kill")]
	Signal(self::process::signal::Args),

	Spawn(self::process::spawn::Args),

	Status(self::process::status::Args),

	Tag(self::tag::Args),

	#[command(hide = true)]
	Tree(self::tree::Args),

	Update(self::update::Args),

	View(self::view::Args),

	Wait(self::process::wait::Args),

	Watch(self::watch::Args),

	Write(self::write::Args),
}

fn main() -> std::process::ExitCode {
	// Parse the args.
	let matches = Args::command().get_matches();
	let mut args = Args::from_arg_matches(&matches).unwrap();

	// Handle internal commands.
	match args.command {
		Command::Builtin(args) => {
			return Cli::command_builtin(&matches, args);
		},
		#[cfg(feature = "js")]
		Command::Js(args) => {
			#[cfg(feature = "v8")]
			Cli::initialize_v8(0);
			return Cli::command_js(&matches, args);
		},
		Command::Sandbox(args) => {
			return Cli::command_sandbox(args);
		},
		Command::Session(args) => {
			return Cli::command_session(args);
		},
		_ => (),
	}

	// Override args from the serve args.
	if let Command::Serve(serve_args)
	| Command::Server(self::server::Args {
		command: self::server::Command::Run(serve_args),
		..
	}) = &args.command
	{
		if let Some(config) = serve_args.config.clone() {
			args.config = Some(config);
		}
		if let Some(directory) = serve_args.directory.clone() {
			args.directory = Some(directory);
		}
		if serve_args.no_remotes {
			args.no_remotes = true;
		}
		if let Some(remotes) = serve_args.remotes.clone() {
			args.remotes = Some(remotes);
		}
		if let Some(token) = serve_args.token.clone() {
			args.token = Some(token);
		}
		if let Some(tracing) = serve_args.tracing.clone() {
			args.tracing = Some(tracing);
		}
		if let Some(url) = serve_args.url.clone() {
			args.url = Some(url);
		}
	}

	// Read the config.
	let config = match Cli::read_config(args.config.clone()) {
		Ok(config) => config,
		Err(error) => {
			Cli::print_error_message("an error occurred");
			Cli::print_error_basic(tg::Referent::with_item(error));
			return std::process::ExitCode::FAILURE;
		},
	};

	// Get the mode.
	let mode = match &args {
		// If the command is `tg serve` or `tg server run`, then set the mode to `server`.
		Args {
			command:
				Command::Serve(_)
				| Command::Server(self::server::Args {
					command: self::server::Command::Run(_),
					..
				}),
			..
		} => Mode::Server,

		// If the command is anything else under `tg server`, then set the mode to `client`.
		Args {
			command: Command::Server(_),
			..
		} => Mode::Client,

		_ => args.mode.unwrap_or_default(),
	};

	// Set the file descriptor limit.
	if matches!(mode, Mode::Server) {
		Cli::set_file_descriptor_limit()
			.inspect_err(|_| {
				if !args.quiet {
					Cli::print_warning_message("failed to set the file descriptor limit");
				}
			})
			.ok();
	}

	// Initialize FoundationDB.
	#[cfg(feature = "foundationdb")]
	let _fdb = if matches!(mode, Mode::Server) {
		Some(unsafe { foundationdb::boot() })
	} else {
		None
	};

	// Initialize miette.
	Cli::initialize_miette();

	// Initialize tracing.
	Cli::initialize_tracing(config.as_ref(), args.tracing.as_ref());

	// Initialize V8.
	#[cfg(feature = "v8")]
	if matches!(mode, Mode::Server) {
		let thread_pool_size = config
			.as_ref()
			.and_then(|config| config.v8_thread_pool_size)
			.unwrap_or(0);
		Cli::initialize_v8(thread_pool_size);
	}

	// Create the tokio runtime.
	let runtime = if config
		.as_ref()
		.is_some_and(|config| config.tokio_single_threaded)
	{
		tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.unwrap()
	} else {
		tokio::runtime::Builder::new_multi_thread()
			.enable_all()
			.build()
			.unwrap()
	};

	// Create the CLI.
	let mut cli = Cli {
		args,
		config,
		exit: None,
		handle: None,
		matches,
		mode,
	};

	// Run the command.
	let result = runtime.block_on(cli.command(cli.args.clone()).boxed());

	// Handle the result.
	let exit = match result {
		Ok(()) => cli.exit.unwrap_or_default().into(),
		Err(error) => {
			Cli::print_error_message("an error occurred");
			runtime.block_on(async {
				let error = tg::Referent::with_item(error);
				cli.print_error(error).await;
			});
			cli.exit.map_or(std::process::ExitCode::FAILURE, Into::into)
		},
	};

	// Drop the handle.
	runtime.block_on(async {
		let handle = cli.handle.take();
		match handle {
			Some(tg::Either::Left(client)) => {
				client.disconnect().await;
			},
			Some(tg::Either::Right(server)) => {
				server.stop();
				server.wait().await.unwrap();
			},
			None => (),
		}
	});

	// Drop the runtime.
	drop(runtime);

	exit
}

impl Cli {
	async fn handle(&mut self) -> tg::Result<tg::Either<Client, Server>> {
		// If the handle has already been created, then return it.
		if let Some(handle) = self.handle.clone() {
			return Ok(handle);
		}

		// Create the handle.
		let handle = match self.mode {
			Mode::Auto => tg::Either::Left(self.auto().boxed().await?),
			Mode::Client => tg::Either::Left(self.client().boxed().await?),
			Mode::Server => tg::Either::Right(self.server().boxed().await?),
		};

		// Get the health and print diagnostics.
		let health = handle
			.health()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the health"))?;
		if !self.args.quiet {
			for diagnostic in health.diagnostics {
				let diagnostic: tg::Diagnostic = diagnostic.try_into()?;
				let diagnostic = tg::Referent::with_item(diagnostic);
				self.print_diagnostic(diagnostic).await;
			}
		}

		// Set the handle.
		self.handle.replace(handle.clone());

		Ok(handle)
	}

	async fn auto(&self) -> tg::Result<Client> {
		// Get the url.
		let url = self
			.args
			.url
			.clone()
			.or(self
				.config
				.as_ref()
				.and_then(|config| config.server.http.as_ref())
				.and_then(|config| config.url.clone()))
			.unwrap_or_else(|| {
				let path = self.directory_path().join("socket");
				let path = path.to_str().unwrap();
				tangram_uri::Uri::builder()
					.scheme("http+unix")
					.authority(path)
					.path("")
					.build()
					.unwrap()
			});

		// Get the token.
		let token = self.args.token.clone();

		// Create the client.
		let client = tg::Client::new(url, Some(version()), token);

		// Attempt to connect to the server.
		let mut connected = client.connect().await.is_ok();

		// If the client is not connected and the URL is local, then start the server and attempt to connect.
		let local = client.url().scheme() == Some("http+unix")
			|| matches!(client.url().host_raw(), Some("localhost" | "0.0.0.0"));
		if !connected && local {
			// Start the server.
			self.start_server().await?;

			// Try to connect for up to one second. If the client is still not connected, then return an error.
			for duration in [10, 20, 30, 50, 100, 300, 500] {
				connected = client.connect().await.is_ok();
				if connected {
					break;
				}
				tokio::time::sleep(Duration::from_millis(duration)).await;
			}
			if !connected {
				return Err(tg::error!(url = %client.url(), "failed to connect to the server"));
			}
		}

		// If the URL is local and the server's version is different from the client, then disconnect and restart the server.
		'a: {
			if !local {
				break 'a;
			}

			let health = client
				.health()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the health"))?;
			let Some(server_version) = &health.version else {
				break 'a;
			};

			if &version() == server_version {
				break 'a;
			}

			// Disconnect.
			client.disconnect().await;

			// Stop the server.
			self.stop_server().await?;

			// Start the server.
			self.start_server().await?;

			// Try to connect for up to one second. If the client is still not connected, then return an error.
			for duration in [10, 20, 30, 50, 100, 300, 500] {
				connected = client.connect().await.is_ok();
				if connected {
					break;
				}
				tokio::time::sleep(Duration::from_millis(duration)).await;
			}
			if !connected {
				return Err(tg::error!(url = %client.url(), "failed to connect to the server"));
			}
		}

		Ok(client)
	}

	async fn client(&self) -> tg::Result<Client> {
		// Get the url.
		let url = self
			.args
			.url
			.clone()
			.or(self
				.config
				.as_ref()
				.and_then(|config| config.server.http.as_ref())
				.and_then(|config| config.url.clone()))
			.unwrap_or_else(|| {
				let path = self.directory_path().join("socket");
				let path = path.to_str().unwrap();
				tangram_uri::Uri::builder()
					.scheme("http+unix")
					.authority(path)
					.path("")
					.build()
					.unwrap()
			});

		// Get the token.
		let token = self.args.token.clone();

		// Create the client.
		let client = tg::Client::new(url, Some(version()), token);

		// Try to connect. If the client is not connected, then return an error.
		let connected = client.connect().await.is_ok();
		if !connected {
			return Err(tg::error!(url = %client.url(), "failed to connect to the server"));
		}

		Ok(client)
	}

	async fn server(&self) -> tg::Result<Server> {
		// Get the server config.
		let mut config = self
			.config
			.as_ref()
			.map(|config| config.server.clone())
			.unwrap_or_default();

		// Get the directory.
		let directory = self.directory_path();

		// Set the directory and version.
		config.directory = Some(directory.clone());
		config.version = Some(version());

		// Set the URL.
		if let Some(url) = &self.args.url {
			config.http = Some(tangram_server::config::Http {
				url: Some(url.clone()),
			});
		}

		// Set the remotes.
		if self.args.no_remotes {
			config.remotes = Some(vec![]);
		} else if let Some(remotes) = &self.args.remotes {
			config.remotes = Some(
				remotes
					.iter()
					.filter_map(|remote_str| {
						let (name, url_str) = remote_str.split_once('=')?;
						let url = url_str.parse().ok()?;
						Some(tangram_server::config::Remote {
							name: name.to_owned(),
							url,
							token: None,
						})
					})
					.collect(),
			);
		}

		// Start the server.
		let server = tangram_server::Server::start(config)
			.await
			.map_err(|source| tg::error!(!source, "failed to start the server"))?;

		Ok(server)
	}

	/// Start the server.
	async fn start_server(&self) -> tg::Result<()> {
		// Ensure the directory exists.
		let directory = self.directory_path();
		tokio::fs::create_dir_all(&directory)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;

		// Get the log file path.
		let log_path = directory.join("log");

		// Create files for stdout and stderr.
		let stdout = tokio::fs::OpenOptions::new()
			.create(true)
			.write(true)
			.truncate(true)
			.open(&log_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the log file"))?
			.into_std()
			.await;
		let stderr = tokio::fs::OpenOptions::new()
			.create(true)
			.write(true)
			.truncate(true)
			.open(&log_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the log file"))?
			.into_std()
			.await;

		// Get the path to the current executable.
		let executable = tangram_util::env::current_exe()
			.map_err(|source| tg::error!(!source, "failed to get the current executable path"))?;

		// Spawn the server.
		let mut command = std::process::Command::new(executable);
		let mut args = vec![];
		if let Some(config) = &self.args.config {
			args.push("-c".to_owned());
			args.push(config.to_string_lossy().into_owned());
		}
		if let Some(directory) = &self.args.directory {
			args.push("-d".to_owned());
			args.push(directory.to_string_lossy().into_owned());
		}
		if self.args.no_remotes {
			args.push("--no-remotes".to_owned());
		} else if let Some(remotes) = &self.args.remotes {
			args.push("-r".to_owned());
			args.push(remotes.join(","));
		}
		if let Some(url) = &self.args.url {
			args.push("-u".to_owned());
			args.push(url.to_string());
		}
		if let Some(tracing) = &self.args.tracing {
			args.push("--tracing".to_owned());
			args.push(tracing.clone());
		}
		args.push("serve".to_owned());
		command
			.args(args)
			.current_dir(PathBuf::from(std::env::var("HOME").unwrap()))
			.stdin(std::process::Stdio::null())
			.stdout(stdout)
			.stderr(stderr);
		unsafe {
			command.pre_exec(|| {
				let id = libc::setsid();
				if id < 0 {
					return Err(std::io::Error::last_os_error());
				}
				Ok(())
			});
		}

		command
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the server"))?;

		Ok(())
	}

	/// Stop the server.
	async fn stop_server(&self) -> tg::Result<()> {
		// Read the PID from the lock file.
		let lock_path = self.directory_path().join("lock");
		let pid = tokio::fs::read_to_string(&lock_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the pid from the lock file"))?
			.parse::<u32>()
			.map_err(|source| tg::error!(!source, "invalid lock file"))?;

		// Send SIGINT to the server.
		let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGINT) };
		if ret != 0 {
			return Err(tg::error!("failed to send SIGINT to the server"));
		}

		// Wait up to one second for the server to exit.
		for duration in [10, 20, 30, 50, 100, 300, 500] {
			// Kill the server. If the server has exited, then return.
			let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGINT) };
			if ret != 0 {
				let error = std::io::Error::last_os_error();
				if error.raw_os_error() == Some(libc::ESRCH) {
					return Ok(());
				}
				return Err(tg::error!(!error, "failed to stop the server"));
			}

			// Otherwise, sleep.
			tokio::time::sleep(Duration::from_millis(duration)).await;
		}

		// If the server has still not exited, then send SIGTERM to the server.
		let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGTERM) };
		if ret != 0 {
			return Err(tg::error!("failed to send SIGTERM to the server"));
		}

		// Wait up to one second for the server to exit.
		for duration in [10, 20, 30, 50, 100, 300, 500] {
			// Kill the server. If the server has exited, then return.
			let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGTERM) };
			if ret != 0 {
				let error = std::io::Error::last_os_error();
				if error.raw_os_error() == Some(libc::ESRCH) {
					return Ok(());
				}
				return Err(tg::error!(!error, "failed to stop the server"));
			}

			// Otherwise, sleep.
			tokio::time::sleep(Duration::from_millis(duration)).await;
		}

		// If the server has still not exited, then return an error.
		Err(tg::error!("failed to terminate the server"))
	}

	// Run the command.
	async fn command(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			#[cfg(feature = "js")]
			Command::Js(_) => {
				unreachable!()
			},
			Command::Builtin(_) | Command::Sandbox(_) | Command::Session(_) => {
				unreachable!()
			},
			Command::Archive(args) => self.command_archive(args).boxed(),
			Command::Build(args) => self.command_build(args).boxed(),
			Command::Bundle(args) => self.command_bundle(args).boxed(),
			Command::Cache(args) => self.command_cache(args).boxed(),
			Command::Cancel(args) => self.command_process_cancel(args).boxed(),
			Command::Check(args) => self.command_check(args).boxed(),
			Command::Checkin(args) => self.command_checkin(args).boxed(),
			Command::Checkout(args) => self.command_checkout(args).boxed(),
			Command::Checksum(args) => self.command_checksum(args).boxed(),
			Command::Children(args) => self.command_children(args).boxed(),
			Command::Clean(args) => self.command_clean(args).boxed(),
			Command::Completion(args) => self.command_completion(args).boxed(),
			Command::Compress(args) => self.command_compress(args).boxed(),
			Command::Decompress(args) => self.command_decompress(args).boxed(),
			Command::Document(args) => self.command_document(args).boxed(),
			Command::Download(args) => self.command_download(args).boxed(),
			Command::Extract(args) => self.command_extract(args).boxed(),
			Command::Format(args) => self.command_format(args).boxed(),
			Command::Get(args) => self.command_get(args).boxed(),
			Command::Health(args) => self.command_health(args).boxed(),
			Command::Id(args) => self.command_id(args).boxed(),
			Command::Index(args) => self.command_index(args).boxed(),
			Command::Init(args) => self.command_init(args).boxed(),
			Command::List(args) => self.command_tag_list(args).boxed(),
			Command::Log(args) => self.command_process_log(args).boxed(),
			Command::Lsp(args) => self.command_lsp(args).boxed(),
			Command::Metadata(args) => self.command_metadata(args).boxed(),
			Command::New(args) => self.command_new(args).boxed(),
			Command::Object(args) => self.command_object(args).boxed(),
			Command::Outdated(args) => self.command_outdated(args).boxed(),
			Command::Output(args) => self.command_process_output(args).boxed(),
			Command::Process(args) => self.command_process(args).boxed(),
			Command::Processes(args) => self.command_process_list(args).boxed(),
			Command::Publish(args) => self.command_publish(args).boxed(),
			Command::Pull(args) => self.command_pull(args).boxed(),
			Command::Push(args) => self.command_push(args).boxed(),
			Command::Put(args) => self.command_put(args).boxed(),
			Command::Read(args) => self.command_read(args).boxed(),
			Command::Remote(args) => self.command_remote(args).boxed(),
			Command::Run(args) => self.command_run(args).boxed(),
			Command::Self_(args) => self.command_tangram(args).boxed(),
			Command::Serve(args) => self.command_server_run(args).boxed(),
			Command::Server(args) => self.command_server(args).boxed(),
			Command::Signal(args) => self.command_process_signal(args).boxed(),
			Command::Spawn(args) => self.command_process_spawn(args).boxed(),
			Command::Status(args) => self.command_process_status(args).boxed(),
			Command::Tag(args) => self.command_tag(args).boxed(),
			Command::Tree(args) => self.command_tree(args).boxed(),
			Command::Update(args) => self.command_update(args).boxed(),
			Command::View(args) => self.command_view(args).boxed(),
			Command::Wait(args) => self.command_process_wait(args).boxed(),
			Command::Watch(args) => self.command_watch(args).boxed(),
			Command::Write(args) => self.command_write(args).boxed(),
		}
		.await
	}

	fn config_path(&self) -> PathBuf {
		self.args.config.clone().unwrap_or_else(|| {
			PathBuf::from(std::env::var("HOME").unwrap()).join(".config/tangram/config.json")
		})
	}

	fn directory_path(&self) -> PathBuf {
		self.args
			.directory
			.clone()
			.or(self
				.config
				.as_ref()
				.and_then(|config| config.server.directory.clone()))
			.unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram"))
	}

	fn read_config(directory: Option<PathBuf>) -> tg::Result<Option<Config>> {
		let directory = directory.unwrap_or_else(|| {
			PathBuf::from(std::env::var("HOME").unwrap()).join(".config/tangram/config.json")
		});
		let config = match std::fs::read_to_string(&directory) {
			Ok(config) => config,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			},
			Err(source) => {
				return Err(
					tg::error!(!source, directory = %directory.display(), "failed to read the config file"),
				);
			},
		};
		let config = serde_json::from_str(&config).map_err(
			|source| tg::error!(!source, directory = %directory.display(), "failed to deserialize the config"),
		)?;
		Ok(Some(config))
	}

	#[expect(dead_code)]
	fn write_config(&self, config: &Config) -> tg::Result<()> {
		let config = serde_json::to_string_pretty(&config)
			.map_err(|source| tg::error!(!source, "failed to serialize the config"))?;
		std::fs::write(self.config_path(), config)
			.map_err(|source| tg::error!(!source, "failed to save the config"))?;
		Ok(())
	}

	async fn get_reference(
		&mut self,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<tg::Either<tg::Object, tg::Process>>> {
		self.get_reference_with_arg(reference, tg::get::Arg::default())
			.boxed()
			.await
	}

	async fn get_reference_with_arg(
		&mut self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<tg::Referent<tg::Either<tg::Object, tg::Process>>> {
		let handle = self.handle().await?;

		// Make the path absolute.
		let relative = reference
			.item()
			.try_unwrap_path_ref()
			.is_ok_and(|path| path.is_relative());
		let mut item = reference.item().clone();
		let options = reference.options().clone();
		if let tg::reference::Item::Path(path) = &mut item {
			*path = tangram_util::fs::canonicalize_parent(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
		}
		let reference = tg::Reference::with_item_and_options(item, options);

		// Get the reference
		let stream = handle
			.get(&reference, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the reference"))?;
		let mut referent = self
			.render_progress_stream(stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the reference"))?;

		// If the reference is a local relative path, then make the referent's path relative to the current working directory.
		if referent.id().is_none()
			&& referent.tag().is_none()
			&& relative
			&& let Some(path) = referent.path()
		{
			let current_dir = std::env::current_dir()
				.map_err(|source| tg::error!(!source, "failed to get the working directory"))?;
			let path = tangram_util::path::diff(&current_dir, path)
				.map_err(|source| tg::error!(!source, "failed to diff the paths"))?;
			referent.options.path = Some(path);
		}

		Ok(referent)
	}

	async fn get_references(
		&mut self,
		references: &[tg::Reference],
	) -> tg::Result<Vec<tg::Referent<tg::Either<tg::Object, tg::Process>>>> {
		let mut referents = Vec::with_capacity(references.len());
		for reference in references {
			let referent = self.get_reference(reference).await?;
			referents.push(referent);
		}
		Ok(referents)
	}

	async fn get_modules(&mut self, references: &[tg::Reference]) -> tg::Result<Vec<tg::Module>> {
		let mut modules = Vec::with_capacity(references.len());
		for reference in references {
			let module = self.get_module(reference).await?;
			modules.push(module);
		}
		Ok(modules)
	}

	async fn get_module(&mut self, reference: &tg::Reference) -> tg::Result<tg::Module> {
		let handle = self.handle().await?;

		// Get the reference.
		let referent = self.get_reference(reference).await?;
		let item = referent
			.item
			.clone()
			.left()
			.ok_or_else(|| tg::error!("expected an object"))?;
		let mut referent = referent.map(|_| item);
		let module = match referent.item.clone() {
			tg::Object::Directory(directory) => {
				let root_module_name = tg::module::try_get_root_module_file_name(
					&handle,
					tg::Either::Left(&directory),
				)
				.await?
				.ok_or_else(
					|| tg::error!(directory = %directory.id(), "failed to find a root module"),
				)?;
				if let Some(path) = &mut referent.options.path {
					*path = path.join(root_module_name);
				} else {
					referent.options.path.replace(root_module_name.into());
				}
				let kind = tg::module::module_kind_for_path(root_module_name).unwrap();
				let item = directory.get_entry_edge(&handle, root_module_name).await?;
				let item = tg::module::Item::Edge(item.into());
				let referent = referent.map(|_| item);
				tg::Module { kind, referent }
			},

			tg::Object::File(file) => {
				let path = referent
					.path()
					.ok_or_else(|| tg::error!("expected a path"))?;
				if !tg::module::is_module_path(path) {
					return Err(tg::error!("expected a module path"));
				}
				let kind = tg::module::module_kind_for_path(path).unwrap();
				let item = file.clone().into();
				let item = tg::graph::Edge::Object(item);
				let item = tg::module::Item::Edge(item);
				let referent = referent.map(|_| item);
				tg::Module { kind, referent }
			},

			tg::Object::Symlink(_) => {
				return Err(tg::error!("unimplemented"));
			},

			_ => {
				return Err(tg::error!("expected an artifact"));
			},
		};

		Ok(module)
	}

	/// Initialize miette.
	fn initialize_miette() {
		let theme = miette::GraphicalTheme {
			characters: miette::ThemeCharacters::unicode(),
			styles: miette::ThemeStyles {
				error: owo_colors::style().red(),
				highlights: vec![owo_colors::style().red()],
				link: owo_colors::style().blue(),
				linum: owo_colors::style().dimmed(),
				warning: owo_colors::style().yellow(),
				..miette::ThemeStyles::none()
			},
		};
		let handler = miette::GraphicalReportHandler::new()
			.with_theme(theme)
			.without_syntax_highlighting();
		miette::set_hook(Box::new(move |_| Box::new(handler.clone()))).unwrap();
	}

	/// Initialize V8.
	#[cfg(feature = "v8")]
	fn initialize_v8(thread_pool_size: u32) {
		// Set the ICU data.
		v8::icu::set_common_data_74(deno_core_icudata::ICU_DATA).unwrap();

		// Initialize the platform.
		let platform = v8::new_default_platform(thread_pool_size, true);
		v8::V8::initialize_platform(platform.make_shared());

		// Initialize V8.
		v8::V8::initialize();
	}

	/// Initialize tracing.
	fn initialize_tracing(config: Option<&Config>, tracing_filter: Option<&String>) {
		let console_layer = if config.is_some_and(|config| config.tokio_console) {
			Some(console_subscriber::spawn())
		} else {
			None
		};
		let config_tracing = config.and_then(|config| config.tracing.as_ref());
		let output_layer = if tracing_filter.is_some() || config_tracing.is_some() {
			let filter_string = tracing_filter
				.or(config_tracing.map(|t| &t.filter))
				.cloned()
				.unwrap_or_default();
			let filter = tracing_subscriber::filter::EnvFilter::try_new(&filter_string).unwrap();
			let format = config_tracing
				.and_then(|t| t.format)
				.unwrap_or(self::config::TracingFormat::Pretty);
			let output_layer = match format {
				self::config::TracingFormat::Json => tracing_subscriber::fmt::layer()
					.with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
					.with_writer(std::io::stderr)
					.json()
					.boxed(),
				self::config::TracingFormat::Pretty => tracing_tree::HierarchicalLayer::new(2)
					.with_bracketed_fields(true)
					.with_span_retrace(true)
					.boxed(),
			};
			Some(output_layer.with_filter(filter))
		} else {
			None
		};
		tracing_subscriber::registry()
			.with(console_layer)
			.with(output_layer)
			.init();
		std::panic::set_hook(Box::new(|info| {
			let payload = info.payload_as_str();
			let location = info.location().map(ToString::to_string);
			let backtrace = std::backtrace::Backtrace::force_capture();
			tracing::error!(payload, location, %backtrace, "panic");
		}));
	}

	fn set_file_descriptor_limit() -> tg::Result<()> {
		let mut rlimit_nofile = libc::rlimit {
			rlim_cur: 0,
			rlim_max: 0,
		};
		let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut rlimit_nofile) };
		if ret != 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to get the file descriptor limit"
			));
		}
		rlimit_nofile.rlim_cur = rlimit_nofile.rlim_max;
		let ret = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raw const rlimit_nofile) };
		if ret != 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to set the file descriptor limit"
			));
		}
		Ok(())
	}
}
