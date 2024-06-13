use self::config::Config;
use clap::{CommandFactory as _, Parser as _};
use crossterm::style::Stylize as _;
use futures::FutureExt as _;
use itertools::Itertools as _;
use num::ToPrimitive as _;
use std::{fmt::Write as _, path::PathBuf, sync::Mutex};
use tangram_client::{self as tg, Client};
use tracing_subscriber::prelude::*;
use url::Url;

mod artifact;
mod blob;
mod build;
mod cat;
mod checksum;
mod config;
mod get;
mod lsp;
mod object;
mod package;
mod pull;
mod push;
mod remote;
mod root;
mod server;
mod target;
mod tree;
mod tui;
mod upgrade;
mod view;

struct Cli {
	args: Args,
	config: Option<Config>,
	client: Mutex<Option<tg::Client>>,
}

#[derive(Clone, Debug, clap::Parser)]
#[command(
	about = "Tangram is a programmable build system and package manager.",
	arg_required_else_help = true,
	before_help = before_help(),
	disable_help_subcommand = true,
	name = "Tangram",
	version = version(),
)]
struct Args {
	#[command(subcommand)]
	command: Command,

	/// The path to the config file.
	#[arg(long)]
	config: Option<PathBuf>,

	/// Disable automatic starting of a server.
	#[arg(long)]
	no_start: bool,

	/// Override the `path` key in the config.
	#[arg(short, long)]
	path: Option<PathBuf>,

	/// Override the `url` key in the config.
	#[arg(short, long)]
	url: Option<Url>,
}

fn before_help() -> String {
	let version = version();
	let logo = include_str!("tangram.ascii").trim_end();
	format!("Tangram {version}\n\n{logo}")
}

fn version() -> String {
	let mut version = env!("CARGO_PKG_VERSION").to_owned();
	if let Some(commit) = option_env!("TANGRAM_CLI_COMMIT_HASH") {
		version.push('-');
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

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, clap::Subcommand)]
enum Command {
	Artifact(self::artifact::Args),
	Blob(self::blob::Args),
	Build(self::build::Args),
	Cat(self::cat::Args),
	Check(self::package::check::Args),
	Checkin(self::artifact::checkin::Args),
	Checkout(self::artifact::checkout::Args),
	Checksum(self::checksum::Args),
	Clean(self::server::clean::Args),
	Doc(self::package::doc::Args),
	Download(self::blob::download::Args),
	Format(self::package::format::Args),
	Get(self::get::Args),
	Init(self::package::init::Args),
	Log(self::build::log::Args),
	Lsp(self::lsp::Args),
	New(self::package::new::Args),
	Object(self::object::Args),
	Outdated(self::package::outdated::Args),
	Package(self::package::Args),
	Publish(self::package::publish::Args),
	Pull(self::pull::Args),
	Push(self::push::Args),
	Put(self::object::put::Args),
	Remote(self::remote::Args),
	Root(self::root::Args),
	Run(self::target::run::Args),
	Search(self::package::search::Args),
	Serve(self::server::run::Args),
	Server(self::server::Args),
	Target(self::target::Args),
	Tree(self::tree::Args),
	Update(self::package::update::Args),
	Upgrade(self::upgrade::Args),
	View(self::view::Args),
}

fn main() -> std::process::ExitCode {
	// Initialize V8.
	Cli::initialize_v8();

	// Parse the args.
	let args = Args::parse();

	// Read the config.
	let config = match Cli::read_config(args.config.clone()) {
		Ok(config) => config,
		Err(error) => {
			eprintln!("{} failed to read the config", "error".red().bold());
			futures::executor::block_on(Cli::print_error(None::<Client>, &error, None));
			return 1.into();
		},
	};

	// Initialize tracing.
	Cli::initialize_tracing(config.as_ref());

	// Set the file descriptor limit.
	Cli::set_file_descriptor_limit(config.as_ref())
		.inspect_err(|_| {
			eprintln!(
				"{} failed to set the file descriptor limit",
				"warning".yellow().bold(),
			);
		})
		.ok();

	// Create the CLI.
	let cli = Cli {
		args,
		config,
		client: Mutex::new(None),
	};

	// Create the future.
	let future = async move {
		match cli.command(cli.args.command.clone()).await {
			Ok(()) => Ok(()),
			Err(error) => {
				eprintln!("{} failed to run the command", "error".red().bold());
				let client = cli.client.lock().unwrap().clone();
				Cli::print_error(client, &error, cli.config.as_ref()).await;
				Err(1)
			},
		}
	};

	// Create the tokio runtime and block on the future.
	let mut builder = tokio::runtime::Builder::new_multi_thread();
	builder.enable_all();
	let runtime = builder.build().unwrap();
	let result = runtime.block_on(future);
	runtime.shutdown_background();

	// Handle the result.
	match result {
		Ok(()) => 0.into(),
		Err(code) => code.into(),
	}
}

impl Cli {
	async fn client(&self) -> tg::Result<Client> {
		if let Some(client) = self.client.lock().unwrap().clone() {
			return Ok(client);
		}

		// Get the path.
		let path = self
			.args
			.path
			.clone()
			.or(self.config.as_ref().and_then(|config| config.path.clone()))
			.unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram"));

		// Get the url.
		let url = self
			.args
			.url
			.clone()
			.or(self.config.as_ref().and_then(|config| config.url.clone()))
			.unwrap_or_else(|| {
				let path = path.join("socket");
				let path = path.to_str().unwrap();
				let path = urlencoding::encode(path);
				format!("http+unix://{path}").parse().unwrap()
			});

		// Create the client.
		let client = tg::Client::new(url);

		// Attempt to connect to the server.
		client.connect().await.ok();

		// If the client is not connected and the URL is local, then start the server and attempt to connect.
		let local = client.url().scheme() == "http+unix"
			|| matches!(client.url().host_str(), Some("localhost" | "0.0.0.0"));
		if !client.connected().await && local {
			// Start the server.
			self.start_server().await?;

			// Try to connect for up to one second.
			for _ in 0..10 {
				if client.connect().await.is_ok() {
					break;
				}
				tokio::time::sleep(std::time::Duration::from_millis(100)).await;
			}

			// If the client is not connected, then return an error.
			if !client.connected().await {
				return Err(tg::error!(%url = client.url(), "failed to connect to the server"));
			}
		}

		// If the URL is local and the server's version is different from the client, then disconnect and restart the server.
		'a: {
			if !local {
				break 'a;
			}

			let Some(server_version) = &client.health().await?.version else {
				break 'a;
			};

			if Args::command().get_version().unwrap() == server_version {
				break 'a;
			};

			// Disconnect.
			client.disconnect().await?;

			// Stop the server.
			self.stop_server().await?;

			// Start the server.
			self.start_server().await?;

			// Try to connect for up to one second.
			for _ in 0..10 {
				if client.connect().await.is_ok() {
					break;
				}
				tokio::time::sleep(std::time::Duration::from_millis(100)).await;
			}

			// If the client is not connected, then return an error.
			if !client.connected().await {
				return Err(tg::error!(%url = client.url(), "failed to connect to the server"));
			}
		}

		// Set the client.
		self.client.lock().unwrap().replace(client.clone());

		Ok(client)
	}

	/// Start the server.
	async fn start_server(&self) -> tg::Result<()> {
		// Ensure the path exists.
		let home = PathBuf::from(std::env::var("HOME").unwrap());
		let path = self
			.args
			.path
			.clone()
			.or(self.config.as_ref().and_then(|config| config.path.clone()))
			.unwrap_or_else(|| home.join(".tangram"));
		tokio::fs::create_dir_all(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;

		// Get the log file path.
		let log_path = path.join("log");

		// Get the path to the current executable.
		let executable = std::env::current_exe()
			.map_err(|source| tg::error!(!source, "failed to get the current executable path"))?;

		// Spawn the server.
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
		tokio::process::Command::new(executable)
			.args(["server", "run"])
			.current_dir(home)
			.stdin(std::process::Stdio::null())
			.stdout(stdout)
			.stderr(stderr)
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the server"))?;

		Ok(())
	}

	/// Stop the server.
	async fn stop_server(&self) -> tg::Result<()> {
		// Get the lock file path.
		let path = self
			.args
			.path
			.clone()
			.or(self.config.as_ref().and_then(|config| config.path.clone()))
			.unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram"));
		let lock_path = path.join("lock");

		// Read the PID from the lock file.
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

		// Wait up to five seconds for the server to exit.
		for _ in 0..50 {
			// If the server has exited, then return.
			let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGINT) };
			if ret != 0 {
				return Ok(());
			}

			// Otherwise, sleep.
			let duration = std::time::Duration::from_millis(100);
			tokio::time::sleep(duration).await;
		}

		// If the server has still not exited, then send SIGTERM.
		let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGTERM) };
		if ret != 0 {
			return Err(tg::error!("failed to send SIGTERM to the server"));
		}

		// Wait up to one second for the server to exit.
		for _ in 0..10 {
			// If the server has exited, then return.
			let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGINT) };
			if ret != 0 {
				return Ok(());
			}

			// Otherwise, sleep.
			let duration = std::time::Duration::from_millis(100);
			tokio::time::sleep(duration).await;
		}

		// If the server has still not exited, then return an error.
		Err(tg::error!("failed to terminate the server"))
	}

	// Run the command
	async fn command(&self, command: Command) -> tg::Result<()> {
		match command {
			Command::Artifact(args) => self.command_artifact(args).boxed(),
			Command::Blob(args) => self.command_blob(args).boxed(),
			Command::Build(args) => self.command_build(args).boxed(),
			Command::Cat(args) => self.command_cat(args).boxed(),
			Command::Check(args) => self.command_package_check(args).boxed(),
			Command::Checkin(args) => self.command_artifact_checkin(args).boxed(),
			Command::Checkout(args) => self.command_artifact_checkout(args).boxed(),
			Command::Checksum(args) => self.command_checksum(args).boxed(),
			Command::Clean(args) => self.command_server_clean(args).boxed(),
			Command::Doc(args) => self.command_package_doc(args).boxed(),
			Command::Download(args) => self.command_blob_download(args).boxed(),
			Command::Format(args) => self.command_package_format(args).boxed(),
			Command::Get(args) => self.command_get(args).boxed(),
			Command::Init(args) => self.command_package_init(args).boxed(),
			Command::Log(args) => self.command_build_log(args).boxed(),
			Command::Lsp(args) => self.command_lsp(args).boxed(),
			Command::New(args) => self.command_package_new(args).boxed(),
			Command::Object(args) => self.command_object(args).boxed(),
			Command::Outdated(args) => self.command_package_outdated(args).boxed(),
			Command::Package(args) => self.command_package(args).boxed(),
			Command::Publish(args) => self.command_package_publish(args).boxed(),
			Command::Pull(args) => self.command_pull(args).boxed(),
			Command::Push(args) => self.command_push(args).boxed(),
			Command::Put(args) => self.command_object_put(args).boxed(),
			Command::Remote(args) => self.command_remote(args).boxed(),
			Command::Root(args) => self.command_root(args).boxed(),
			Command::Run(args) => self.command_target_run(args).boxed(),
			Command::Search(args) => self.command_package_search(args).boxed(),
			Command::Serve(args) => self.command_server_run(args).boxed(),
			Command::Server(args) => self.command_server(args).boxed(),
			Command::Target(args) => self.command_target(args).boxed(),
			Command::Tree(args) => self.command_tree(args).boxed(),
			Command::Update(args) => self.command_package_update(args).boxed(),
			Command::Upgrade(args) => self.command_upgrade(args).boxed(),
			Command::View(args) => self.command_view(args).boxed(),
		}
		.await
	}

	fn read_config(path: Option<PathBuf>) -> tg::Result<Option<Config>> {
		let path = path.unwrap_or_else(|| {
			PathBuf::from(std::env::var("HOME").unwrap()).join(".config/tangram/config.json")
		});
		let config = match std::fs::read_to_string(&path) {
			Ok(config) => config,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
			Err(source) => {
				return Err(
					tg::error!(!source, %path = path.display(), "failed to read the config file"),
				)
			},
		};
		let config = serde_json::from_str(&config).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to deserialize the config"),
		)?;
		Ok(Some(config))
	}

	fn _write_config(config: &Config, path: Option<PathBuf>) -> tg::Result<()> {
		let path = path.unwrap_or_else(|| {
			PathBuf::from(std::env::var("HOME").unwrap()).join(".config/tangram/config.json")
		});
		let config = serde_json::to_string_pretty(&config)
			.map_err(|source| tg::error!(!source, "failed to serialize the config"))?;
		std::fs::write(path, config)
			.map_err(|source| tg::error!(!source, "failed to save the config"))?;
		Ok(())
	}

	async fn print_error<H>(handle: Option<H>, error: &tg::Error, config: Option<&Config>)
	where
		H: tg::Handle,
	{
		let options = config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.error_trace_options.clone())
			.unwrap_or_default();
		let trace = error.trace(&options);
		let mut errors = vec![trace.error];
		while let Some(next) = errors.last().unwrap().source.as_ref() {
			errors.push(next);
		}
		if !trace.options.reverse {
			errors.reverse();
		}
		for error in errors {
			eprintln!("{} {}", "->".red(), error.message);
			if let Some(location) = &error.location {
				if !location.source.is_internal() || trace.options.internal {
					let source = match &location.source {
						tg::error::Source::Internal { path } => {
							path.components().iter().skip(1).join("/")
						},
						tg::error::Source::External { package, path } => {
							let source = if let Some(handle) = handle.as_ref() {
								Self::get_description_for_package(handle, package)
									.await
									.ok()
									.unwrap_or_else(|| package.to_string())
							} else {
								package.to_string()
							};
							if let Some(path) = path {
								let path = path.components().iter().skip(1).join("/");
								format!("{source}:{path}")
							} else {
								source.to_string()
							}
						},
					};
					let mut string = String::new();
					let line = location.line + 1;
					let column = location.column + 1;
					write!(string, "{source}:{line}:{column}").unwrap();
					if let Some(symbol) = &location.symbol {
						write!(string, " {symbol}").unwrap();
					}
					eprintln!("   {}", string.yellow());
				}
			}
			for (name, value) in &error.values {
				let name = name.as_str().blue();
				let value = value.as_str().green();
				eprintln!("   {name} = {value}");
			}
			let mut stack = error.stack.iter().flatten().collect::<Vec<_>>();
			if !trace.options.reverse {
				stack.reverse();
			}
			for location in stack {
				if !location.source.is_internal() || trace.options.internal {
					let location = location.to_string().yellow();
					eprintln!("   {location}");
				}
			}
		}
	}

	async fn print_diagnostic(&self, diagnostic: &tg::Diagnostic) {
		let title = match diagnostic.severity {
			tg::diagnostic::Severity::Error => "error".red().bold(),
			tg::diagnostic::Severity::Warning => "warning".yellow().bold(),
			tg::diagnostic::Severity::Info => "info".blue().bold(),
			tg::diagnostic::Severity::Hint => "hint".cyan().bold(),
		};
		eprintln!("{title}: {}", diagnostic.message);
		let mut string = String::new();
		if let Some(location) = &diagnostic.location {
			match &location.module {
				tg::Module::Js(tg::module::Js::PackageArtifact(package_artifact))
				| tg::Module::Ts(tg::module::Js::PackageArtifact(package_artifact)) => {
					let client = self.client.lock().unwrap().clone();
					let id = package_artifact.artifact.clone();
					let artifact = tg::Artifact::with_id(id.clone());
					let metadata = if let Some(client) = client {
						tg::package::try_get_metadata(&client, &artifact).await.ok()
					} else {
						None
					};
					if let Some(metadata) = metadata {
						let (name, version) = metadata
							.map(|metadata| (metadata.name, metadata.version))
							.unwrap_or_default();
						let name = name.as_deref().unwrap_or("<unknown>");
						let version = version.as_deref().unwrap_or("<unknown>");
						write!(string, "{name}@{version}").unwrap();
					} else {
						write!(string, "{id}").unwrap();
					}
				},

				tg::Module::Js(tg::module::Js::PackagePath(package_path))
				| tg::Module::Ts(tg::module::Js::PackagePath(package_path)) => {
					let path = package_path.package_path.join(&package_path.path);
					let path = path.display();
					write!(string, "{path}").unwrap();
				},

				tg::Module::Artifact(tg::module::Artifact::Path(path))
				| tg::Module::Directory(tg::module::Directory::Path(path))
				| tg::Module::File(tg::module::File::Path(path))
				| tg::Module::Symlink(tg::module::Symlink::Path(path)) => {
					let path = path.clone();
					write!(string, "{path}").unwrap();
				},

				_ => (),
			}
			let mut string = if string.is_empty() {
				"<unknown>".to_owned()
			} else {
				string
			};
			let line = location.range.start.line + 1;
			let character = location.range.start.character + 1;
			write!(string, ":{line}:{character}").unwrap();
			eprint!("   {}", string.yellow());
		}
		eprintln!();
	}

	async fn get_description_for_package<H>(
		handle: &H,
		package: &tg::artifact::Id,
	) -> tg::Result<String>
	where
		H: tg::Handle,
	{
		let dependency = tg::Dependency::with_artifact(package.clone());
		let arg = tg::package::get::Arg {
			metadata: true,
			path: true,
			..Default::default()
		};
		let output = handle
			.get_package(&dependency, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the package"))?;
		if let Some(path) = output.path {
			return Ok(path.components().iter().skip(1).join("/"));
		}
		let (name, version) = output
			.metadata
			.as_ref()
			.map(|metadata| (metadata.name.as_deref(), metadata.version.as_deref()))
			.unwrap_or_default();
		let name = name.unwrap_or("<unknown>");
		let version = version.unwrap_or("<unknown>");
		Ok(format!("{name}@{version}"))
	}

	/// Initialize V8.
	fn initialize_v8() {
		// Set the ICU data.
		v8::icu::set_common_data_73(deno_core_icudata::ICU_DATA).unwrap();

		// Initialize the platform.
		let platform = v8::new_default_platform(0, true);
		v8::V8::initialize_platform(platform.make_shared());

		// Set flags.
		v8::V8::set_flags_from_string("--harmony-import-attributes");

		// Initialize V8.
		v8::V8::initialize();
	}

	/// Initialize tracing.
	fn initialize_tracing(config: Option<&Config>) {
		let console_layer = if config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.map_or(false, |advanced| advanced.tokio_console)
		{
			Some(console_subscriber::spawn())
		} else {
			None
		};
		let output_layer = config
			.as_ref()
			.and_then(|config| config.tracing.as_ref())
			.map(|tracing| {
				let filter =
					tracing_subscriber::filter::EnvFilter::try_new(&tracing.filter).unwrap();
				let format = tracing
					.format
					.unwrap_or(self::config::TracingFormat::Pretty);
				let output_layer = match format {
					self::config::TracingFormat::Compact
					| self::config::TracingFormat::Json
					| self::config::TracingFormat::Pretty => {
						let layer = tracing_subscriber::fmt::layer()
							.with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
							.with_writer(std::io::stderr);
						let layer = match format {
							self::config::TracingFormat::Compact => layer.compact().boxed(),
							self::config::TracingFormat::Hierarchical => unreachable!(),
							self::config::TracingFormat::Json => layer.json().boxed(),
							self::config::TracingFormat::Pretty => layer.pretty().boxed(),
						};
						layer.boxed()
					},
					self::config::TracingFormat::Hierarchical => {
						tracing_tree::HierarchicalLayer::new(2)
							.with_bracketed_fields(true)
							.with_span_retrace(true)
							.with_targets(true)
							.boxed()
					},
				};
				output_layer.with_filter(filter)
			});
		tracing_subscriber::registry()
			.with(console_layer)
			.with(output_layer)
			.init();
	}

	fn set_file_descriptor_limit(config: Option<&Config>) -> tg::Result<()> {
		if let Some(file_descriptor_limit) = config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.file_descriptor_limit)
		{
			let new_fd_rlimit = libc::rlimit {
				rlim_cur: file_descriptor_limit,
				rlim_max: file_descriptor_limit,
			};
			let ret = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &new_fd_rlimit) };
			if ret != 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to set the file descriptor limit"
				));
			}
		}
		Ok(())
	}

	// Get the host.
	fn host() -> &'static str {
		#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
		{
			"aarch64-darwin"
		}
		#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
		{
			"aarch64-linux"
		}
		#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
		{
			"x86_64-darwin"
		}
		#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
		{
			"x86_64-linux"
		}
	}
}
