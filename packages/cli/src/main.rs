use {
	clap::{CommandFactory as _, FromArgMatches as _},
	futures::FutureExt as _,
	std::path::PathBuf,
	tangram_client::prelude::*,
	tangram_uri::Uri,
};

mod archive;
mod builtin;
mod bundle;
mod cache;
mod check;
mod checkin;
mod checkout;
mod checksum;
mod children;
mod clean;
mod client;
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
mod location;
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
#[cfg(feature = "js")]
mod repl;
mod sandbox;
mod server;
mod shell;
mod tag;
mod tangram;
mod telemetry;
mod theme;
mod touch;
mod tracing;
mod tree;
mod update;
mod user;
mod view;
mod viewer;
mod watch;
mod write;

pub use self::config::Config;

pub mod config;

pub struct Cli {
	args: Args,
	client: Option<tg::Client>,
	config: Option<Config>,
	exit: Option<std::process::ExitCode>,
	health: Option<tg::Health>,
	matches: clap::ArgMatches,
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
	#[arg(default_value_t, env = "TANGRAM_MODE", long, short)]
	mode: Mode,

	#[clap(flatten)]
	quiet: Quiet,

	#[command(flatten)]
	remotes: Remotes,

	#[arg(env = "TANGRAM_TOKEN", long)]
	token: Option<String>,

	/// Override the tracing filter.
	#[arg(env = "TANGRAM_TRACING", long)]
	tracing: Option<String>,

	/// Override the HTTP listener URL in the config.
	#[arg(env = "TANGRAM_URL", long, short)]
	url: Option<Uri>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Remotes {
	/// Override the `remotes` key in the config.
	#[arg(conflicts_with = "no_remotes", long, short, value_delimiter = ',')]
	remotes: Option<Vec<String>>,

	/// Override the `remotes` key in the config.
	#[arg(conflicts_with = "remotes", long)]
	no_remotes: bool,
}

impl Remotes {
	#[must_use]
	pub fn get(&self) -> Option<&[String]> {
		if self.no_remotes {
			Some(&[])
		} else {
			self.remotes.as_deref()
		}
	}
}

#[derive(Clone, Debug, Default, clap::Args)]
struct Quiet {
	/// Whether to show progress and other helpful information.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_quiet",
		require_equals = true,
		short,
	)]
	quiet: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "quiet",
		require_equals = true,
	)]
	no_quiet: Option<bool>,
}

impl Quiet {
	fn get(&self) -> bool {
		self.quiet
			.or(self.no_quiet.map(|value| !value))
			.or_else(|| {
				std::env::var("TANGRAM_QUIET")
					.ok()
					.and_then(|value| value.parse().ok())
			})
			.unwrap_or(false)
	}
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

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	clap::ValueEnum,
	derive_more::Display,
	derive_more::FromStr,
	serde::Deserialize,
	serde::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
enum Mode {
	#[default]
	Auto,
	Client,
}

#[derive(Clone, Debug, clap::Subcommand)]
enum Command {
	Archive(self::archive::Args),

	#[command(alias = "b")]
	Build(self::process::build::Args),

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

	Compress(self::compress::Args),

	Decompress(self::decompress::Args),

	#[command(alias = "doc")]
	Document(self::document::Args),

	Download(self::download::Args),

	Exec(self::process::exec::Args),

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

	Login(self::user::login::Args),

	Log(self::process::stdio::read::Args),

	Logout(self::user::logout::Args),

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

	#[command(alias = "add")]
	Put(self::put::Args),

	#[command(alias = "cat")]
	Read(self::read::Args),

	Remote(self::remote::Args),

	#[cfg(feature = "js")]
	Repl(self::repl::Args),

	#[command(alias = "r")]
	Run(self::process::run::Args),

	Sandbox(self::sandbox::Args),

	#[command(name = "self")]
	Self_(self::tangram::Args),

	Shell(self::shell::Args),

	Serve(self::server::run::Args),

	Server(self::server::Args),

	#[command(alias = "kill")]
	Signal(self::process::signal::Args),

	Spawn(self::process::spawn::Args),

	Status(self::process::status::Args),

	Tag(self::tag::Args),

	Touch(self::touch::Args),

	#[command(hide = true)]
	Tree(self::tree::Args),

	Update(self::update::Args),

	User(self::user::Args),

	View(self::view::Args),

	Wait(self::process::wait::Args),

	Watch(self::watch::Args),

	Whoami(self::user::whoami::Args),

	Write(self::write::Args),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
	// Parse the args.
	let matches = Args::command().get_matches();
	let args = Args::from_arg_matches(&matches).unwrap();

	// Create the CLI.
	let mut cli = Cli {
		args,
		client: None,
		config: None,
		exit: None,
		health: None,
		matches,
	};

	// Handle internal commands.
	let result = match cli.args.command.clone() {
		Command::Builtin(command_args) => Some(cli.command_builtin(command_args).await),
		#[cfg(feature = "js")]
		Command::Js(command_args) => {
			#[cfg(feature = "v8")]
			if command_args.engine.is_auto() || command_args.engine.is_v_8() {
				Cli::initialize_v8(0);
			}
			Some(cli.command_js(command_args).await)
		},
		Command::Sandbox(self::sandbox::Args {
			command: self::sandbox::Command::Serve(command_args),
			..
		}) => Some(cli.command_sandbox_serve(command_args).await),
		#[cfg(target_os = "linux")]
		Command::Sandbox(self::sandbox::Args {
			command: self::sandbox::Command::Container(command_args),
			..
		}) => Some(cli.command_sandbox_container(command_args).await),
		#[cfg(target_os = "macos")]
		Command::Sandbox(self::sandbox::Args {
			command: self::sandbox::Command::Seatbelt(command_args),
			..
		}) => Some(cli.command_sandbox_seatbelt(command_args).await),
		#[cfg(target_os = "linux")]
		Command::Sandbox(self::sandbox::Args {
			command: self::sandbox::Command::Vm(command_args),
			..
		}) => Some(cli.command_sandbox_vm(command_args).await),
		_ => None,
	};
	if let Some(result) = result {
		return match result {
			Ok(()) => cli.exit.unwrap_or_default(),
			Err(error) => {
				Cli::print_error_basic(tg::Referent::with_item(error));
				std::process::ExitCode::FAILURE
			},
		};
	}

	// Override args from the serve args.
	if let Command::Serve(serve_args)
	| Command::Server(self::server::Args {
		command: self::server::Command::Run(serve_args),
		..
	}) = &cli.args.command
	{
		if let Some(config) = serve_args.config.clone() {
			cli.args.config = Some(config);
		}
		if let Some(directory) = serve_args.directory.clone() {
			cli.args.directory = Some(directory);
		}
		if serve_args.remotes.get().is_some() {
			cli.args.remotes = serve_args.remotes.clone();
		}
		if let Some(tracing) = serve_args.tracing.clone() {
			cli.args.tracing = Some(tracing);
		}
		if let Some(url) = serve_args.url.clone() {
			cli.args.url = Some(url);
		}
	}

	// Read the config.
	cli.config = match Cli::read_config_with_path(cli.args.config.clone()).await {
		Ok(config) => config,
		Err(error) => {
			Cli::print_error_message("an error occurred");
			Cli::print_error_basic(tg::Referent::with_item(error));
			return std::process::ExitCode::FAILURE;
		},
	};

	let server = matches!(
		&cli.args.command,
		Command::Serve(_)
			| Command::Server(self::server::Args {
				command: self::server::Command::Run(_),
				..
			})
	);

	// Set the file descriptor limit.
	if server {
		Cli::set_file_descriptor_limit()
			.inspect_err(|_| {
				if !cli.args.quiet.get() {
					Cli::print_warning_message("failed to set the file descriptor limit");
				}
			})
			.ok();
	}

	// Initialize FoundationDB.
	#[cfg(feature = "foundationdb")]
	let _fdb = if server
		&& cli
			.config
			.as_ref()
			.is_some_and(|config| config.server.index.is_fdb())
	{
		Some(unsafe { foundationdb::boot() })
	} else {
		None
	};

	// Initialize miette.
	Cli::initialize_miette();

	// Initialize V8.
	#[cfg(feature = "v8")]
	let initialize_v8 = if server {
		true
	} else if let Command::Js(args) = &cli.args.command
		&& (args.engine.is_auto() || args.engine.is_v_8())
	{
		true
	} else if let Command::Repl(args) = &cli.args.command
		&& (args.engine.is_auto() || args.engine.is_v_8())
	{
		true
	} else {
		false
	};
	#[cfg(feature = "v8")]
	if initialize_v8 {
		let thread_pool_size = cli
			.config
			.as_ref()
			.and_then(|config| config.v8_thread_pool_size)
			.unwrap_or(0);
		Cli::initialize_v8(thread_pool_size);
	}

	// Initialize telemetry and tracing.
	let telemetry = Cli::initialize_telemetry(cli.config.as_ref());
	Cli::initialize_tracing(
		cli.config.as_ref(),
		cli.args.tracing.as_ref(),
		telemetry.as_ref(),
	);

	// Run the command.
	let result = cli.command(cli.args.clone()).await;

	// Handle the result.
	let exit = match result {
		Ok(()) => cli.exit.unwrap_or_default(),
		Err(error) => {
			Cli::print_error_message("an error occurred");
			let error = tg::Referent::with_item(error);
			cli.print_error(error).await;
			cli.exit.unwrap_or(std::process::ExitCode::FAILURE)
		},
	};

	// Drop the client.
	if let Some(client) = cli.client.take() {
		client.disconnect().await;
	}

	// Shutdown telemetry.
	if let Some(telemetry) = telemetry {
		telemetry.shutdown();
	}

	exit
}

impl Cli {
	// Run the command.
	async fn command(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Archive(args) => self.command_archive(args).boxed_local(),
			Command::Build(args) => self.command_build(args).boxed_local(),
			Command::Builtin(args) => self.command_builtin(args).boxed_local(),
			Command::Bundle(args) => self.command_bundle(args).boxed_local(),
			Command::Cache(args) => self.command_cache(args).boxed_local(),
			Command::Cancel(args) => self.command_process_cancel(args).boxed_local(),
			Command::Check(args) => self.command_check(args).boxed_local(),
			Command::Checkin(args) => self.command_checkin(args).boxed_local(),
			Command::Checkout(args) => self.command_checkout(args).boxed_local(),
			Command::Checksum(args) => self.command_checksum(args).boxed_local(),
			Command::Children(args) => self.command_children(args).boxed_local(),
			Command::Clean(args) => self.command_clean(args).boxed_local(),
			Command::Compress(args) => self.command_compress(args).boxed_local(),
			Command::Decompress(args) => self.command_decompress(args).boxed_local(),
			Command::Document(args) => self.command_document(args).boxed_local(),
			Command::Download(args) => self.command_download(args).boxed_local(),
			Command::Exec(args) => self.command_process_exec(args).boxed_local(),
			Command::Extract(args) => self.command_extract(args).boxed_local(),
			Command::Format(args) => self.command_format(args).boxed_local(),
			Command::Get(args) => self.command_get(args).boxed_local(),
			Command::Health(args) => self.command_health(args).boxed_local(),
			Command::Id(args) => self.command_id(args).boxed_local(),
			Command::Index(args) => self.command_index(args).boxed_local(),
			Command::Init(args) => self.command_init(args).boxed_local(),
			#[cfg(feature = "js")]
			Command::Js(args) => self.command_js(args).boxed_local(),
			Command::List(args) => self.command_tag_list(args).boxed_local(),
			Command::Login(args) => self.command_user_login(args).boxed_local(),
			Command::Log(args) => self.command_process_stdio_read(args).boxed_local(),
			Command::Logout(args) => self.command_user_logout(args).boxed_local(),
			Command::Lsp(args) => self.command_lsp(args).boxed_local(),
			Command::Metadata(args) => self.command_metadata(args).boxed_local(),
			Command::New(args) => self.command_new(args).boxed_local(),
			Command::Object(args) => self.command_object(args).boxed_local(),
			Command::Outdated(args) => self.command_outdated(args).boxed_local(),
			Command::Output(args) => self.command_process_output(args).boxed_local(),
			Command::Process(args) => self.command_process(args).boxed_local(),
			Command::Processes(args) => self.command_process_list(args).boxed_local(),
			Command::Publish(args) => self.command_publish(args).boxed_local(),
			Command::Pull(args) => self.command_pull(args).boxed_local(),
			Command::Push(args) => self.command_push(args).boxed_local(),
			Command::Put(args) => self.command_put(args).boxed_local(),
			Command::Read(args) => self.command_read(args).boxed_local(),
			Command::Remote(args) => self.command_remote(args).boxed_local(),
			#[cfg(feature = "js")]
			Command::Repl(args) => self.command_repl(args).boxed_local(),
			Command::Run(args) => self.command_run(args).boxed_local(),
			Command::Sandbox(args) => self.command_sandbox(args).boxed_local(),
			Command::Self_(args) => self.command_tangram(args).boxed_local(),
			Command::Shell(args) => self.command_shell(args).boxed_local(),
			Command::Serve(args) => self.command_server_run(args).boxed_local(),
			Command::Server(args) => self.command_server(args).boxed_local(),
			Command::Signal(args) => self.command_process_signal(args).boxed_local(),
			Command::Spawn(args) => self.command_process_spawn(args).boxed_local(),
			Command::Status(args) => self.command_process_status(args).boxed_local(),
			Command::Tag(args) => self.command_tag(args).boxed_local(),
			Command::Touch(args) => self.command_touch(args).boxed_local(),
			Command::Tree(args) => self.command_tree(args).boxed_local(),
			Command::Update(args) => self.command_update(args).boxed_local(),
			Command::User(args) => self.command_user(args).boxed_local(),
			Command::View(args) => self.command_view(args).boxed_local(),
			Command::Wait(args) => self.command_process_wait(args).boxed_local(),
			Command::Watch(args) => self.command_watch(args).boxed_local(),
			Command::Whoami(args) => self.command_user_whoami(args).boxed_local(),
			Command::Write(args) => self.command_write(args).boxed_local(),
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
		let highlighter =
			miette::highlighters::SyntectHighlighter::new_themed(crate::theme::tangram(), false);
		let handler = miette::GraphicalReportHandler::new()
			.with_theme(theme)
			.with_syntax_highlighting(highlighter);
		miette::set_hook(Box::new(move |_| Box::new(handler.clone()))).unwrap();
	}

	/// Initialize V8.
	#[cfg(feature = "v8")]
	fn initialize_v8(thread_pool_size: u32) {
		// Set the ICU data.
		v8::icu::set_common_data_77(deno_core_icudata::ICU_DATA).unwrap();

		// Initialize the platform.
		let platform = v8::new_default_platform(thread_pool_size, true);
		v8::V8::initialize_platform(platform.make_shared());

		// Initialize V8.
		v8::V8::initialize();
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

	async fn spawn_thread<F, T>(f: F) -> tg::Result<T>
	where
		F: FnOnce() -> tg::Result<T> + Send + 'static,
		T: Send + 'static,
	{
		let (sender, receiver) = tokio::sync::oneshot::channel();
		std::thread::spawn(move || {
			let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
				.map_err(|payload| {
					let message = payload
						.downcast_ref::<&str>()
						.map(|message| (*message).to_owned())
						.or_else(|| payload.downcast_ref::<String>().cloned());
					tg::error!(?message, "the thread panicked")
				})
				.and_then(|result| result);
			sender.send(result).ok();
		});
		receiver
			.await
			.map_err(|error| tg::error!(!error, "failed to receive the thread result"))?
	}
}
