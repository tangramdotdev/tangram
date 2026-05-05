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
	exit: Option<u8>,
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
	#[arg(env = "TANGRAM_MODE", default_value_t, long, short)]
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
	#[arg(long, short, value_delimiter = ',', conflicts_with = "no_remotes")]
	remotes: Option<Vec<String>>,

	/// Override the `remotes` key in the config.
	#[arg(long, conflicts_with = "remotes")]
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

	Log(self::process::stdio::read::Args),

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
			if args.engine.is_auto() || args.engine.is_v_8() {
				Cli::initialize_v8(0);
			}
			return Cli::command_js(&matches, args);
		},
		Command::Sandbox(self::sandbox::Args {
			command: self::sandbox::Command::Serve(args),
			..
		}) => {
			return Cli::command_sandbox_serve(args);
		},
		#[cfg(target_os = "linux")]
		Command::Sandbox(self::sandbox::Args {
			command: self::sandbox::Command::Container(args),
			..
		}) => {
			return Cli::command_sandbox_container(args);
		},
		#[cfg(target_os = "macos")]
		Command::Sandbox(self::sandbox::Args {
			command: self::sandbox::Command::Seatbelt(args),
			..
		}) => {
			return Cli::command_sandbox_seatbelt(args);
		},
		#[cfg(target_os = "linux")]
		Command::Sandbox(self::sandbox::Args {
			command: self::sandbox::Command::Vm(args),
			..
		}) => {
			return Cli::command_sandbox_vm(args);
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
		if serve_args.remotes.get().is_some() {
			args.remotes = serve_args.remotes.clone();
		}
		if let Some(tracing) = serve_args.tracing.clone() {
			args.tracing = Some(tracing);
		}
		if let Some(url) = serve_args.url.clone() {
			args.url = Some(url);
		}
	}

	// Read the config.
	let config = match Cli::read_config_with_path(args.config.clone()) {
		Ok(config) => config,
		Err(error) => {
			Cli::print_error_message("an error occurred");
			Cli::print_error_basic(tg::Referent::with_item(error));
			return std::process::ExitCode::FAILURE;
		},
	};

	let server = matches!(
		&args.command,
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
				if !args.quiet.get() {
					Cli::print_warning_message("failed to set the file descriptor limit");
				}
			})
			.ok();
	}

	// Initialize FoundationDB.
	#[cfg(feature = "foundationdb")]
	let _fdb = if server
		&& config
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
	} else if let Command::Repl(args) = &args.command
		&& (args.engine.is_auto() || args.engine.is_v_8())
	{
		true
	} else {
		false
	};
	#[cfg(feature = "v8")]
	if initialize_v8 {
		let thread_pool_size = config
			.as_ref()
			.and_then(|config| config.v8_thread_pool_size)
			.unwrap_or(0);
		Cli::initialize_v8(thread_pool_size);
	}

	// Create the tokio runtime.
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	// Initialize telemetry and tracing.
	let runtime_guard = runtime.enter();
	let telemetry = Cli::initialize_telemetry(config.as_ref());
	Cli::initialize_tracing(config.as_ref(), args.tracing.as_ref(), telemetry.as_ref());
	drop(runtime_guard);

	// Create the CLI.
	let mut cli = Cli {
		args,
		client: None,
		config,
		exit: None,
		health: None,
		matches,
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

	// Drop the client.
	runtime.block_on(async {
		if let Some(client) = cli.client.take() {
			client.disconnect().await;
		}
	});

	// Shutdown telemetry.
	if let Some(telemetry) = telemetry {
		telemetry.shutdown();
	}

	// Drop the runtime.
	drop(runtime);

	exit
}

impl Cli {
	// Run the command.
	async fn command(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Archive(args) => self.command_archive(args).boxed(),
			Command::Build(args) => self.command_build(args).boxed(),
			Command::Builtin(_) => unreachable!(),
			Command::Bundle(args) => self.command_bundle(args).boxed(),
			Command::Cache(args) => self.command_cache(args).boxed(),
			Command::Cancel(args) => self.command_process_cancel(args).boxed(),
			Command::Check(args) => self.command_check(args).boxed(),
			Command::Checkin(args) => self.command_checkin(args).boxed(),
			Command::Checkout(args) => self.command_checkout(args).boxed(),
			Command::Checksum(args) => self.command_checksum(args).boxed(),
			Command::Children(args) => self.command_children(args).boxed(),
			Command::Clean(args) => self.command_clean(args).boxed(),
			Command::Compress(args) => self.command_compress(args).boxed(),
			Command::Decompress(args) => self.command_decompress(args).boxed(),
			Command::Document(args) => self.command_document(args).boxed(),
			Command::Download(args) => self.command_download(args).boxed(),
			Command::Exec(args) => self.command_process_exec(args).boxed(),
			Command::Extract(args) => self.command_extract(args).boxed(),
			Command::Format(args) => self.command_format(args).boxed(),
			Command::Get(args) => self.command_get(args).boxed(),
			Command::Health(args) => self.command_health(args).boxed(),
			Command::Id(args) => self.command_id(args).boxed(),
			Command::Index(args) => self.command_index(args).boxed(),
			Command::Init(args) => self.command_init(args).boxed(),
			#[cfg(feature = "js")]
			Command::Js(_) => unreachable!(),
			Command::List(args) => self.command_tag_list(args).boxed(),
			Command::Log(args) => self.command_process_stdio_read(args).boxed(),
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
			#[cfg(feature = "js")]
			Command::Repl(args) => self.command_repl(args).boxed(),
			Command::Run(args) => self.command_run(args).boxed(),
			Command::Sandbox(args) => self.command_sandbox(args).boxed(),
			Command::Self_(args) => self.command_tangram(args).boxed(),
			Command::Shell(args) => self.command_shell(args).boxed(),
			Command::Serve(args) => self.command_server_run(args).boxed(),
			Command::Server(args) => self.command_server(args).boxed(),
			Command::Signal(args) => self.command_process_signal(args).boxed(),
			Command::Spawn(args) => self.command_process_spawn(args).boxed(),
			Command::Status(args) => self.command_process_status(args).boxed(),
			Command::Tag(args) => self.command_tag(args).boxed(),
			Command::Touch(args) => self.command_touch(args).boxed(),
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
}
