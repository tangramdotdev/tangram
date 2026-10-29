#![recursion_limit = "256"]

use {
	clap::{CommandFactory as _, FromArgMatches as _},
	futures::FutureExt as _,
	tangram_cli_core::Cli,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, clap::Parser)]
#[command(
	about = "Tangram is a build system and package manager.",
	arg_required_else_help = true,
	before_help = tangram_cli_core::before_help(),
	disable_help_subcommand = true,
	name = "tangram",
	version = tangram_cli_core::version(),
)]
struct Args {
	#[command(subcommand)]
	command: Command,

	#[command(flatten)]
	global: tangram_cli_core::Args,
}

#[derive(Clone, Debug, clap::Subcommand)]
enum Command {
	Archive(tangram_cli_archive::Args),

	Authorization(tangram_cli_authorization::Args),

	Availability(tangram_cli_availability::Args),

	#[command(alias = "b")]
	Build(tangram_cli_process::build::Args),

	#[command(hide = true)]
	Builtin(tangram_cli_builtin::Args),

	Bundle(tangram_cli_bundle::Args),

	Cancel(tangram_cli_process::cancel::Args),

	Check(tangram_cli_check::Args),

	#[command(hide = true)]
	Checkpoint(tangram_cli_checkpoint::Args),

	#[command(alias = "ci")]
	Checkin(tangram_cli_checkin::Args),

	#[command(alias = "co")]
	Checkout(tangram_cli_checkout::Args),

	Checksum(tangram_cli_checksum::Args),

	Children(tangram_cli_children::Args),

	Clean(tangram_cli_clean::Args),

	#[command(hide = true)]
	Clock(tangram_cli_clock::Args),

	Compress(tangram_cli_compress::Args),

	Decompress(tangram_cli_decompress::Args),

	#[command(alias = "doc")]
	Document(tangram_cli_document::Args),

	Download(tangram_cli_download::Args),

	Exec(tangram_cli_process::exec::Args),

	Extract(tangram_cli_extract::Args),

	Format(tangram_cli_format::Args),

	Get(tangram_cli_get::Args),

	Grant(tangram_cli_grants::create::Args),

	Grants(tangram_cli_grants::Args),

	#[command(alias = "groups")]
	Group(tangram_cli_group::Args),

	Health(tangram_cli_health::Args),

	#[command(hide = true)]
	Id(tangram_cli_id::Args),

	Index(tangram_cli_index::Args),

	Init(tangram_cli_init::Args),

	Interface(tangram_cli_interface::Args),

	#[command(alias = "ls")]
	List(tangram_cli_list::Args),

	Login(tangram_cli_user::login::Args),

	Log(tangram_cli_process::stdio::read::Args),

	Logout(tangram_cli_user::logout::Args),

	Lsp(tangram_cli_lsp::Args),

	Match(tangram_cli_match::Args),

	Metadata(tangram_cli_metadata::Args),

	New(tangram_cli_new::Args),

	#[command(alias = "obj")]
	Object(tangram_cli_object::Args),

	#[command(alias = "org")]
	Organization(tangram_cli_organization::Args),

	Outdated(tangram_cli_outdated::Args),

	Output(tangram_cli_process::output::Args),

	#[command(alias = "pcs")]
	Process(tangram_cli_process::Args),

	Ps(tangram_cli_sandbox::list::Args),

	Publish(tangram_cli_publish::Args),

	Pull(tangram_cli_pull::Args),

	Push(tangram_cli_push::Args),

	#[command(alias = "add")]
	Put(tangram_cli_put::Args),

	#[command(alias = "cat")]
	Read(tangram_cli_read::Args),

	Remote(tangram_cli_remote::Args),

	Revoke(tangram_cli_grants::delete::Args),

	#[cfg(any(feature = "quickjs", feature = "v8"))]
	Repl(tangram_cli_repl::Args),

	#[command(alias = "r")]
	Run(tangram_cli_process::run::Args),

	Runner(tangram_cli_runner::Args),

	#[command(alias = "sbx")]
	Sandbox(tangram_cli_sandbox::Args),

	#[command(name = "self")]
	Self_(tangram_cli_tangram::Args),

	Shell(tangram_cli_shell::Args),

	Serve(tangram_cli_server::run::Args),

	Server(tangram_cli_server::Args),

	#[command(alias = "kill")]
	Signal(tangram_cli_process::signal::Args),

	Spawn(tangram_cli_process::spawn::Args),

	Tag(tangram_cli_tag::Args),

	Touch(tangram_cli_touch::Args),

	#[command(hide = true)]
	Tree(tangram_cli_tree::Args),

	Update(tangram_cli_update::Args),

	User(tangram_cli_user::Args),

	Usage(tangram_cli_usage::Args),

	View(tangram_cli_view::Args),

	Wait(tangram_cli_wait::Args),

	Watch(tangram_cli_watch::Args),

	Whoami(tangram_cli_user::whoami::Args),

	Write(tangram_cli_write::Args),
}

fn main() -> std::process::ExitCode {
	// Parse the args.
	let command = Args::command();
	let matches = command.clone().get_matches();
	let args = Args::from_arg_matches(&matches).unwrap();
	if let Command::Sandbox(tangram_cli_sandbox::Args { command }) = args.command.clone() {
		let result = match command {
			tangram_cli_sandbox::Command::Run(args) => Some(tangram_cli_sandbox::run::run(args)),
			_ => None,
		};
		if let Some(result) = result {
			return match result {
				Ok(exit) => exit,
				Err(error) => {
					Cli::print_error_basic(tg::Referent::with_node(error), false);
					std::process::ExitCode::FAILURE
				},
			};
		}
	}
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();
	runtime.block_on(main_inner(command, matches, args))
}

async fn main_inner(
	command: clap::Command,
	matches: clap::ArgMatches,
	args: Args,
) -> std::process::ExitCode {
	// Create the CLI.
	let mut cli = Cli::new(args.global, command, matches);
	let command = args.command;

	// Handle internal commands.
	let result = match command.clone() {
		Command::Builtin(command_args) => {
			#[cfg(feature = "v8")]
			if let tangram_cli_builtin::Command::JavaScript(args) = &command_args.command
				&& (args.engine.is_auto() || args.engine.is_v_8())
			{
				tangram_cli_builtin::javascript::initialize_v8(0);
			}
			Some(tangram_cli_builtin::run(&mut cli, command_args).await)
		},

		Command::Sandbox(tangram_cli_sandbox::Args {
			command: tangram_cli_sandbox::Command::Serve(command_args),
			..
		}) => {
			Cli::initialize_tracing(None, cli.args.tracing.as_ref(), None);
			let result = tangram_cli_sandbox::serve::run(command_args).await;
			Some(result)
		},

		Command::Sandbox(tangram_cli_sandbox::Args {
			command: tangram_cli_sandbox::Command::Init(command_args),
		}) => {
			Cli::initialize_tracing(None, cli.args.tracing.as_ref(), None);
			let result = tangram_cli_sandbox::init::run(command_args).map(|exit| {
				cli.exit = Some(exit);
			});
			Some(result)
		},

		_ => None,
	};
	if let Some(result) = result {
		return match result {
			Ok(()) => cli.exit.unwrap_or_default(),
			Err(error) => {
				let internal = Cli::read_config_with_path(cli.args.config.clone())
					.await
					.ok()
					.flatten()
					.and_then(|config| config.advanced)
					.and_then(|advanced| advanced.internal_error_locations)
					.unwrap_or(false);
				Cli::print_error_basic(tg::Referent::with_node(error), internal);
				std::process::ExitCode::FAILURE
			},
		};
	}

	// Override args from the serve args.
	if let Command::Serve(serve_args)
	| Command::Server(tangram_cli_server::Args {
		command: tangram_cli_server::Command::Run(serve_args),
		..
	}) = &command
	{
		if let Some(config) = serve_args.config.clone() {
			cli.args.config = Some(config);
		}
		if let Some(directory) = serve_args.directory.clone() {
			cli.args.directory = Some(directory);
		}
		cli.args.remotes.overlay(&serve_args.remotes);
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
			Cli::print_error_basic(tg::Referent::with_node(error), false);
			return std::process::ExitCode::FAILURE;
		},
	};

	let server = matches!(
		&command,
		Command::Serve(_)
			| Command::Server(tangram_cli_server::Args {
				command: tangram_cli_server::Command::Run(_),
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
			.and_then(|config| config.index.as_ref())
			.is_some_and(|index| matches!(index, tangram_cli_core::config::Index::Fdb(_)))
	{
		Some(unsafe { foundationdb::boot() })
	} else {
		None
	};

	// Initialize miette.
	Cli::initialize_miette();

	// Initialize Rustls.
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	// Initialize V8.
	#[cfg(feature = "v8")]
	let initialize_v8 = server || {
		#[cfg(feature = "v8")]
		{
			match &command {
				Command::Builtin(tangram_cli_builtin::Args {
					command: tangram_cli_builtin::Command::JavaScript(args),
				}) => args.engine.is_auto() || args.engine.is_v_8(),
				Command::Repl(args) => args.engine.is_auto() || args.engine.is_v_8(),
				_ => false,
			}
		}
		#[cfg(not(feature = "v8"))]
		{
			false
		}
	};
	#[cfg(feature = "v8")]
	if initialize_v8 {
		let thread_pool_size = cli
			.config
			.as_ref()
			.and_then(|config| config.v8_thread_pool_size)
			.unwrap_or(0);
		tangram_cli_builtin::javascript::initialize_v8(thread_pool_size);
	}

	// Initialize telemetry and tracing.
	let telemetry = Cli::initialize_telemetry(cli.config.as_ref());
	Cli::initialize_tracing(
		cli.config.as_ref(),
		cli.args.tracing.as_ref(),
		telemetry.as_ref(),
	);

	// Run the command.
	let result = run(&mut cli, command).await;

	// Handle the result.
	let exit = match result {
		Ok(()) => cli.exit.unwrap_or_default(),
		Err(error) => {
			Cli::print_error_message("an error occurred");
			let error = tg::Referent::with_node(error);
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

// Run the command.
async fn run(cli: &mut Cli, command: Command) -> tg::Result<()> {
	match command {
		Command::Archive(args) => tangram_cli_archive::run(cli, args).boxed_local().await,

		Command::Authorization(args) => tangram_cli_authorization::run(args),

		Command::Availability(args) => tangram_cli_availability::run(cli, args).boxed_local().await,

		Command::Build(args) => {
			tangram_cli_process::build::run(cli, args)
				.boxed_local()
				.await
		},

		Command::Builtin(args) => tangram_cli_builtin::run(cli, args).boxed_local().await,

		Command::Bundle(args) => tangram_cli_bundle::run(cli, args).boxed_local().await,

		Command::Cancel(args) => {
			tangram_cli_process::cancel::run(cli, args)
				.boxed_local()
				.await
		},

		Command::Check(args) => tangram_cli_check::run(cli, args).boxed_local().await,

		Command::Checkpoint(args) => tangram_cli_checkpoint::run(cli, args).boxed_local().await,

		Command::Checkin(args) => tangram_cli_checkin::run(cli, args).boxed_local().await,

		Command::Checkout(args) => tangram_cli_checkout::run(cli, args).boxed_local().await,

		Command::Checksum(args) => tangram_cli_checksum::run(cli, args).boxed_local().await,

		Command::Children(args) => tangram_cli_children::run(cli, args).boxed_local().await,

		Command::Clean(args) => tangram_cli_clean::run(cli, args).boxed_local().await,

		Command::Clock(args) => tangram_cli_clock::run(cli, args).boxed_local().await,

		Command::Compress(args) => tangram_cli_compress::run(cli, args).boxed_local().await,

		Command::Decompress(args) => tangram_cli_decompress::run(cli, args).boxed_local().await,

		Command::Document(args) => tangram_cli_document::run(cli, args).boxed_local().await,

		Command::Download(args) => tangram_cli_download::run(cli, args).boxed_local().await,

		Command::Exec(args) => {
			tangram_cli_process::exec::run(cli, args)
				.boxed_local()
				.await
		},

		Command::Extract(args) => tangram_cli_extract::run(cli, args).boxed_local().await,

		Command::Format(args) => tangram_cli_format::run(cli, args).boxed_local().await,

		Command::Get(args) => tangram_cli_get::run(cli, args).boxed_local().await,

		Command::Grant(args) => {
			tangram_cli_grants::create::run(cli, args)
				.boxed_local()
				.await
		},

		Command::Grants(args) => tangram_cli_grants::run(cli, args).boxed_local().await,

		Command::Group(args) => tangram_cli_group::run(cli, args).boxed_local().await,

		Command::Health(args) => tangram_cli_health::run(cli, args).boxed_local().await,

		Command::Id(args) => tangram_cli_id::run(args).boxed_local().await,

		Command::Index(args) => tangram_cli_index::run(cli, args).boxed_local().await,

		Command::Init(args) => tangram_cli_init::run(args).boxed_local().await,

		Command::Interface(args) => tangram_cli_interface::run(cli, args).boxed_local().await,

		Command::List(args) => tangram_cli_list::run(cli, args).boxed_local().await,

		Command::Login(args) => tangram_cli_user::login::run(cli, args).boxed_local().await,

		Command::Log(args) => {
			tangram_cli_process::stdio::read::run(cli, args)
				.boxed_local()
				.await
		},

		Command::Logout(args) => tangram_cli_user::logout::run(cli, args).boxed_local().await,

		Command::Lsp(args) => tangram_cli_lsp::run(cli, args).boxed_local().await,

		Command::Match(args) => tangram_cli_match::run(cli, args).boxed_local().await,

		Command::Metadata(args) => tangram_cli_metadata::run(cli, args).boxed_local().await,

		Command::New(args) => tangram_cli_new::run(args).boxed_local().await,

		Command::Object(args) => tangram_cli_object::run(cli, args).boxed_local().await,

		Command::Organization(args) => tangram_cli_organization::run(cli, args).boxed_local().await,

		Command::Outdated(args) => tangram_cli_outdated::run(cli, args).boxed_local().await,

		Command::Output(args) => {
			tangram_cli_process::output::run(cli, args)
				.boxed_local()
				.await
		},

		Command::Process(args) => tangram_cli_process::run(cli, args).boxed_local().await,

		Command::Ps(args) => {
			tangram_cli_sandbox::list::run(cli, args)
				.boxed_local()
				.await
		},

		Command::Publish(args) => tangram_cli_publish::run(cli, args).boxed_local().await,

		Command::Pull(args) => tangram_cli_pull::run(cli, args).boxed_local().await,

		Command::Push(args) => tangram_cli_push::run(cli, args).boxed_local().await,

		Command::Put(args) => tangram_cli_put::run(cli, args).boxed_local().await,

		Command::Read(args) => tangram_cli_read::run(cli, args).boxed_local().await,

		Command::Remote(args) => tangram_cli_remote::run(cli, args).boxed_local().await,

		Command::Revoke(args) => {
			tangram_cli_grants::delete::run(cli, args)
				.boxed_local()
				.await
		},

		#[cfg(any(feature = "quickjs", feature = "v8"))]
		Command::Repl(args) => tangram_cli_repl::run(cli, args).boxed_local().await,

		Command::Run(args) => tangram_cli_process::run::run(cli, args).boxed_local().await,

		Command::Runner(args) => tangram_cli_runner::run(cli, args).boxed_local().await,

		Command::Sandbox(args) => tangram_cli_sandbox::run(cli, args).boxed_local().await,

		Command::Self_(args) => tangram_cli_tangram::run(args).boxed_local().await,

		Command::Shell(args) => tangram_cli_shell::run(cli, args).boxed_local().await,

		Command::Serve(args) => tangram_cli_server::run::run(cli, args).boxed_local().await,

		Command::Server(args) => tangram_cli_server::run(cli, args).boxed_local().await,

		Command::Signal(args) => {
			tangram_cli_process::signal::run(cli, args)
				.boxed_local()
				.await
		},

		Command::Spawn(args) => {
			tangram_cli_process::spawn::run(cli, args)
				.boxed_local()
				.await
		},

		Command::Tag(args) => tangram_cli_tag::run(cli, args).boxed_local().await,

		Command::Touch(args) => tangram_cli_touch::run(cli, args).boxed_local().await,

		Command::Tree(args) => tangram_cli_tree::run(cli, args).boxed_local().await,

		Command::Update(args) => tangram_cli_update::run(cli, args).boxed_local().await,

		Command::User(args) => tangram_cli_user::run(cli, args).boxed_local().await,

		Command::Usage(args) => tangram_cli_usage::run(cli, args).boxed_local().await,

		Command::View(args) => tangram_cli_view::run(cli, args).boxed_local().await,

		Command::Wait(args) => tangram_cli_wait::run(cli, args).boxed_local().await,

		Command::Watch(args) => tangram_cli_watch::run(cli, args).boxed_local().await,

		Command::Whoami(args) => tangram_cli_user::whoami::run(cli, args).boxed_local().await,

		Command::Write(args) => tangram_cli_write::run(cli, args).boxed_local().await,
	}
}
