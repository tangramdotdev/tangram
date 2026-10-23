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

	#[cfg(any(feature = "quickjs", feature = "v8"))]
	#[command(name = "javascript", alias = "js", hide = true)]
	JavaScript(tangram_cli_javascript::Args),

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

	#[cfg(feature = "cpython")]
	#[command(alias = "py")]
	Python(tangram_cli_python::Args),

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
			tangram_cli_sandbox::Command::Run(args) => {
				Some(tangram_cli_sandbox::Component::command_sandbox_run(args))
			},
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
		Command::Builtin(command_args) => Some(
			tangram_cli_builtin::Component::new(&mut cli)
				.command_builtin(command_args)
				.await,
		),

		#[cfg(any(feature = "quickjs", feature = "v8"))]
		Command::JavaScript(command_args) => {
			#[cfg(feature = "v8")]
			if command_args.engine.is_auto() || command_args.engine.is_v_8() {
				tangram_cli_javascript::initialize_v8(0);
			}
			let result = tangram_cli_javascript::Component::new(&mut cli)
				.command_javascript(command_args)
				.await;
			Some(result)
		},

		#[cfg(feature = "cpython")]
		Command::Python(command_args) => Some(
			tangram_cli_python::Component::new(&mut cli)
				.command_python(command_args)
				.await,
		),

		Command::Sandbox(tangram_cli_sandbox::Args {
			command: tangram_cli_sandbox::Command::Serve(command_args),
			..
		}) => {
			Cli::initialize_tracing(None, cli.args.tracing.as_ref(), None);
			let result = tangram_cli_sandbox::Component::new(&mut cli)
				.command_sandbox_serve(command_args)
				.await;
			Some(result)
		},

		Command::Sandbox(tangram_cli_sandbox::Args {
			command: tangram_cli_sandbox::Command::Init(command_args),
		}) => {
			Cli::initialize_tracing(None, cli.args.tracing.as_ref(), None);
			let result =
				tangram_cli_sandbox::Component::command_sandbox_init(command_args).map(|exit| {
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
				Command::JavaScript(args) => args.engine.is_auto() || args.engine.is_v_8(),
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
		tangram_cli_javascript::initialize_v8(thread_pool_size);
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
		Command::Archive(args) => {
			tangram_cli_archive::Component::new(cli)
				.command_archive(args)
				.boxed_local()
				.await
		},

		Command::Availability(args) => {
			tangram_cli_availability::Component::new(cli)
				.command_availability(args)
				.boxed_local()
				.await
		},

		Command::Build(args) => {
			tangram_cli_process::Component::new(cli)
				.command_build(args)
				.boxed_local()
				.await
		},

		Command::Builtin(args) => {
			tangram_cli_builtin::Component::new(cli)
				.command_builtin(args)
				.boxed_local()
				.await
		},

		Command::Bundle(args) => {
			tangram_cli_bundle::Component::new(cli)
				.command_bundle(args)
				.boxed_local()
				.await
		},

		Command::Cancel(args) => {
			tangram_cli_process::Component::new(cli)
				.command_process_cancel(args)
				.boxed_local()
				.await
		},

		Command::Check(args) => {
			tangram_cli_check::Component::new(cli)
				.command_check(args)
				.boxed_local()
				.await
		},

		Command::Checkpoint(args) => {
			tangram_cli_checkpoint::Component::new(cli)
				.command_checkpoint(args)
				.boxed_local()
				.await
		},

		Command::Checkin(args) => {
			tangram_cli_checkin::Component::new(cli)
				.command_checkin(args)
				.boxed_local()
				.await
		},

		Command::Checkout(args) => {
			tangram_cli_checkout::Component::new(cli)
				.command_checkout(args)
				.boxed_local()
				.await
		},

		Command::Checksum(args) => {
			tangram_cli_checksum::Component::new(cli)
				.command_checksum(args)
				.boxed_local()
				.await
		},

		Command::Children(args) => {
			tangram_cli_children::Component::new(cli)
				.command_children(args)
				.boxed_local()
				.await
		},

		Command::Clean(args) => {
			tangram_cli_clean::Component::new(cli)
				.command_clean(args)
				.boxed_local()
				.await
		},

		Command::Clock(args) => {
			tangram_cli_clock::Component::new(cli)
				.command_clock(args)
				.boxed_local()
				.await
		},

		Command::Compress(args) => {
			tangram_cli_compress::Component::new(cli)
				.command_compress(args)
				.boxed_local()
				.await
		},

		Command::Decompress(args) => {
			tangram_cli_decompress::Component::new(cli)
				.command_decompress(args)
				.boxed_local()
				.await
		},

		Command::Document(args) => {
			tangram_cli_document::Component::new(cli)
				.command_document(args)
				.boxed_local()
				.await
		},

		Command::Download(args) => {
			tangram_cli_download::Component::new(cli)
				.command_download(args)
				.boxed_local()
				.await
		},

		Command::Exec(args) => {
			tangram_cli_process::Component::new(cli)
				.command_process_exec(args)
				.boxed_local()
				.await
		},

		Command::Extract(args) => {
			tangram_cli_extract::Component::new(cli)
				.command_extract(args)
				.boxed_local()
				.await
		},

		Command::Format(args) => {
			tangram_cli_format::Component::new(cli)
				.command_format(args)
				.boxed_local()
				.await
		},

		Command::Get(args) => {
			tangram_cli_get::Component::new(cli)
				.command_get(args)
				.boxed_local()
				.await
		},

		Command::Grant(args) => {
			tangram_cli_grants::Component::new(cli)
				.command_grants_create(args)
				.boxed_local()
				.await
		},

		Command::Grants(args) => {
			tangram_cli_grants::Component::new(cli)
				.command_grants(args)
				.boxed_local()
				.await
		},

		Command::Group(args) => {
			tangram_cli_group::Component::new(cli)
				.command_group(args)
				.boxed_local()
				.await
		},

		Command::Health(args) => {
			tangram_cli_health::Component::new(cli)
				.command_health(args)
				.boxed_local()
				.await
		},

		Command::Id(args) => {
			tangram_cli_id::Component::new(cli)
				.command_id(args)
				.boxed_local()
				.await
		},

		Command::Index(args) => {
			tangram_cli_index::Component::new(cli)
				.command_index(args)
				.boxed_local()
				.await
		},

		Command::Init(args) => {
			tangram_cli_init::Component::new(cli)
				.command_init(args)
				.boxed_local()
				.await
		},

		#[cfg(any(feature = "quickjs", feature = "v8"))]
		Command::JavaScript(args) => {
			tangram_cli_javascript::Component::new(cli)
				.command_javascript(args)
				.boxed_local()
				.await
		},

		Command::List(args) => {
			tangram_cli_list::Component::new(cli)
				.command_list(args)
				.boxed_local()
				.await
		},

		Command::Login(args) => {
			tangram_cli_user::Component::new(cli)
				.command_user_login(args)
				.boxed_local()
				.await
		},

		Command::Log(args) => {
			tangram_cli_process::Component::new(cli)
				.command_process_stdio_read(args)
				.boxed_local()
				.await
		},

		Command::Logout(args) => {
			tangram_cli_user::Component::new(cli)
				.command_user_logout(args)
				.boxed_local()
				.await
		},

		Command::Lsp(args) => {
			tangram_cli_lsp::Component::new(cli)
				.command_lsp(args)
				.boxed_local()
				.await
		},

		Command::Match(args) => {
			tangram_cli_match::Component::new(cli)
				.command_match(args)
				.boxed_local()
				.await
		},

		Command::Metadata(args) => {
			tangram_cli_metadata::Component::new(cli)
				.command_metadata(args)
				.boxed_local()
				.await
		},

		Command::New(args) => {
			tangram_cli_new::Component::new(cli)
				.command_new(args)
				.boxed_local()
				.await
		},

		Command::Object(args) => {
			tangram_cli_object::Component::new(cli)
				.command_object(args)
				.boxed_local()
				.await
		},

		Command::Organization(args) => {
			tangram_cli_organization::Component::new(cli)
				.command_organization(args)
				.boxed_local()
				.await
		},

		Command::Outdated(args) => {
			tangram_cli_outdated::Component::new(cli)
				.command_outdated(args)
				.boxed_local()
				.await
		},

		Command::Output(args) => {
			tangram_cli_process::Component::new(cli)
				.command_process_output(args)
				.boxed_local()
				.await
		},

		Command::Process(args) => {
			tangram_cli_process::Component::new(cli)
				.command_process(args)
				.boxed_local()
				.await
		},

		Command::Ps(args) => {
			tangram_cli_sandbox::Component::new(cli)
				.command_sandbox_list(args)
				.boxed_local()
				.await
		},

		Command::Publish(args) => {
			tangram_cli_publish::Component::new(cli)
				.command_publish(args)
				.boxed_local()
				.await
		},

		Command::Pull(args) => {
			tangram_cli_pull::Component::new(cli)
				.command_pull(args)
				.boxed_local()
				.await
		},

		Command::Push(args) => {
			tangram_cli_push::Component::new(cli)
				.command_push(args)
				.boxed_local()
				.await
		},

		#[cfg(feature = "cpython")]
		Command::Python(args) => {
			tangram_cli_python::Component::new(cli)
				.command_python(args)
				.boxed_local()
				.await
		},

		Command::Put(args) => {
			tangram_cli_put::Component::new(cli)
				.command_put(args)
				.boxed_local()
				.await
		},

		Command::Read(args) => {
			tangram_cli_read::Component::new(cli)
				.command_read(args)
				.boxed_local()
				.await
		},

		Command::Remote(args) => {
			tangram_cli_remote::Component::new(cli)
				.command_remote(args)
				.boxed_local()
				.await
		},

		Command::Revoke(args) => {
			tangram_cli_grants::Component::new(cli)
				.command_grants_delete(args)
				.boxed_local()
				.await
		},

		#[cfg(any(feature = "quickjs", feature = "v8"))]
		Command::Repl(args) => {
			tangram_cli_repl::Component::new(cli)
				.command_repl(args)
				.boxed_local()
				.await
		},

		Command::Run(args) => {
			tangram_cli_process::Component::new(cli)
				.command_run(args)
				.boxed_local()
				.await
		},

		Command::Runner(args) => {
			tangram_cli_runner::Component::new(cli)
				.command_runner(args)
				.boxed_local()
				.await
		},

		Command::Sandbox(args) => {
			tangram_cli_sandbox::Component::new(cli)
				.command_sandbox(args)
				.boxed_local()
				.await
		},

		Command::Self_(args) => {
			tangram_cli_tangram::Component::new(cli)
				.command_tangram(args)
				.boxed_local()
				.await
		},

		Command::Shell(args) => {
			tangram_cli_shell::Component::new(cli)
				.command_shell(args)
				.boxed_local()
				.await
		},

		Command::Serve(args) => tangram_cli_server::run::run(cli, args).boxed_local().await,

		Command::Server(args) => tangram_cli_server::server(cli, args).boxed_local().await,

		Command::Signal(args) => {
			tangram_cli_process::Component::new(cli)
				.command_process_signal(args)
				.boxed_local()
				.await
		},

		Command::Spawn(args) => {
			tangram_cli_process::Component::new(cli)
				.command_process_spawn(args)
				.boxed_local()
				.await
		},

		Command::Tag(args) => {
			tangram_cli_tag::Component::new(cli)
				.command_tag(args)
				.boxed_local()
				.await
		},

		Command::Touch(args) => {
			tangram_cli_touch::Component::new(cli)
				.command_touch(args)
				.boxed_local()
				.await
		},

		Command::Tree(args) => {
			tangram_cli_tree::Component::new(cli)
				.command_tree(args)
				.boxed_local()
				.await
		},

		Command::Update(args) => {
			tangram_cli_update::Component::new(cli)
				.command_update(args)
				.boxed_local()
				.await
		},

		Command::User(args) => {
			tangram_cli_user::Component::new(cli)
				.command_user(args)
				.boxed_local()
				.await
		},

		Command::Usage(args) => {
			tangram_cli_usage::Component::new(cli)
				.command_usage(args)
				.boxed_local()
				.await
		},

		Command::View(args) => {
			tangram_cli_view::Component::new(cli)
				.command_view(args)
				.boxed_local()
				.await
		},

		Command::Wait(args) => {
			tangram_cli_wait::Component::new(cli)
				.command_wait(args)
				.boxed_local()
				.await
		},

		Command::Watch(args) => {
			tangram_cli_watch::Component::new(cli)
				.command_watch(args)
				.boxed_local()
				.await
		},

		Command::Whoami(args) => {
			tangram_cli_user::Component::new(cli)
				.command_user_whoami(args)
				.boxed_local()
				.await
		},

		Command::Write(args) => {
			tangram_cli_write::Component::new(cli)
				.command_write(args)
				.boxed_local()
				.await
		},
	}
}
