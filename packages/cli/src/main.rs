#![recursion_limit = "256"]

use {
	futures::FutureExt as _,
	std::ffi::{OsStr, OsString},
	tangram_cli_core::{ArgumentKind, Cli},
	tangram_client::prelude::*,
	usage_rs::{DoubleDash, Error, Event, Parser},
};

// Include the FoundationDB native library search path.
#[cfg(feature = "foundationdb")]
use tangram_fdb as _;

#[cfg(test)]
mod tests;

#[derive(Debug, usage_rs::Cli)]
#[usage(
	about = "Tangram is a build system and package manager.",
	after_help = "Run a process directly with: tg <reference> [options] [-- args]",
	arg_required_else_help,
	args_override_self = false,
	bin = "tangram",
	completion,
	default_subcommand = "run",
	disable_help_subcommand,
	unknown_flags = "error",
	version
)]
struct Args {
	#[usage(subcommand)]
	command: Command,

	#[usage(flatten)]
	global: tangram_cli_core::Args,
}

#[derive(Debug, usage_rs::Subcommands)]
enum Command {
	Archive(tangram_cli_archive::Args),

	Authorization(tangram_cli_authorization::Args),

	Availability(tangram_cli_availability::Args),

	#[usage(alias = "b")]
	Build(tangram_cli_process::build::Args),

	#[usage(hide = true)]
	Builtin(tangram_cli_builtin::Args),

	Bundle(tangram_cli_bundle::Args),

	Cancel(tangram_cli_process::cancel::Args),

	Check(tangram_cli_check::Args),

	#[usage(hide = true)]
	Checkpoint(tangram_cli_checkpoint::Args),

	#[usage(alias = "ci")]
	Checkin(tangram_cli_checkin::Args),

	#[usage(alias = "co")]
	Checkout(tangram_cli_checkout::Args),

	Checksum(tangram_cli_checksum::Args),

	Clean(tangram_cli_clean::Args),

	#[usage(hide = true)]
	Clock(tangram_cli_clock::Args),

	Compress(tangram_cli_compress::Args),

	Decompress(tangram_cli_decompress::Args),

	#[usage(alias = "doc")]
	Document(tangram_cli_document::Args),

	Download(tangram_cli_download::Args),

	Exec(tangram_cli_process::exec::Args),

	Extract(tangram_cli_extract::Args),

	Format(tangram_cli_format::Args),

	Get(tangram_cli_get::Args),

	Grant(tangram_cli_grants::grant::Args),

	Grants(tangram_cli_grants::Args),

	#[usage(alias = "groups")]
	Group(tangram_cli_group::Args),

	Health(tangram_cli_health::Args),

	#[usage(hide = true)]
	Id(tangram_cli_id::Args),

	Index(tangram_cli_index::Args),

	Init(tangram_cli_init::Args),

	Interface(tangram_cli_interface::Args),

	#[usage(alias = "ls")]
	List(tangram_cli_list::Args),

	Login(tangram_cli_user::login::Args),

	Log(tangram_cli_process::stdio::read::Args),

	Logout(tangram_cli_user::logout::Args),

	Lsp(tangram_cli_lsp::Args),

	Match(tangram_cli_match::Args),

	Metadata(tangram_cli_metadata::Args),

	New(tangram_cli_new::Args),

	#[usage(alias = "obj")]
	Object(tangram_cli_object::Args),

	#[usage(alias = "org")]
	Organization(tangram_cli_organization::Args),

	Outdated(tangram_cli_outdated::Args),

	Output(tangram_cli_process::output::Args),

	#[usage(alias = "pcs")]
	Process(tangram_cli_process::Args),

	Ps(tangram_cli_process::list::Args),

	Publish(tangram_cli_publish::Args),

	Pull(tangram_cli_pull::Args),

	Push(tangram_cli_push::Args),

	#[usage(alias = "add")]
	Put(tangram_cli_put::Args),

	#[usage(alias = "cat")]
	Read(tangram_cli_read::Args),

	Remote(tangram_cli_remote::Args),

	Revoke(tangram_cli_grants::revoke::Args),

	#[cfg(any(feature = "quickjs", feature = "v8"))]
	Repl(tangram_cli_repl::Args),

	#[usage(alias = "r")]
	Run(tangram_cli_process::run::Args),

	Runner(tangram_cli_runner::Args),

	#[usage(alias = "sbx")]
	Sandbox(tangram_cli_sandbox::Args),

	#[usage(name = "self")]
	Self_(tangram_cli_tangram::Args),

	Shell(tangram_cli_shell::Args),

	Serve(tangram_cli_server::run::Args),

	Server(tangram_cli_server::Args),

	#[usage(alias = "kill")]
	Signal(tangram_cli_process::signal::Args),

	Spawn(tangram_cli_process::spawn::Args),

	Tag(tangram_cli_tag::Args),

	Touch(tangram_cli_touch::Args),

	#[usage(hide = true)]
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
	let argv = std::env::args_os().collect::<Vec<OsString>>();
	let argv = argv.get(1..).unwrap_or_default();
	if let Some(output) = Args::completion_request(argv) {
		print!("{output}");
		return std::process::ExitCode::SUCCESS;
	}
	let mut argv = argv.iter().map(OsString::as_os_str).collect();
	let (args, argument_order) = match parse_args(&mut argv) {
		Ok(output) => output,
		Err(error) => return render_parse_error(&argv, &error),
	};

	// Run native sandbox commands before initializing the runtime.
	let result = match args.command {
		#[cfg(target_os = "linux")]
		Command::Sandbox(tangram_cli_sandbox::Args {
			command: tangram_cli_sandbox::Command::Configure(args),
		}) => tangram_cli_sandbox::configure::run(args),
		Command::Sandbox(tangram_cli_sandbox::Args {
			command: tangram_cli_sandbox::Command::Run(args),
		}) => tangram_cli_sandbox::run::run(args),
		command => {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.unwrap();
			return runtime.block_on(main_inner(args.global, argument_order, command));
		},
	};

	match result {
		Ok(exit) => exit,
		Err(error) => {
			Cli::print_error_basic(tg::Referent::with_node(error), false);
			std::process::ExitCode::FAILURE
		},
	}
}

fn parse_args<'a>(
	argv: &mut Vec<&'a OsStr>,
) -> Result<(Args, Vec<ArgumentKind>), Error<'static, 'a>> {
	let mut args = match Args::parse_from(argv) {
		Err(error @ Error::UnexpectedArg { .. }) => {
			let Some(index) = implicit_run_offset(argv) else {
				return Err(error);
			};
			argv.insert(index, OsStr::new("run"));
			Args::parse_from(argv)?
		},
		result => result?,
	};
	if let Command::Sandbox(tangram_cli_sandbox::Args {
		command: tangram_cli_sandbox::Command::Run(args),
	}) = &args.command
	{
		args.validate()?;
	}
	args.global.apply_server_env()?;
	let (argument_order, separator) = inspect_args(argv)?;
	if let Some(index) = separator {
		insert_trailing_separator(&mut args.command, index);
	}
	Ok((args, argument_order))
}

fn implicit_run_offset(argv: &[&OsStr]) -> Option<usize> {
	let command = Args::command();
	for (index, word) in argv.iter().enumerate() {
		if *word != "--" && *word != "-" {
			continue;
		}
		let mut parser = Parser::new(command, &argv[..=index]);
		let error = loop {
			match parser.next_event() {
				Some(Ok(_)) => (),
				Some(Err(error)) => break Some(error),
				None => break None,
			}
		};
		if !std::ptr::eq(parser.command(), command) {
			return None;
		}
		if parser.double_dash_seen() || matches!(error, Some(Error::UnexpectedArg { token: b"-" }))
		{
			return Some(index);
		}
	}
	None
}

fn inspect_args<'a>(
	argv: &[&'a OsStr],
) -> Result<(Vec<ArgumentKind>, Option<usize>), Error<'static, 'a>> {
	// Skip the event walk when argv cannot need compatibility handling.
	let mut has_string = false;
	let mut has_value = false;
	let mut has_empty_value = false;
	let mut has_separator = false;
	let mut attachments = Vec::new();
	for word in argv {
		let word = word.as_encoded_bytes();
		has_separator |= word == b"--";
		has_empty_value |= word.starts_with(b"-") && word.ends_with(b"=");
		if let Some(long) = word.strip_prefix(b"--") {
			let name = if let Some(index) = long.iter().position(|byte| *byte == b'=') {
				attachments.push((word, &long[index + 1..]));
				&long[..index]
			} else {
				long
			};
			has_string |= name == b"arg-string";
			has_value |= name == b"arg-value";
		} else if let Some(short) = word.strip_prefix(b"-") {
			has_string |= short.contains(&b'a');
			has_value |= short.contains(&b'A');
		}
	}
	let mixed = has_string && has_value;
	if !mixed && !has_empty_value && !has_separator && attachments.is_empty() {
		return Ok((Vec::new(), None));
	}

	// Read the flag order and keep a separator typed after trailing arguments began.
	let mut order = Vec::new();
	let mut separator = None;
	let mut trailing = 0;
	let mut parser = Parser::new(Args::command(), argv);
	loop {
		let after_trailing = parser.flags_stopped() && !parser.double_dash_seen();
		let event = parser.next_event().transpose()?;
		if after_trailing && parser.double_dash_seen() {
			separator = Some(trailing);
		}
		// Remove attachments consumed as either a flag value or literal input.
		if let Some(
			Event::Flag {
				value: Some(value), ..
			}
			| Event::Arg { value, .. },
		) = event
		{
			attachments.retain(|(word, attached)| {
				!std::ptr::eq(value, *word) && !std::ptr::eq(value, *attached)
			});
		}
		match event {
			Some(Event::Arg { arg, .. }) if arg.double_dash == DoubleDash::Automatic => {
				trailing += 1;
			},
			Some(Event::Flag { flag, value, .. }) => match flag.longs.first().copied() {
				Some("arg-string") if mixed => order.push(ArgumentKind::String),
				Some("arg-value") if mixed => order.push(ArgumentKind::Value),
				Some("checkout") if value == Some(b"") => {
					let error = usage_rs::ValidationError::field("--checkout")
						.reason("expected a non-empty path");
					return Err(error.into_parse_error());
				},
				_ => (),
			},
			Some(_) => (),
			None => break,
		}
	}
	if let Some(&(token, _)) = attachments.first() {
		return Err(Error::UnexpectedArg { token });
	}

	Ok((order, separator))
}

fn insert_trailing_separator(command: &mut Command, index: usize) {
	match command {
		Command::Build(args) | Command::Run(args) => args.trailing.insert_separator(index),
		Command::Exec(args)
		| Command::Spawn(args)
		| Command::Process(tangram_cli_process::Args {
			command:
				tangram_cli_process::Command::Exec(args) | tangram_cli_process::Command::Spawn(args),
		}) => args.trailing.insert_separator(index),
		Command::Sandbox(tangram_cli_sandbox::Args {
			command: tangram_cli_sandbox::Command::Run(args),
		}) => args.options.command.insert(index, OsString::from("--")),
		Command::Builtin(args) => match &mut args.command {
			tangram_cli_builtin::Command::Config(args) => {
				args.trailing.insert(index, "--".to_owned());
			},
			#[cfg(any(feature = "quickjs", feature = "v8"))]
			tangram_cli_builtin::Command::JavaScript(args) => args.trailing.insert(index, "--".to_owned()),
			#[cfg(feature = "cpython")]
			tangram_cli_builtin::Command::Python(args) => args.trailing.insert(index, "--".to_owned()),
			_ => (),
		},
		_ => (),
	}
}

fn render_parse_error(argv: &[&OsStr], error: &Error<'static, '_>) -> std::process::ExitCode {
	match error {
		Error::Help { cmd, long } => {
			if std::ptr::eq(*cmd, Args::command()) {
				println!("{}\n", tangram_cli_core::before_help());
			}
			print!("{}", Args::render_help(cmd, *long).unwrap_or_default());
			std::process::ExitCode::SUCCESS
		},
		Error::MissingArgsHelp { cmd } if std::ptr::eq(*cmd, Args::command()) => {
			eprintln!("{}\n", tangram_cli_core::before_help());
			eprint!("{}", Args::render_failure(argv, error));
			std::process::ExitCode::from(2)
		},
		Error::Version { .. } => {
			println!("tangram {}", tangram_cli_core::version());
			std::process::ExitCode::SUCCESS
		},
		_ => {
			eprint!("{}", Args::render_failure(argv, error));
			std::process::ExitCode::from(2)
		},
	}
}

async fn main_inner(
	args: tangram_cli_core::Args,
	argument_order: Vec<ArgumentKind>,
	command: Command,
) -> std::process::ExitCode {
	// Create the CLI.
	let mut cli = Cli::new(args);
	cli.argument_order = argument_order;

	// Handle internal commands.
	let command = match command {
		Command::Builtin(command_args) => {
			#[cfg(feature = "v8")]
			if let tangram_cli_builtin::Command::JavaScript(args) = &command_args.command
				&& (args.engine.is_auto() || args.engine.is_v_8())
			{
				tangram_cli_builtin::javascript::initialize_v8(0);
			}
			let result = tangram_cli_builtin::run(&mut cli, command_args).await;
			return finish_internal_command(&cli, result);
		},

		Command::Sandbox(tangram_cli_sandbox::Args {
			command: tangram_cli_sandbox::Command::Serve(command_args),
			..
		}) => {
			Cli::initialize_tracing(None, cli.args.tracing.as_ref(), None);
			let result = tangram_cli_sandbox::serve::run(command_args).await;
			return finish_internal_command(&cli, result);
		},

		Command::Sandbox(tangram_cli_sandbox::Args {
			command: tangram_cli_sandbox::Command::Init(command_args),
		}) => {
			Cli::initialize_tracing(None, cli.args.tracing.as_ref(), None);
			let result = tangram_cli_sandbox::init::run(command_args).map(|exit| {
				cli.exit = Some(exit);
			});
			return finish_internal_command(&cli, result);
		},

		command => command,
	};

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
	cli.config = match Cli::try_read_config(cli.args.config.as_deref()) {
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
fn finish_internal_command(cli: &Cli, result: tg::Result<()>) -> std::process::ExitCode {
	match result {
		Ok(()) => cli.exit.unwrap_or_default(),
		Err(error) => {
			let internal = Cli::try_read_config(cli.args.config.as_deref())
				.ok()
				.flatten()
				.and_then(|config| config.advanced)
				.and_then(|advanced| advanced.internal_error_locations)
				.unwrap_or(false);
			Cli::print_error_basic(tg::Referent::with_node(error), internal);
			std::process::ExitCode::FAILURE
		},
	}
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
			tangram_cli_grants::grant::run(cli, args)
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
			tangram_cli_process::list::run(cli, args)
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
			tangram_cli_grants::revoke::run(cli, args)
				.boxed_local()
				.await
		},

		#[cfg(any(feature = "quickjs", feature = "v8"))]
		Command::Repl(args) => tangram_cli_repl::run(cli, args).boxed_local().await,

		Command::Run(args) => tangram_cli_process::run::run(cli, args).boxed_local().await,

		Command::Runner(args) => tangram_cli_runner::run(cli, args).boxed_local().await,

		Command::Sandbox(args) => tangram_cli_sandbox::run(cli, args).boxed_local().await,

		Command::Self_(args) => tangram_cli_tangram::run(args).boxed_local().await,

		Command::Shell(args) => {
			tangram_cli_shell::run(cli, args, completion_script)
				.boxed_local()
				.await
		},

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

fn completion_script(shell: tangram_cli_shell::completion::Shell) -> String {
	let shell = match shell {
		tangram_cli_shell::completion::Shell::Bash => usage_rs::complete::Shell::Bash,
		tangram_cli_shell::completion::Shell::Fish => usage_rs::complete::Shell::Fish,
		tangram_cli_shell::completion::Shell::Nu => usage_rs::complete::Shell::Nu,
		tangram_cli_shell::completion::Shell::Zsh => usage_rs::complete::Shell::Zsh,
	};
	let mut script = Args::completion_script(shell);
	script.push_str(&Args::completion_script_for_alias("tg", shell));
	if shell == usage_rs::complete::Shell::Nu {
		script = format!("export-env {{\n{script}\n}}\nexport alias tg = tangram\n");
	}
	script
}
