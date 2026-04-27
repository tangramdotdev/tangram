use {
	clap::{Parser, Subcommand, ValueEnum},
	std::{net::SocketAddr, path::PathBuf, process},
};

mod inspector;
mod protocol;
mod repl;
mod runner;

const DEFAULT_INSPECTOR_ADDRESS: &str = "127.0.0.1:9229";

fn main() {
	let cli = Cli::parse();
	let command = cli.command.unwrap_or_default();
	let result = match command {
		Command::Repl { engine } => {
			if matches!(engine, Engine::V8) {
				init_v8();
			}
			repl::run(engine);
			Ok(())
		},
		Command::Run {
			script,
			inspect,
			inspect_wait,
			inspect_brk,
		} => {
			init_v8();
			let inspector = inspector_options(inspect, inspect_wait, inspect_brk);
			runner::run(script, inspector)
		},
	};

	if let Err(error) = result {
		eprintln!("{error}");
		process::exit(1);
	}
}

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
	#[command(subcommand)]
	command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
	Repl {
		#[arg(default_value_t = Engine::V8)]
		engine: Engine,
	},

	Run {
		script: PathBuf,

		#[arg(long, value_name = "ADDR", default_missing_value = DEFAULT_INSPECTOR_ADDRESS, num_args = 0..=1, require_equals = true, conflicts_with_all = ["inspect_wait", "inspect_brk"])]
		inspect: Option<SocketAddr>,

		#[arg(long = "inspect-wait", value_name = "ADDR", default_missing_value = DEFAULT_INSPECTOR_ADDRESS, num_args = 0..=1, require_equals = true, conflicts_with_all = ["inspect", "inspect_brk"])]
		inspect_wait: Option<SocketAddr>,

		#[arg(long = "inspect-brk", value_name = "ADDR", default_missing_value = DEFAULT_INSPECTOR_ADDRESS, num_args = 0..=1, require_equals = true, conflicts_with_all = ["inspect", "inspect_wait"])]
		inspect_brk: Option<SocketAddr>,
	},
}

impl Default for Command {
	fn default() -> Self {
		Self::Repl { engine: Engine::V8 }
	}
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Engine {
	V8,
	#[value(name = "quickjs", alias = "quick-js")]
	QuickJs,
}

impl std::fmt::Display for Engine {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::V8 => f.write_str("v8"),
			Self::QuickJs => f.write_str("quickjs"),
		}
	}
}

fn inspector_options(
	inspect: Option<SocketAddr>,
	inspect_wait: Option<SocketAddr>,
	inspect_brk: Option<SocketAddr>,
) -> Option<inspector::Options> {
	if let Some(address) = inspect {
		Some(inspector::Options {
			address,
			mode: inspector::Mode::Inspect,
		})
	} else if let Some(address) = inspect_wait {
		Some(inspector::Options {
			address,
			mode: inspector::Mode::Wait,
		})
	} else {
		inspect_brk.map(|address| inspector::Options {
			address,
			mode: inspector::Mode::Break,
		})
	}
}

fn init_v8() {
	let platform = v8::new_default_platform(0, false).make_shared();
	v8::V8::initialize_platform(platform);
	v8::V8::initialize();
}
