use {std::path::PathBuf, tangram_client::prelude::*, tangram_uri::Uri};

pub use self::config::Config;

pub mod browser;
pub mod client;
pub mod config;
pub mod error;
pub mod location;
pub mod node;
pub mod print;
pub mod progress;
pub mod server;
pub mod telemetry;
pub mod theme;
pub mod tracing;

pub struct Cli {
	pub args: Args,
	pub client: Option<tg::Client>,
	pub command: clap::Command,
	pub config: Option<Config>,
	pub exit: Option<std::process::ExitCode>,
	pub health: Option<tg::Health>,
	pub matches: clap::ArgMatches,
}

/// The global args.
#[derive(Clone, Debug, clap::Args)]
pub struct Args {
	/// The path to the config file.
	#[arg(env = "TANGRAM_CONFIG", long, short)]
	pub config: Option<PathBuf>,

	/// Override the `directory` key in the config.
	#[arg(env = "TANGRAM_DIRECTORY", long, short)]
	pub directory: Option<PathBuf>,

	/// The mode.
	#[arg(default_value_t, env = "TANGRAM_MODE", long, short)]
	pub mode: Mode,

	#[clap(flatten)]
	pub quiet: Quiet,

	#[command(flatten)]
	pub remotes: RemoteOptions,

	#[arg(env = "TANGRAM_TOKEN", long)]
	pub token: Option<String>,

	/// Override the tracing filter.
	#[arg(env = "TANGRAM_TRACING", long)]
	pub tracing: Option<String>,

	/// Override the HTTP listener URL in the config.
	#[arg(env = "TANGRAM_URL", long, short)]
	pub url: Option<Uri>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct RemoteOptions {
	/// Override the `remotes` key in the config.
	#[arg(
		conflicts_with_all = ["main.remotes.remotes", "main.remotes.trusted_remotes"],
		id = "main.remotes.no_remotes",
		long = "no-remotes"
	)]
	disabled: bool,

	/// Mark remotes as trusted.
	#[arg(
		conflicts_with = "main.remotes.no_remotes",
		id = "main.remotes.trusted_remotes",
		long = "trusted-remotes",
		value_delimiter = ','
	)]
	trusted: Option<Vec<String>>,

	/// Override the `remotes` key in the config.
	#[arg(
		conflicts_with = "main.remotes.no_remotes",
		id = "main.remotes.remotes",
		long = "remotes",
		short = 'r',
		value_delimiter = ','
	)]
	values: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Quiet {
	/// Whether to show progress and other helpful information.
	#[arg(
		default_missing_value = "true",
		id = "main.quiet.quiet",
		long = "quiet",
		num_args = 0..=1,
		overrides_with = "main.quiet.no_quiet",
		require_equals = true,
		short = 'q',
	)]
	quiet: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "main.quiet.no_quiet",
		long = "no-quiet",
		num_args = 0..=1,
		overrides_with = "main.quiet.quiet",
		require_equals = true,
	)]
	no_quiet: Option<bool>,
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
pub enum Mode {
	#[default]
	Auto,

	Client,
}

#[must_use]
pub fn before_help() -> String {
	let version = version();
	let logo = include_str!("tangram.ascii").trim_end();
	format!("Tangram {version}\n\n{logo}")
}

#[must_use]
pub fn version() -> String {
	let mut version = env!("CARGO_PKG_VERSION").to_owned();
	if let Some(commit) = option_env!("TANGRAM_CLI_COMMIT_HASH") {
		version.push('+');
		version.push_str(commit);
	}
	version
}

#[must_use]
pub fn source_parser() -> impl clap::builder::TypedValueParser<Value = tg::process::Source> {
	use clap::builder::TypedValueParser as _;
	clap::builder::PossibleValuesParser::new(["auto", "runner", "index"])
		.map(|value| serde_json::from_value(serde_json::Value::String(value)).unwrap())
}

impl RemoteOptions {
	pub fn overlay(&mut self, options: &Self) {
		if options.disabled {
			*self = options.clone();
			return;
		}
		if options.trusted.is_some() || options.values.is_some() {
			self.disabled = false;
		}
		if let Some(trusted) = &options.trusted {
			self.trusted = Some(trusted.clone());
		}
		if let Some(values) = &options.values {
			self.values = Some(values.clone());
		}
	}

	#[must_use]
	pub fn values(&self) -> Option<&[String]> {
		if self.disabled {
			Some(&[])
		} else {
			self.values.as_deref()
		}
	}

	#[must_use]
	pub fn trusted(&self) -> Option<&[String]> {
		self.trusted.as_deref()
	}
}

impl Quiet {
	#[must_use]
	pub fn get(&self) -> bool {
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

impl Cli {
	#[must_use]
	pub fn new(args: Args, command: clap::Command, matches: clap::ArgMatches) -> Self {
		Self {
			args,
			client: None,
			command,
			config: None,
			exit: None,
			health: None,
			matches,
		}
	}

	fn config_path(&self) -> PathBuf {
		self.args.config.clone().unwrap_or_else(|| {
			PathBuf::from(std::env::var("HOME").unwrap()).join(".config/tangram/config.json")
		})
	}

	#[must_use]
	pub fn directory_path(&self) -> PathBuf {
		self.args
			.directory
			.clone()
			.or_else(|| {
				self.config
					.as_ref()
					.and_then(|config| config.directory.clone())
			})
			.unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram"))
	}

	/// Initialize miette.
	pub fn initialize_miette() {
		miette::set_hook(Box::new(|_| {
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
			let highlighter = miette::highlighters::SyntectHighlighter::new_themed(
				crate::theme::tangram(),
				false,
			);
			let handler = miette::GraphicalReportHandler::new()
				.with_theme(theme)
				.with_syntax_highlighting(highlighter);

			Box::new(handler)
		}))
		.unwrap();
	}

	pub fn set_file_descriptor_limit() -> tg::Result<()> {
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

	#[cfg(any(feature = "cpython", feature = "quickjs", feature = "v8"))]
	pub async fn spawn_thread<F, T>(f: F) -> tg::Result<T>
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
