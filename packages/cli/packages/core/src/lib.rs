use {std::path::PathBuf, tangram_client::prelude::*, tangram_uri::Uri, usage_rs as usage};

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
	pub argument_order: Vec<ArgumentKind>,
	pub client: Option<tg::Client>,
	pub config: Option<Config>,
	pub exit: Option<std::process::ExitCode>,
	pub health: Option<tg::Health>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArgumentKind {
	String,
	Value,
}

/// The global args.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// The path to the config file.
	#[usage(env = "TANGRAM_CONFIG", long, short)]
	pub config: Option<PathBuf>,

	/// Override the `directory` key in the config.
	#[usage(long, short)]
	pub directory: Option<PathBuf>,

	/// The mode.
	#[usage(env = "TANGRAM_MODE", long, short)]
	pub mode: Option<Mode>,

	#[usage(flatten)]
	pub quiet: Quiet,

	#[usage(flatten)]
	pub remotes: RemoteOptions,

	#[usage(long)]
	pub token: Option<String>,

	/// Override the tracing filter.
	#[usage(env = "TANGRAM_TRACING", long)]
	pub tracing: Option<String>,

	/// Override the HTTP listener URL in the config.
	#[usage(long, short)]
	pub url: Option<Uri>,
}

#[derive(Clone, Debug, Default, usage::Args)]
#[usage(args_override_self = false)]
pub struct RemoteOptions {
	/// Override the `remotes` key in the config.
	#[usage(conflicts("--remotes", "--trusted-remotes"), long = "no-remotes")]
	disabled: bool,

	/// Mark remotes as trusted.
	#[usage(conflicts("--no-remotes"), long = "trusted-remotes", delimiter = ',')]
	trusted: Option<Vec<String>>,

	/// Override the `remotes` key in the config.
	#[usage(
		conflicts("--no-remotes"),
		long = "remotes",
		short = 'r',
		delimiter = ','
	)]
	values: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, usage::Args)]
#[usage(args_override_self = false)]
pub struct Quiet {
	/// Whether to show progress and other helpful information.
	#[usage(
		default_missing = "true",
		long = "quiet",
		overrides("--no-quiet"),
		require_equals = true,
		short = 'q'
	)]
	quiet: Option<bool>,

	#[usage(
		default_missing = "true",
		long = "no-quiet",
		overrides("--quiet"),
		require_equals = true
	)]
	no_quiet: Option<bool>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	derive_more::Display,
	derive_more::FromStr,
	serde::Deserialize,
	serde::Serialize,
	usage::ValueEnum,
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

impl Args {
	/// Apply the environment for the inherited server.
	pub fn apply_server_env(&mut self) -> Result<(), usage::Error<'static, 'static>> {
		let explicit_server = self.directory.is_some() || self.url.is_some();
		if self.directory.is_none() {
			self.directory = std::env::var_os("TANGRAM_DIRECTORY").map(PathBuf::from);
		}
		let url = if self.url.is_none() {
			parse_env("TANGRAM_URL", "--url")?
		} else {
			None
		};
		let token = if self.token.is_none() {
			parse_env("TANGRAM_TOKEN", "--token")?
		} else {
			None
		};
		if !explicit_server {
			self.url = url;
			if self.token.is_none() {
				self.token = token;
			}
		}

		Ok(())
	}
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
	pub fn new(args: Args) -> Self {
		Self {
			args,
			argument_order: Vec::new(),
			client: None,
			config: None,
			exit: None,
			health: None,
		}
	}

	pub fn arguments(
		&self,
		strings: Vec<String>,
		values: Vec<String>,
	) -> impl Iterator<Item = (ArgumentKind, String)> + '_ {
		let mut strings = strings.into_iter();
		let mut values = values.into_iter();
		let order = if strings.len() > 0
			&& values.len() > 0
			&& self.argument_order.len() == strings.len() + values.len()
		{
			self.argument_order.as_slice()
		} else {
			&[]
		};
		let mut order = order.iter().copied();
		std::iter::from_fn(move || match order.next() {
			Some(ArgumentKind::String) => strings.next().map(|value| (ArgumentKind::String, value)),
			Some(ArgumentKind::Value) => values.next().map(|value| (ArgumentKind::Value, value)),
			None => strings
				.next()
				.map(|value| (ArgumentKind::String, value))
				.or_else(|| values.next().map(|value| (ArgumentKind::Value, value))),
		})
	}

	fn config_path(&self) -> Option<PathBuf> {
		self.args.config.clone().or_else(Self::default_config_path)
	}

	fn default_config_path() -> Option<PathBuf> {
		let directory = std::env::var_os("XDG_CONFIG_HOME")
			.map(PathBuf::from)
			.filter(|path| path.is_absolute())
			.or_else(|| std::env::home_dir().map(|home| home.join(".config")))?;
		let path = directory.join("tangram/config.json");
		Some(path)
	}

	pub fn directory_path(&self) -> tg::Result<PathBuf> {
		if let Some(directory) = &self.args.directory {
			return Ok(directory.clone());
		}
		let directory = self
			.config
			.as_ref()
			.and_then(|config| config.directory.as_ref());
		if let Some(directory) = directory {
			if !directory.is_absolute() {
				return Err(tg::error!(
					path = %directory.display(),
					"the config directory path must be absolute"
				));
			}
			return Ok(directory.clone());
		}
		let home = std::env::home_dir()
			.ok_or_else(|| tg::error!("failed to get the home directory, pass --directory"))?;
		let path = home.join(".tangram");
		Ok(path)
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

fn parse_env<T>(
	variable: &str,
	flag: &'static str,
) -> Result<Option<T>, usage::Error<'static, 'static>>
where
	T: std::str::FromStr,
	T::Err: std::fmt::Display,
{
	let Some(value) = std::env::var_os(variable) else {
		return Ok(None);
	};
	let value = String::from_utf8(value.into_encoded_bytes())
		.map_err(|error| usage::invalid_utf8_value(flag, error))?;
	let value = value
		.parse()
		.map_err(|error| usage::invalid_parsed_value(flag, value, &error))?;
	Ok(Some(value))
}
