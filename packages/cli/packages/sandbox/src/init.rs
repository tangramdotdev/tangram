use {crate::Component, std::path::PathBuf, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long, default_value = "builtin", value_parser = crate::parse_backend)]
	pub backend: tangram_sandbox::Kind,

	#[command(flatten)]
	pub options: Options,
}

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Options {
	#[arg(long)]
	pub http2_connection_window_size: Option<u32>,

	#[arg(long)]
	pub http2_max_concurrent_reset_streams: Option<usize>,

	#[arg(long)]
	pub http2_max_concurrent_streams: Option<u32>,

	#[arg(long)]
	pub http2_stream_window_size: Option<u32>,

	#[arg(action = clap::ArgAction::Append, long = "library-path", num_args = 1)]
	pub library_paths: Vec<PathBuf>,

	#[arg(long)]
	pub listen: bool,

	#[arg(long)]
	pub output_path: Option<PathBuf>,

	#[arg(long)]
	pub tangram_path: Option<PathBuf>,

	#[arg(long)]
	pub url: Option<tangram_uri::Uri>,
}

impl Component<'_> {
	pub fn command_sandbox_init(args: Args) -> tg::Result<std::process::ExitCode> {
		let arg = args.arg()?;
		tangram_sandbox::init::run(&arg)
	}
}

impl Args {
	fn arg(self) -> tg::Result<tangram_sandbox::init::Arg> {
		let arg = match self.backend {
			#[cfg(target_os = "linux")]
			tangram_sandbox::Kind::Builtin
			| tangram_sandbox::Kind::Bubblewrap
			| tangram_sandbox::Kind::Gvisor => {
				let serve = self.options.serve()?;
				match self.backend {
					tangram_sandbox::Kind::Builtin => {
						tangram_sandbox::init::Arg::Builtin(tangram_sandbox::builtin::init::Arg {
							serve,
						})
					},
					tangram_sandbox::Kind::Bubblewrap => tangram_sandbox::init::Arg::Bubblewrap(
						tangram_sandbox::bubblewrap::init::Arg { serve },
					),
					tangram_sandbox::Kind::Gvisor => {
						tangram_sandbox::init::Arg::Gvisor(tangram_sandbox::gvisor::init::Arg {
							serve,
						})
					},
					_ => unreachable!(),
				}
			},

			#[cfg(target_os = "linux")]
			tangram_sandbox::Kind::CloudHypervisor => tangram_sandbox::init::Arg::CloudHypervisor(
				tangram_sandbox::cloud_hypervisor::init::Arg,
			),

			#[cfg(target_os = "macos")]
			tangram_sandbox::Kind::Seatbelt => {
				tangram_sandbox::init::Arg::Seatbelt(tangram_sandbox::seatbelt::init::Arg {
					serve: self.options.serve()?,
				})
			},

			#[cfg(target_os = "linux")]
			tangram_sandbox::Kind::Seatbelt => {
				return Err(tg::error!("the seatbelt backend requires macos"));
			},

			#[cfg(target_os = "macos")]
			_ => return Err(tg::error!("the selected backend requires linux")),
		};

		Ok(arg)
	}
}

impl Options {
	fn serve(self) -> tg::Result<tangram_sandbox::serve::Arg> {
		// Resolve the HTTP/2 settings.
		let mut http2 = tangram_http::http2::Config::default();
		if let Some(value) = self.http2_connection_window_size {
			http2.connection_window_size = value;
		}
		if let Some(value) = self.http2_max_concurrent_reset_streams {
			http2.max_concurrent_reset_streams = value;
		}
		if let Some(value) = self.http2_max_concurrent_streams {
			http2.max_concurrent_streams = Some(value);
		}
		if let Some(value) = self.http2_stream_window_size {
			http2.stream_window_size = value;
		}

		// Create the server configuration.
		let output_path = self
			.output_path
			.ok_or_else(|| tg::error!("the sandbox output path is required"))?;
		let tangram_path = self
			.tangram_path
			.ok_or_else(|| tg::error!("the tangram executable path is required"))?;
		let url = self
			.url
			.ok_or_else(|| tg::error!("the sandbox URL is required"))?;
		let arg = tangram_sandbox::serve::Arg {
			http2,
			library_paths: self.library_paths,
			listen: self.listen,
			output_path,
			tangram_path,
			url,
		};

		Ok(arg)
	}
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
	use {super::*, clap::Parser as _};

	#[derive(clap::Parser)]
	struct Parser {
		#[command(flatten)]
		args: Args,
	}

	#[test]
	fn cloud_hypervisor_init_reads_its_guest_configuration() {
		let args = Parser::try_parse_from(["test", "--backend", "cloud-hypervisor"])
			.unwrap()
			.args;
		assert!(matches!(
			args.arg().unwrap(),
			tangram_sandbox::init::Arg::CloudHypervisor(_)
		));
	}

	#[test]
	fn linux_init_dispatch_preserves_the_server_settings() {
		for backend in ["builtin", "bubblewrap", "gvisor"] {
			// Parse the backend init configuration.
			let args = Parser::try_parse_from([
				"test",
				"--backend",
				backend,
				"--output-path",
				"/output",
				"--tangram-path",
				"/tangram",
				"--url",
				"http+stdio://localhost",
				"--http2-connection-window-size",
				"1234",
				"--http2-max-concurrent-reset-streams",
				"5",
				"--http2-max-concurrent-streams",
				"6",
				"--http2-stream-window-size",
				"7890",
			])
			.unwrap()
			.args;

			// Resolve the backend init configuration.
			let (kind, serve) = match args.arg().unwrap() {
				tangram_sandbox::init::Arg::Builtin(arg) => ("builtin", arg.serve),
				tangram_sandbox::init::Arg::Bubblewrap(arg) => ("bubblewrap", arg.serve),
				tangram_sandbox::init::Arg::CloudHypervisor(_) => {
					panic!("expected a Linux init configuration")
				},
				tangram_sandbox::init::Arg::Gvisor(arg) => ("gvisor", arg.serve),
			};

			// Verify the server settings.
			assert_eq!(kind, backend);
			assert!(!serve.listen);
			assert_eq!(serve.output_path, PathBuf::from("/output"));
			assert_eq!(serve.tangram_path, PathBuf::from("/tangram"));
			assert_eq!(serve.url.to_string(), "http+stdio://localhost");
			assert_eq!(serve.http2.connection_window_size, 1234);
			assert_eq!(serve.http2.max_concurrent_reset_streams, 5);
			assert_eq!(serve.http2.max_concurrent_streams, Some(6));
			assert_eq!(serve.http2.stream_window_size, 7890);
		}
	}

	#[test]
	fn linux_init_requires_the_server_paths() {
		let args = Parser::try_parse_from(["test", "--backend", "builtin"])
			.unwrap()
			.args;
		assert!(args.arg().is_err());
	}
}
