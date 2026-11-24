use {std::path::PathBuf, tangram_client::prelude::*, usage_rs as usage};

/// Serve sandbox requests.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub http2: super::Http2,

	#[usage(long = "library-path")]
	pub library_paths: Vec<PathBuf>,

	#[usage(flatten)]
	pub listen: Listen,

	#[usage(long, hide = true)]
	pub namespaces: Option<PathBuf>,

	#[usage(long)]
	pub output_path: PathBuf,

	#[usage(long)]
	pub url: tangram_uri::Uri,

	#[usage(long)]
	pub tangram_path: PathBuf,
}

#[derive(Clone, Debug, Default, usage::Args)]
#[usage(args_override_self = false)]
pub struct Listen {
	#[usage(
		default_missing = "true",
		long = "connect",
		overrides("--listen"),
		require_equals = true
	)]
	connect: Option<bool>,

	#[usage(
		default_missing = "true",
		long = "listen",
		overrides("--connect"),
		require_equals = true
	)]
	listen: Option<bool>,
}

pub async fn run(args: Args) -> tg::Result<()> {
	let arg = tangram_sandbox::serve::Arg {
		http2: args.http2.config(),
		library_paths: args.library_paths,
		listen: args.listen.get(),
		namespaces: args.namespaces,
		output_path: args.output_path,
		tangram_path: args.tangram_path,
		url: args.url,
	};
	tangram_sandbox::serve::run(&arg).await?;
	Ok(())
}

impl Listen {
	#[must_use]
	pub fn get(&self) -> bool {
		self.listen
			.or(self.connect.map(|value| !value))
			.unwrap_or(true)
	}
}
