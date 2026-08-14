use {
	crate::Cli,
	anstream::{eprintln, println},
	crossterm::style::Stylize,
	futures::{Stream, TryStreamExt as _},
	std::pin::pin,
	tangram_client::prelude::*,
	tokio::io::AsyncWriteExt as _,
};

const INDENTATION: &str = "  ";

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	/// Whether to print blobs in the value.
	#[arg(id = "print.blobs", long = "blobs")]
	pub blobs: bool,

	/// The depth with which to print the value.
	#[arg(id = "print.depth", long = "depth")]
	pub depth: Option<Depth>,

	/// Whether to pretty print the value.
	#[arg(id = "print.pretty", long = "pretty")]
	pub pretty: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum Depth {
	Finite(u64),
	Infinite,
}

impl Cli {
	pub(crate) fn print_display<T>(value: T)
	where
		T: std::fmt::Display,
	{
		println!("{value}");
	}

	pub(crate) async fn print_serde<T>(&mut self, value: T, options: Options) -> tg::Result<()>
	where
		T: serde::Serialize,
	{
		let value = serde_json::to_value(&value)
			.map_err(|error| tg::error!(!error, "failed to serialize the value"))?;
		self.print_value(&value.into(), options, None).await?;
		Ok(())
	}

	pub(crate) async fn print_serde_stream<T, S>(
		&mut self,
		stream: S,
		options: Options,
	) -> tg::Result<()>
	where
		T: serde::Serialize + Send + 'static,
		S: Stream<Item = tg::Result<T>> + Send + 'static,
	{
		let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());
		let tty = tangram_util::tty::is_foreground_controlling_tty(libc::STDOUT_FILENO);
		let pretty = options.pretty || tty;
		stdout
			.write_all(b"[")
			.await
			.map_err(|error| tg::error!(!error, "failed to write to stdout"))?;
		let mut stream = pin!(stream);
		let mut first = true;
		while let Some(value) = stream.try_next().await? {
			if !first {
				stdout
					.write_all(b",")
					.await
					.map_err(|error| tg::error!(!error, "failed to write to stdout"))?;
			}
			if pretty {
				stdout
					.write_all(b"\n")
					.await
					.map_err(|error| tg::error!(!error, "failed to write to stdout"))?;
				stdout
					.write_all(INDENTATION.as_bytes())
					.await
					.map_err(|error| tg::error!(!error, "failed to write to stdout"))?;
			}
			first = false;
			let value = serde_json::to_value(&value)
				.map_err(|error| tg::error!(!error, "failed to serialize the value"))?;
			let indent = if pretty { 1 } else { 0 };
			let value = value.into();
			self.load_value(&value, options.clone(), None).await?;
			Self::print_value_inner(&mut stdout, &value, options.clone(), indent).await?;
			if tty {
				stdout
					.flush()
					.await
					.map_err(|error| tg::error!(!error, "failed to flush stdout"))?;
			}
		}
		if pretty {
			stdout
				.write_all(b"\n")
				.await
				.map_err(|error| tg::error!(!error, "failed to write to stdout"))?;
		}
		stdout
			.write_all(b"]\n")
			.await
			.map_err(|error| tg::error!(!error, "failed to write to stdout"))?;
		stdout
			.flush()
			.await
			.map_err(|error| tg::error!(!error, "failed to flush stdout"))?;
		Ok(())
	}

	pub(crate) fn print_id(id: &tg::Id) {
		println!("{id}");
	}

	pub(crate) async fn print_value(
		&mut self,
		value: &tg::Value,
		options: Options,
		location: Option<tg::location::Arg>,
	) -> tg::Result<()> {
		let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());
		self.load_value(value, options.clone(), location).await?;
		Self::print_value_inner(&mut stdout, value, options, 0).await?;
		stdout
			.write_all(b"\n")
			.await
			.map_err(|error| tg::error!(!error, "failed to write to stdout"))?;
		stdout
			.flush()
			.await
			.map_err(|error| tg::error!(!error, "failed to flush stdout"))?;
		Ok(())
	}

	async fn load_value(
		&mut self,
		value: &tg::Value,
		options: Options,
		location: Option<tg::location::Arg>,
	) -> tg::Result<()> {
		let depth = match options.depth.unwrap_or(Depth::Finite(0)) {
			Depth::Finite(depth) => Some(depth),
			Depth::Infinite => None,
		};
		if depth == Some(0) || value.objects().is_empty() {
			return Ok(());
		}
		let client = self.client().await?;
		let options = tg::value::load::Options {
			blobs: options.blobs,
			depth,
			location,
		};
		value.load_with_handle(&client, options).await?;
		Ok(())
	}

	pub(crate) async fn print_value_inner(
		stdout: &mut tokio::io::BufWriter<tokio::io::Stdout>,
		value: &tg::Value,
		options: Options,
		indent: usize,
	) -> tg::Result<()> {
		let depth = match options.depth.unwrap_or(Depth::Finite(0)) {
			Depth::Finite(depth) => Some(depth),
			Depth::Infinite => None,
		};
		let blobs = options.blobs;
		let tty = tangram_util::tty::is_foreground_controlling_tty(libc::STDOUT_FILENO);
		let indentation = (options.pretty || tty).then_some(INDENTATION);
		let options = tg::value::print::Options {
			blobs,
			color: tty,
			depth,
			indent,
			indentation,
			tokens: false,
		};
		let output = value.print(options);
		stdout
			.write_all(output.as_bytes())
			.await
			.map_err(|error| tg::error!(!error, "failed to write the output"))?;
		Ok(())
	}

	pub fn print_info_message(&self, string: &str) {
		if self.args.quiet.get() {
			return;
		}
		eprintln!("{} {string}", "info".blue().bold());
	}

	pub fn print_warning_message(string: &str) {
		eprintln!("{} {string}", "warning".yellow().bold());
	}

	pub fn print_error_message(string: &str) {
		eprintln!("{} {string}", "error".red().bold());
	}
}

impl std::str::FromStr for Depth {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let depth = if matches!(s, "inf" | "infinite" | "infinity") {
			Self::Infinite
		} else {
			s.parse()
				.map(Self::Finite)
				.map_err(|_| tg::error!("invalid depth"))?
		};
		Ok(depth)
	}
}
