use {
	crate::Cli,
	anstream::eprintln,
	crossterm::{style::Stylize, tty::IsTty as _},
	futures::{Stream, TryStreamExt as _},
	std::pin::pin,
	tangram_client::prelude::*,
	tokio::io::AsyncWriteExt as _,
};

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	/// Whether to print blobs in the value.
	#[arg(long)]
	pub blobs: bool,

	/// The depth with which to print the value.
	#[arg(long)]
	pub depth: Option<Depth>,

	/// Whether to pretty print the value.
	#[arg(long)]
	pub pretty: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum Depth {
	Finite(u64),
	Infinite,
}

impl Cli {
	pub(crate) async fn print(&mut self, value: &tg::Value, options: Options) -> tg::Result<()> {
		let handle = self.handle().await?;
		let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());
		let depth = match options.depth.unwrap_or(Depth::Finite(0)) {
			Depth::Finite(depth) => Some(depth),
			Depth::Infinite => None,
		};
		let blobs = options.blobs;
		value.load(&handle, depth, blobs).await?;
		let pretty = options.pretty || stdout.get_ref().is_tty();
		let style = if pretty {
			tg::value::print::Style::Pretty { indentation: "  " }
		} else {
			tg::value::print::Style::Compact
		};
		let options = tg::value::print::Options {
			depth,
			style,
			blobs,
		};
		let mut output = value.print(options);
		if style.is_pretty() {
			output.push('\n');
		}
		stdout
			.write_all(output.as_bytes())
			.await
			.map_err(|source| tg::error!(!source, "failed to write the output"))?;
		stdout
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush stdout"))?;
		Ok(())
	}

	pub(crate) async fn print_serde<T>(&mut self, value: T, options: Options) -> tg::Result<()>
	where
		T: serde::Serialize,
	{
		let value = serde_json::to_value(&value)
			.map_err(|source| tg::error!(!source, "failed to serialize the value"))?;
		self.print(&value.into(), options).await?;
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
		let pretty = options.pretty || stdout.get_ref().is_tty();
		if true {
			stdout
				.write_all(b"[")
				.await
				.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
			if pretty {
				stdout
					.write_all(b"\n")
					.await
					.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
			}
		}
		let mut stream = pin!(stream);
		let mut first = true;
		while let Some(value) = stream.try_next().await? {
			if !first {
				stdout
					.write_all(b",")
					.await
					.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
				if pretty {
					stdout
						.write_all(b"\n")
						.await
						.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
				}
			}
			first = false;
			self.print_serde(value, options.clone()).await?;
		}
		if true {
			stdout
				.write_all(b"]\n")
				.await
				.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
		}
		stdout
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush stdout"))?;
		Ok(())
	}

	pub fn print_info_message(string: &str) {
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
