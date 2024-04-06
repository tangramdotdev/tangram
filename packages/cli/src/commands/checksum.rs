use crate::Cli;
use futures::TryStreamExt as _;
use tangram_client as tg;
use tokio_util::io::StreamReader;
use url::Url;

/// Compute a checksum.
#[derive(Debug, clap::Args)]
pub struct Args {
	/// The checksum algorithm to use.
	#[clap(short, long)]
	pub algorithm: tg::checksum::Algorithm,

	/// The blob or URL to checksum.
	pub arg: Arg,
}

#[derive(Debug, Clone)]
pub enum Arg {
	Blob(tg::blob::Id),
	Url(Url),
}

impl Cli {
	pub async fn command_checksum(&self, args: Args) -> tg::Result<()> {
		match args.arg {
			Arg::Blob(blob) => {
				let client = &self.client().await?;
				let blob = tg::Blob::with_id(blob);
				let mut reader = blob.reader(client).await?;
				let mut checksum = tg::checksum::Writer::new(args.algorithm);
				tokio::io::copy(&mut reader, &mut checksum)
					.await
					.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
				let checksum = checksum.finalize();
				println!("{checksum}");
			},

			Arg::Url(url) => {
				let response = reqwest::get(url.clone())
					.await
					.map_err(|source| tg::error!(!source, %url, "failed to perform the request"))?
					.error_for_status()
					.map_err(|source| tg::error!(!source, %url, "expected a sucess status"))?;
				let mut body = Box::pin(StreamReader::new(
					response.bytes_stream().map_err(std::io::Error::other),
				));
				let mut checksum = tg::checksum::Writer::new(args.algorithm);
				tokio::io::copy(&mut body, &mut checksum)
					.await
					.map_err(|source| tg::error!(!source, "failed to read the body"))?;
				let checksum = checksum.finalize();
				println!("{checksum}");
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for Arg {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(blob) = s.parse() {
			return Ok(Arg::Blob(blob));
		}
		if let Ok(url) = s.parse() {
			return Ok(Arg::Url(url));
		}
		Err(tg::error!(%s, "expected a blob or url"))
	}
}
