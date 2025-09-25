use tangram_client as tg;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

#[derive(Clone, Debug, clap::Args)]
pub struct Args {
	pub id: Option<String>,
}

impl crate::Cli {
	pub async fn command_id(&mut self, args: Args) -> tg::Result<()> {
		let id = if let Some(id) = args.id {
			id.into_bytes()
		} else {
			let mut id = Vec::new();
			let mut stdin = tokio::io::stdin();
			stdin
				.read_to_end(&mut id)
				.await
				.map_err(|source| tg::error!(!source, "failed to read from stdin"))?;
			id
		};

		let output = if let Ok(id) = std::str::from_utf8(&id)
			&& let Ok(id) = id.parse::<tg::Id>()
		{
			id.to_bytes().to_vec()
		} else if let Ok(id) = tg::Id::from_slice(&id) {
			id.to_string().into_bytes()
		} else {
			return Err(tg::error!("invalid id"));
		};

		let mut stdout = tokio::io::stdout();
		stdout
			.write_all(&output)
			.await
			.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
		stdout
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush stdout"))?;

		Ok(())
	}
}
