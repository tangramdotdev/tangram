use {
	tangram_cli_core::Cli,
	tangram_client::prelude::*,
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
};

/// The component.
pub struct Component<'a>(&'a mut Cli);

#[derive(Clone, Debug, clap::Args)]
pub struct Args {
	pub id: Option<String>,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_id(&mut self, args: Args) -> tg::Result<()> {
		let id = if let Some(id) = args.id {
			id.into_bytes()
		} else {
			let mut id = Vec::new();
			let mut stdin = tokio::io::stdin();
			stdin
				.read_to_end(&mut id)
				.await
				.map_err(|error| tg::error!(!error, "failed to read from stdin"))?;
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
			.map_err(|error| tg::error!(!error, "failed to write to stdout"))?;
		stdout
			.flush()
			.await
			.map_err(|error| tg::error!(!error, "failed to flush stdout"))?;

		Ok(())
	}
}

impl std::ops::Deref for Component<'_> {
	type Target = Cli;

	fn deref(&self) -> &Self::Target {
		self.0
	}
}

impl std::ops::DerefMut for Component<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.0
	}
}
