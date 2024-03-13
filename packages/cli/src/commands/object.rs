use crate::Cli;
use tangram_client as tg;
use tangram_error::{error, Result};
use tg::Handle;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Manage objects.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
#[command(verbatim_doc_comment)]
pub enum Command {
	Get(GetArgs),
	Put(PutArgs),
	Push(PushArgs),
	Pull(PullArgs),
}

/// Get an object.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct GetArgs {
	pub id: tg::object::Id,
}

/// Put an object.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct PutArgs {
	#[clap(short, long)]
	bytes: Option<String>,
	#[clap(short, long)]
	kind: tg::object::Kind,
}

/// Push an object.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct PushArgs {
	pub id: tg::object::Id,
}

/// Pull an object.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct PullArgs {
	pub id: tg::object::Id,
}

impl Cli {
	pub async fn command_object(&self, args: Args) -> Result<()> {
		match args.command {
			Command::Get(args) => {
				self.command_object_get(args).await?;
			},
			Command::Put(args) => {
				self.command_object_put(args).await?;
			},
			Command::Push(args) => {
				self.command_object_push(args).await?;
			},
			Command::Pull(args) => {
				self.command_object_pull(args).await?;
			},
		}
		Ok(())
	}

	pub async fn command_object_get(&self, args: GetArgs) -> Result<()> {
		let client = &self.client().await?;
		let tg::object::GetOutput { bytes, .. } = client.get_object(&args.id).await?;
		tokio::io::stdout()
			.write_all(&bytes)
			.await
			.map_err(|error| error!(source = error, "Failed to write the data."))?;
		Ok(())
	}

	pub async fn command_object_put(&self, args: PutArgs) -> Result<()> {
		let client = &self.client().await?;
		let kind = args.kind.into();
		let bytes = if let Some(bytes) = args.bytes {
			bytes.into_bytes()
		} else {
			let mut bytes = Vec::new();
			tokio::io::stdin()
				.read_to_end(&mut bytes)
				.await
				.map_err(|error| error!(source = error, "Failed to read stdin."))?;
			bytes
		};
		let id = tg::Id::new_blake3(kind, &bytes).try_into().unwrap();
		let arg = tg::object::PutArg {
			bytes: bytes.into(),
			count: None,
			weight: None,
		};
		client.put_object(&id, &arg).await?;
		Ok(())
	}

	pub async fn command_object_push(&self, args: PushArgs) -> Result<()> {
		let client = &self.client().await?;
		client.push_object(&args.id).await?;
		Ok(())
	}

	pub async fn command_object_pull(&self, args: PullArgs) -> Result<()> {
		let client = &self.client().await?;
		client.pull_object(&args.id).await?;
		Ok(())
	}
}
