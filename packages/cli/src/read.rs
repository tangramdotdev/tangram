use {crate::Cli, std::pin::pin, tangram_client::prelude::*, tokio::io::AsyncWriteExt as _};

/// Read files and blobs.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[arg(index = 1)]
	pub references: Vec<tg::Reference>,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_read(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());

		let arg = tg::get::Arg {
			local: args.local.local,
			remotes: args.remotes.remotes,
			..Default::default()
		};

		for reference in &args.references {
			let referent = self
				.get_reference_with_arg(reference, arg.clone(), true)
				.await?;
			let tg::Either::Left(object) = referent.item() else {
				return Err(tg::error!("expected an object"));
			};
			if let Ok(blob) = tg::Blob::try_from(object.clone()) {
				let reader = blob.read(&handle, tg::read::Options::default()).await?;
				tokio::io::copy(&mut pin!(reader), &mut stdout)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the blob to stdout"))?;
			} else if let Ok(artifact) = tg::Artifact::try_from(object.clone()) {
				// Get the blob.
				let blob = match artifact {
					tg::Artifact::Directory(_) => {
						return Err(tg::error!("cannot read a directory"));
					},
					tg::Artifact::File(file) => file
						.contents(&handle)
						.await
						.map_err(|source| tg::error!(!source, "failed to get file contents"))?
						.clone(),
					tg::Artifact::Symlink(symlink) => {
						let artifact = symlink.try_resolve(&handle).await?;
						match artifact {
							None | Some(tg::Artifact::Symlink(_)) => {
								return Err(tg::error!("failed to resolve the symlink"));
							},
							Some(tg::Artifact::Directory(_)) => {
								return Err(tg::error!("cannot read a directory"));
							},
							Some(tg::Artifact::File(file)) => file
								.contents(&handle)
								.await
								.map_err(|source| {
									tg::error!(!source, "failed to get the file contents")
								})?
								.clone(),
						}
					},
				};

				// Create a reader.
				let reader = blob.read(&handle, tg::read::Options::default()).await?;

				// Copy from the reader to stdout.
				tokio::io::copy(&mut pin!(reader), &mut stdout)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to write the blob contents to stdout")
					})?;
			} else {
				return Err(tg::error!("expected an artifact or a blob"));
			}
		}

		// Flush.
		stdout
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush stdout"))?;

		Ok(())
	}
}
