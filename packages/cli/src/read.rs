use {crate::Cli, std::pin::pin, tangram_client::prelude::*, tokio::io::AsyncWriteExt as _};

/// Read files and blobs.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub references: Vec<tg::Reference>,
}

impl Cli {
	pub async fn command_read(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());

		for reference in &args.references {
			let arg = tg::get::Arg::default();
			let referent = self.get_reference_with_arg(reference, arg).await?;
			let edge = referent
				.item()
				.as_ref()
				.left()
				.ok_or_else(|| tg::error!("expected an object"))?;

			let blob = match edge {
				tg::graph::Edge::Object(tg::Object::Blob(blob)) => blob.clone(),
				tg::graph::Edge::Object(tg::Object::File(file)) => file
					.contents_with_handle(&handle)
					.await
					.map_err(|source| tg::error!(!source, "failed to get file contents"))?,
				tg::graph::Edge::Object(tg::Object::Symlink(symlink)) => {
					let artifact = symlink.try_resolve_with_handle(&handle).await?;
					match artifact {
						None | Some(tg::Artifact::Symlink(_)) => {
							return Err(tg::error!("failed to resolve the symlink"));
						},
						Some(tg::Artifact::Directory(_)) => {
							return Err(tg::error!("cannot read a directory"));
						},
						Some(tg::Artifact::File(file)) => file
							.contents_with_handle(&handle)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to get the file contents")
							})?
							.clone(),
					}
				},
				tg::graph::Edge::Pointer(pointer)
					if matches!(pointer.kind, tg::artifact::Kind::Directory) =>
				{
					return Err(tg::error!("cannot read a directory"));
				},

				tg::graph::Edge::Pointer(pointer)
					if matches!(pointer.kind, tg::artifact::Kind::File) =>
				{
					let file = tg::File::with_object(tg::file::Object::Pointer(pointer.clone()));
					file.contents_with_handle(&handle)
						.await
						.map_err(|source| tg::error!(!source, "failed to get file contents"))?
				},

				tg::graph::Edge::Pointer(pointer)
					if matches!(pointer.kind, tg::artifact::Kind::Symlink) =>
				{
					let symlink =
						tg::Symlink::with_object(tg::symlink::Object::Pointer(pointer.clone()));
					let artifact = symlink.try_resolve_with_handle(&handle).await?;
					match artifact {
						None | Some(tg::Artifact::Symlink(_)) => {
							return Err(tg::error!("failed to resolve the symlink"));
						},
						Some(tg::Artifact::Directory(_)) => {
							return Err(tg::error!("cannot read a directory"));
						},
						Some(tg::Artifact::File(file)) => file
							.contents_with_handle(&handle)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to get the file contents")
							})?
							.clone(),
					}
				},

				tg::graph::Edge::Pointer(_) => {
					return Err(tg::error!("expected a file or symlink"));
				},
				tg::graph::Edge::Object(_) => {
					return Err(tg::error!(
						"expected a blob, file, or symlink that points to a file"
					));
				},
			};

			let reader = blob
				.read_with_handle(&handle, tg::read::Options::default())
				.await?;
			tokio::io::copy(&mut pin!(reader), &mut stdout)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the blob to stdout"))?;
		}

		// Flush.
		stdout
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush stdout"))?;

		Ok(())
	}
}
