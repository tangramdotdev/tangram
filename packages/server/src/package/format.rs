use crate::Server;
use futures::{TryStreamExt as _, stream::FuturesUnordered};
use std::path::Path;
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_ignore::Matcher;

impl Server {
	pub async fn format_package(&self, arg: tg::package::format::Arg) -> tg::Result<()> {
		// let mut path = arg
		// 	.module
		// 	.referent
		// 	.item
		// 	.try_unwrap_path_ref()
		// 	.map_err(|_| tg::error!("expected a path"))?
		// 	.clone();

		// // Canonicalize the path's parent.
		// path = crate::util::fs::canonicalize_parent(&path)
		// 	.await
		// 	.map_err(|source| tg::error!(!source, "failed to canonicalize the path's parent"))?;

		// // Create the ignore matcher.
		// let mut ignore = self.ignore_matcher_for_checkin()?;

		// // Format.
		// self.format_package_inner(&path, &mut ignore).await?;

		// Ok(())
		todo!()
	}

	// async fn format_package_inner(&self, path: &Path, ignore: &mut Matcher) -> tg::Result<()> {
	// 	let metadata = tokio::fs::metadata(path)
	// 		.await
	// 		.map_err(|source| tg::error!(!source, "failed to read the metadata"))?;
	// 	if metadata.is_dir() {
	// 		self.format_directory(path, ignore).await?;
	// 	} else if path.is_file() && tg::package::is_module_path(path) {
	// 		self.format_file(path).await?;
	// 	}
	// 	Ok(())
	// }

	// async fn format_directory(&self, path: &Path, ignore: &mut Matcher) -> tg::Result<()> {
	// 	// Read the directory entries.
	// 	let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
	// 	let mut entries = Vec::new();
	// 	let mut read_dir = tokio::fs::read_dir(path)
	// 		.await
	// 		.map_err(|error| tg::error!(source = error, "could not read the directory"))?;
	// 	while let Some(entry) = read_dir
	// 		.next_entry()
	// 		.await
	// 		.map_err(|error| tg::error!(source = error, "could not read the directory entry"))?
	// 	{
	// 		entries.push(entry);
	// 	}
	// 	drop(read_dir);
	// 	drop(permit);

	// 	// Handle the directory entries.
	// 	entries
	// 		.into_iter()
	// 		.map(|entry| async move {
	// 			// Get the path.
	// 			let path = entry.path();

	// 			// Check to see if the path should be ignored.
	// 			let file_type = entry
	// 				.file_type()
	// 				.await
	// 				.map_err(|source| tg::error!(!source, "failed to get the file type"))?;
	// 			let is_directory = file_type.is_dir();
	// 			if ignore
	// 				.matches(path.as_ref(), Some(is_directory))
	// 				.map_err(|source| {
	// 					tg::error!(!source, "failed to check if the path should be ignored")
	// 				})? {
	// 				return Ok::<_, tg::Error>(());
	// 			}

	// 			// Recurse.
	// 			self.format_package_inner(&path, ignore).await?;

	// 			Ok::<_, tg::Error>(())
	// 		})
	// 		.collect::<FuturesUnordered<_>>()
	// 		.try_collect::<()>()
	// 		.await?;

	// 	Ok(())
	// }

	// async fn format_file(&self, path: &Path) -> tg::Result<()> {
	// 	// Get the text.
	// 	let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
	// 	let text = tokio::fs::read_to_string(&path)
	// 		.await
	// 		.map_err(|source| tg::error!(!source, "failed to read the module"))?;
	// 	drop(permit);

	// 	// Format the text.
	// 	let text = self.format(text).await.map_err(
	// 		|source| tg::error!(!source, %path = path.display(), "failed to format the module"),
	// 	)?;

	// 	// Write the text.
	// 	let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
	// 	tokio::fs::write(&path, text.as_bytes()).await.map_err(
	// 		|source| tg::error!(!source, %path = path.display(), "failed to write the formatted module"),
	// 	)?;
	// 	drop(permit);

	// 	Ok(())
	// }
}

impl Server {
	pub(crate) async fn handle_format_package_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		handle.format_package(arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
