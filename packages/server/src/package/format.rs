use crate::{
	util::path::{ALLOW, DENY, IGNORE_FILES},
	Server,
};
use std::path::Path;
use tangram_client::{self as tg, package::is_module_path};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tangram_ignore::Ignore;

#[cfg(test)]
mod tests;

impl Server {
	pub async fn format_package(&self, arg: tg::package::format::Arg) -> tg::Result<()> {
		let path = arg.path.as_path();
		// Format the path if it's a directory.
		if path.is_dir() {
			let ignore = Ignore::new(IGNORE_FILES, ALLOW, DENY)
				.await
				.map_err(|source| tg::error!(!source, "failed to create ignore tree"))?;
			self.format_dir(path, &ignore).await
		} else {
			Err(tg::error!("invalid path to package"))
		}
	}

	async fn format_dir(&self, path: &Path, ignore: &Ignore) -> tg::Result<()> {
		let mut read_dir = tokio::fs::read_dir(path)
			.await
			.map_err(|error| tg::error!(source = error, "could not read package path"))?;
		while let Some(entry) = read_dir
			.next_entry()
			.await
			.map_err(|error| tg::error!(source = error, "could not read directory entry"))?
		{
			let path_buf = entry.path();
			let path = path_buf.as_path();

			// Get the file type.
			let file_type = entry
				.file_type()
				.await
				.map_err(|source| tg::error!(!source, "failed to get file type"))?;

			// Check to see if the path should be ignored.
			if ignore
				.should_ignore(path.as_ref(), file_type)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to check if the path should be ignored")
				})? {
				continue;
			}

			// Format file, or recurse into subdirectory.
			if path.is_file() && is_module_path(path) {
				Box::pin(self.format_file(path)).await?;
			} else if path.is_dir() {
				Box::pin(self.format_dir(path, ignore)).await?;
			}
		}
		Ok(())
	}

	async fn format_file(&self, path: &Path) -> tg::Result<()> {
		// Get the text.
		let text = tokio::fs::read_to_string(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the module"))?;

		// Format the text.
		let text = self.format(text).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to format module"),
		)?;

		// Write the text.
		tokio::fs::write(&path, text.as_bytes()).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to write formatted module"),
		)?;

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_format_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		handle.format_package(arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
