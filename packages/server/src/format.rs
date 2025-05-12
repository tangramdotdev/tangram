use crate::{Server, compiler::Compiler};
use std::path::Path;
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_ignore as ignore;

impl Server {
	pub async fn format(&self, arg: tg::format::Arg) -> tg::Result<()> {
		// Canonicalize the path's parent.
		let path = crate::util::fs::canonicalize_parent(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path's parent"))?;

		// Create the ignore matcher.
		let mut ignore = Self::checkin_create_ignorer()?;

		// Format.
		tokio::task::spawn_blocking({
			let server = self.clone();
			move || server.format_inner(&path, &mut ignore)
		})
		.await
		.unwrap()?;

		Ok(())
	}

	fn format_inner(&self, path: &Path, ignore: &mut ignore::Ignorer) -> tg::Result<()> {
		let metadata = std::fs::metadata(path)
			.map_err(|source| tg::error!(!source, "failed to read the metadata"))?;
		if metadata.is_dir() {
			self.format_directory(path, ignore)?;
		} else if path.is_file() && tg::package::is_module_path(path) {
			Self::format_file(path)?;
		}
		Ok(())
	}

	fn format_directory(&self, path: &Path, ignore: &mut ignore::Ignorer) -> tg::Result<()> {
		// Read the directory entries.
		let mut entries = Vec::new();
		let mut read_dir = std::fs::read_dir(path)
			.map_err(|error| tg::error!(source = error, "failed to read the directory"))?;
		while let Some(entry) = read_dir
			.next()
			.transpose()
			.map_err(|error| tg::error!(source = error, "failed to read the directory entry"))?
		{
			entries.push(entry);
		}
		drop(read_dir);

		// Handle the directory entries.
		for entry in entries {
			// Get the path.
			let path = entry.path();

			// Check to see if the path should be ignored.
			let file_type = entry
				.file_type()
				.map_err(|source| tg::error!(!source, "failed to get the file type"))?;
			let is_directory = file_type.is_dir();
			if ignore
				.matches(path.as_ref(), Some(is_directory))
				.map_err(|source| {
					tg::error!(!source, "failed to check if the path should be ignored")
				})? {
				continue;
			}

			// Recurse.
			self.format_inner(&path, ignore)?;
		}

		Ok(())
	}

	fn format_file(path: &Path) -> tg::Result<()> {
		// Get the text.
		let text = std::fs::read_to_string(path)
			.map_err(|source| tg::error!(!source, "failed to read the module"))?;

		// Format the text.
		let text = Compiler::format(&text).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to format the module"),
		)?;

		// Write the text.
		std::fs::write(path, text.as_bytes()).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to write the formatted module"),
		)?;

		Ok(())
	}

	pub(crate) async fn handle_format_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		handle.format(arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
