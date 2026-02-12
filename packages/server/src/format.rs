use {
	crate::{Context, Server},
	std::path::Path,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_ignore as ignore,
};

impl Server {
	pub(crate) async fn format_with_context(
		&self,
		context: &Context,
		arg: tg::format::Arg,
	) -> tg::Result<()> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Canonicalize the path's parent.
		if !arg.path.is_absolute() {
			return Err(tg::error!(path = ?arg.path, "the path must be absolute"));
		}
		let path = tangram_util::fs::canonicalize_parent(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path's parent"))?;

		// Create the ignore matcher.
		let mut ignore = Self::checkin_create_ignorer()
			.map_err(|source| tg::error!(!source, "failed to create the ignorer"))?;

		// Format.
		tokio::task::spawn_blocking({
			let server = self.clone();
			move || server.format_inner(&path, &mut ignore)
		})
		.await
		.map_err(|source| tg::error!(!source, "the format task panicked"))??;

		Ok(())
	}

	fn format_inner(&self, path: &Path, ignore: &mut ignore::Ignorer) -> tg::Result<()> {
		let metadata = std::fs::symlink_metadata(path)
			.map_err(|source| tg::error!(!source, "failed to read the metadata"))?;
		if metadata.is_dir() {
			self.format_directory(path, ignore)
				.map_err(|source| tg::error!(!source, "failed to format the directory"))?;
		} else if path.is_file() && tg::module::is_module_path(path) {
			Self::format_file(path)
				.map_err(|source| tg::error!(!source, "failed to format the file"))?;
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
				.matches(None, path.as_ref(), Some(is_directory))
				.map_err(|source| {
					tg::error!(!source, "failed to check if the path should be ignored")
				})? {
				continue;
			}

			// Recurse.
			self.format_inner(&path, ignore)
				.map_err(|source| tg::error!(!source, "failed to format the path"))?;
		}

		Ok(())
	}

	fn format_file(path: &Path) -> tg::Result<()> {
		// Get the text.
		let text = std::fs::read_to_string(path)
			.map_err(|source| tg::error!(!source, "failed to read the module"))?;

		// Format the text.
		let text = tangram_compiler::Compiler::format(&text).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to format the module"),
		)?;

		// Write the text.
		std::fs::write(path, text.as_bytes()).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to write the formatted module"),
		)?;

		Ok(())
	}

	pub(crate) async fn handle_format_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Format.
		self.format_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to format"))?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().body(BoxBody::empty()).unwrap();
		Ok(response)
	}
}
