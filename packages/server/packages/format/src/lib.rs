use {
	futures::{FutureExt as _, future::BoxFuture},
	std::path::Path,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_ignore as ignore,
	tangram_server_core::Session,
};

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn format(&self, session: &Session, arg: tg::format::Arg) -> tg::Result<()> {
		session.verify_request_from_host()?;

		// Canonicalize the path's parent.
		if !arg.path.is_absolute() {
			return Err(tg::error!(path = ?arg.path, "the path must be absolute"));
		}
		let path = tangram_util::fs::canonicalize_parent(&arg.path)
			.await
			.map_err(|error| tg::error!(!error, "failed to canonicalize the path's parent"))?;

		// Create the ignore matcher.
		let mut ignore = tangram_server_core::checkin::create_ignorer()
			.map_err(|error| tg::error!(!error, "failed to create the ignorer"))?;

		// Format.
		tokio::task::spawn_blocking({
			let component = self.clone();
			let session = session.clone();
			move || component.format_inner(&session, &path, &mut ignore)
		})
		.await
		.map_err(|error| tg::error!(!error, "the format task panicked"))??;

		Ok(())
	}

	fn format_inner(
		&self,
		session: &Session,
		path: &Path,
		ignore: &mut ignore::Ignorer,
	) -> tg::Result<()> {
		let metadata = std::fs::symlink_metadata(path)
			.map_err(|error| tg::error!(!error, "failed to read the metadata"))?;
		if metadata.is_dir() {
			self.format_directory(session, path, ignore)
				.map_err(|error| tg::error!(!error, "failed to format the directory"))?;
		} else if path.is_file() && tg::module::is_module_path(path) {
			Self::format_file(path)
				.map_err(|error| tg::error!(!error, "failed to format the file"))?;
		}
		Ok(())
	}

	fn format_directory(
		&self,
		session: &Session,
		path: &Path,
		ignore: &mut ignore::Ignorer,
	) -> tg::Result<()> {
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
				.map_err(|error| tg::error!(!error, "failed to get the file type"))?;
			let is_directory = file_type.is_dir();
			if ignore
				.matches(None, path.as_ref(), Some(is_directory))
				.map_err(|error| {
					tg::error!(!error, "failed to check if the path should be ignored")
				})? {
				continue;
			}

			// Recurse.
			self.format_inner(session, &path, ignore)
				.map_err(|error| tg::error!(!error, "failed to format the path"))?;
		}

		Ok(())
	}

	fn format_file(path: &Path) -> tg::Result<()> {
		// Get the text.
		let text = std::fs::read_to_string(path)
			.map_err(|error| tg::error!(!error, "failed to read the module"))?;

		// Format the text.
		let kind = tg::module::module_kind_for_path(path)?;
		let text = tangram_compiler::Compiler::format(&text, kind).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to format the module"),
		)?;

		// Write the text.
		std::fs::write(path, text.as_bytes()).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to write the formatted module"),
		)?;

		Ok(())
	}

	pub(crate) async fn format_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Format.
		self.format(session, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to format"))?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::POST, ["format"]) => self.format_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::format::Component for Component {
	fn format<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::format::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.format(session, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
