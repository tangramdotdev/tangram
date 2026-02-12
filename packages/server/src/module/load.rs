use {
	crate::{Server, context::Context},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Server {
	pub async fn load_module_with_context(
		&self,
		_context: &Context,
		arg: tg::module::load::Arg,
	) -> tg::Result<tg::module::load::Output> {
		match &arg.module {
			// Handle a declaration.
			tg::module::Data {
				kind: tg::module::Kind::Dts,
				referent:
					tg::Referent {
						item: tg::module::data::Item::Path(path),
						..
					},
			} => {
				let file = tangram_compiler::LIBRARY
					.get_file(path)
					.ok_or_else(|| tg::error!(?path, "failed to find the library module"))?;
				let text = file.contents_utf8().unwrap().to_owned();
				Ok(tg::module::load::Output { text })
			},

			// Handle a JS or TS module from a path.
			tg::module::Data {
				kind: tg::module::Kind::Js | tg::module::Kind::Ts,
				referent:
					tg::Referent {
						item: tg::module::data::Item::Path(path),
						..
					},
				..
			} => {
				// Otherwise, load from the path.
				let text = tokio::fs::read_to_string(&path).await.map_err(
					|source| tg::error!(!source, path = %path.display(), "failed to read the file"),
				)?;
				Ok(tg::module::load::Output { text })
			},

			// Handle a JS or TS module from an object.
			tg::module::Data {
				kind: tg::module::Kind::Js | tg::module::Kind::Ts,
				referent:
					tg::Referent {
						item: tg::module::data::Item::Edge(edge),
						..
					},
				..
			} => {
				let object = match edge {
					tg::graph::data::Edge::Pointer(pointer) => {
						let pointer = tg::graph::Pointer::try_from_data(pointer.clone())?;
						tg::Artifact::with_pointer(pointer).into()
					},
					tg::graph::data::Edge::Object(object) => tg::Object::with_id(object.clone()),
				};
				let file = object
					.try_unwrap_file()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				let text = file
					.text(self)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the file text"))?;
				Ok(tg::module::load::Output { text })
			},

			// Handle object modules.
			tg::module::Data {
				kind:
					tg::module::Kind::Object
					| tg::module::Kind::Blob
					| tg::module::Kind::Artifact
					| tg::module::Kind::Directory
					| tg::module::Kind::File
					| tg::module::Kind::Symlink
					| tg::module::Kind::Graph
					| tg::module::Kind::Command,
				referent: tg::Referent { item, .. },
				..
			} => {
				let class = match arg.module.kind {
					tg::module::Kind::Object => "Object",
					tg::module::Kind::Blob => "Blob",
					tg::module::Kind::Artifact => "Artifact",
					tg::module::Kind::Directory => "Directory",
					tg::module::Kind::File => "File",
					tg::module::Kind::Symlink => "Symlink",
					tg::module::Kind::Graph => "Graph",
					tg::module::Kind::Command => "Command",
					_ => unreachable!(),
				};
				let text = match item {
					tg::module::data::Item::Edge(edge) => match edge {
						tg::graph::data::Edge::Pointer(pointer) => {
							formatdoc!(
								r#"
									// @ts-nocheck
									export default tg.{class}.withPointer(tg.Graph.Pointer.fromDataString("{pointer}")) as tg.{class};
								"#
							)
						},
						tg::graph::data::Edge::Object(object) => {
							formatdoc!(
								r#"
									// @ts-nocheck
									export default tg.{class}.withId("{object}") as tg.{class};
								"#
							)
						},
					},
					tg::module::data::Item::Path(_) => {
						formatdoc!(
							r"
								// @ts-nocheck
								export default undefined as tg.{class};
							"
						)
					},
				};
				Ok(tg::module::load::Output { text })
			},

			_ => Err(tg::error!("invalid module")),
		}
	}

	pub(crate) async fn handle_load_module_request(
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

		// Load the module.
		let output = self
			.load_module_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to load the module"))?;

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();
		Ok(response)
	}
}
