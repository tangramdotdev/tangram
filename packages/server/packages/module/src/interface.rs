use {
	crate::Component,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_server_core::Session,
};

impl Component {
	pub async fn module_interface(
		&self,
		session: &Session,
		arg: tg::module::interface::Arg,
	) -> tg::Result<tg::module::interface::Output> {
		if !matches!(
			arg.module.kind,
			tg::module::Kind::Config
				| tg::module::Kind::JavaScript
				| tg::module::Kind::Python
				| tg::module::Kind::TypeScript
		) {
			return Err(
				tg::error!(kind = %arg.module.kind, "cannot get the interface of the module"),
			);
		}
		if matches!(arg.module.referent.node, tg::module::data::Source::Path(_))
			&& !matches!(session.context.principal, tg::Principal::Root)
		{
			return Err(tg::error!("unauthorized"));
		}

		// Get the interface with the types that the module's checker infers.
		#[cfg(all(feature = "javascript", feature = "python"))]
		let interface = {
			let compiler = session.create_compiler();
			let interface = compiler.interface(&arg.module).await;
			compiler.stop();
			compiler
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "failed to wait for the compiler"))?;
			interface?
		};

		// Otherwise, get the interface from the module's source without types.
		#[cfg(not(all(feature = "javascript", feature = "python")))]
		let interface = {
			let output = self.load_module_inner(session, &arg.module).await?;
			tangram_compiler::interface::module(&arg.module, &output.text)?
		};

		let output = tg::module::interface::Output { interface };

		Ok(output)
	}

	pub(crate) async fn module_interface_request(
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
		let mut arg: tg::module::interface::Arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		if let tg::module::data::Source::Path(path) = &mut arg.module.referent.node {
			*path = session
				.server
				.components
				.runner
				.host_path_for_guest_path(session, path)?;
		}

		// Get the interface.
		let output = self
			.module_interface(session, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the module interface"))?;

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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
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
