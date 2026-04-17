use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Server {
	#[cfg(not(feature = "typescript"))]
	pub(crate) async fn check_with_context(
		&self,
		_context: &Context,
		_arg: tg::check::Arg,
	) -> tg::Result<tg::check::Output> {
		Err(tg::error!(
			"this version of tangram was not compiled with typescript support"
		))
	}

	#[cfg(feature = "typescript")]
	pub(crate) async fn check_with_context(
		&self,
		context: &Context,
		arg: tg::check::Arg,
	) -> tg::Result<tg::check::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let location = self.location_with_regions(arg.location.as_ref())?;

		let output = match location {
			crate::location::Location::Local { region: None } => self.check_local(arg).await?,
			crate::location::Location::Local {
				region: Some(region),
			} => self.check_region(arg, region).await?,
			crate::location::Location::Remote { remote, region } => {
				self.check_remote(arg, remote, region).await?
			},
		};

		Ok(output)
	}

	#[cfg(feature = "typescript")]
	async fn check_local(&self, arg: tg::check::Arg) -> tg::Result<tg::check::Output> {
		// Create the compiler.
		let compiler = self.create_compiler();

		// Check the modules.
		let diagnostics = compiler
			.check(arg.modules)
			.await
			.map_err(|source| tg::error!(!source, "failed to check the modules"))?;

		// Create the output.
		let diagnostics = diagnostics.iter().map(tg::Diagnostic::to_data).collect();
		let output = tg::check::Output { diagnostics };

		// Stop and await the compiler.
		compiler.stop();
		compiler
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "failed to wait for the compiler"))?;

		Ok(output)
	}

	#[cfg(feature = "typescript")]
	async fn check_region(
		&self,
		arg: tg::check::Arg,
		region: String,
	) -> tg::Result<tg::check::Output> {
		let client = self.get_region_client(region.clone()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let arg = tg::check::Arg {
			location: Some(tg::location::Location::Local(tg::location::Local {
				regions: Some(vec![region.clone()]),
			})),
			..arg
		};
		let output = client
			.check(arg)
			.await
			.map_err(|source| tg::error!(!source, region = %region, "failed to check"))?;
		Ok(output)
	}

	#[cfg(feature = "typescript")]
	async fn check_remote(
		&self,
		arg: tg::check::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<tg::check::Output> {
		let client = self.get_remote_client(remote.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote, "failed to get the remote client"),
		)?;
		let arg = tg::check::Arg {
			location: Some(tg::location::Location::Local(tg::location::Local {
				regions: region.map(|region| vec![region]),
			})),
			..arg
		};
		let output = client
			.check(arg)
			.await
			.map_err(|source| tg::error!(!source, remote = %remote, "failed to check"))?;
		Ok(output)
	}

	pub(crate) async fn handle_check_request(
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

		// Check the modules.
		let output = self
			.check_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check the modules"))?;

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
