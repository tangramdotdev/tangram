use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Server {
	pub(crate) async fn try_delete_sandbox_with_context(
		&self,
		context: &Context,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<()>> {
		self.try_finish_sandbox_with_context(context, id, tg::sandbox::finish::Arg::default())
			.await
	}

	pub(crate) async fn handle_delete_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		let id = id
			.parse::<tg::sandbox::Id>()
			.map_err(|source| tg::error!(!source, "failed to parse the sandbox id"))?;

		let Some(()) = self
			.try_delete_sandbox_with_context(context, &id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to delete the sandbox"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
