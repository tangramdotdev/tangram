use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
};

impl Server {
	pub(crate) async fn try_touch_process_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> tg::Result<Option<()>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		if Self::local(arg.local, arg.remotes.as_ref()) {
			return self.touch_process_local(id).await;
		}

		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			return self.touch_process_remote(id, remote).await;
		}

		Ok(None)
	}

	async fn touch_process_local(&self, id: &tg::process::Id) -> tg::Result<Option<()>> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let output = self
			.index
			.touch_process(id, touched_at)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to touch the process"))?;
		Ok(output.map(|_| ()))
	}

	async fn touch_process_remote(
		&self,
		id: &tg::process::Id,
		remote: String,
	) -> tg::Result<Option<()>> {
		let client = self
			.get_remote_client(remote)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
		let arg = tg::process::touch::Arg {
			local: None,
			remotes: None,
		};
		client
			.try_touch_process(id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to touch the process on the remote"))
	}

	pub(crate) async fn handle_touch_process_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Get the arg.
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Touch the process.
		let Some(()) = self
			.try_touch_process_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to touch the process"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

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

		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
