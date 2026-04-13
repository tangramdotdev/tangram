use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Server {
	pub(crate) async fn try_finish_sandbox_with_context(
		&self,
		context: &Context,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> tg::Result<Option<()>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		if Self::local(arg.local, arg.remotes.as_ref()) {
			return self.finish_sandbox_local(id).await;
		}

		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			return self.finish_sandbox_remote(id, remote).await;
		}

		Ok(None)
	}

	async fn finish_sandbox_local(&self, id: &tg::sandbox::Id) -> tg::Result<Option<()>> {
		let finished = self.try_finish_sandbox_local(id).await?;
		if !finished && !self.get_sandbox_exists_local(id).await? {
			return Ok(None);
		}
		Ok(Some(()))
	}

	async fn finish_sandbox_remote(
		&self,
		id: &tg::sandbox::Id,
		remote: String,
	) -> tg::Result<Option<()>> {
		let client = self
			.get_remote_client(remote)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
		let arg = tg::sandbox::finish::Arg {
			local: None,
			remotes: None,
		};
		client.try_finish_sandbox(id, arg).await.map_err(
			|source| tg::error!(!source, %id, "failed to finish the sandbox on the remote"),
		)
	}

	pub(crate) async fn handle_finish_sandbox_request(
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
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		let Some(()) = self
			.try_finish_sandbox_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to finish the sandbox"))?
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

		Ok(http::Response::builder().empty().unwrap().boxed_body())
	}
}
