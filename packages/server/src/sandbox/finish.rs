use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Server {
	pub(crate) async fn finish_sandbox_with_context(
		&self,
		context: &Context,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> tg::Result<()> {
		if Self::local(arg.local, arg.remotes.as_ref())
			&& self
				.get_sandbox_exists_local(id)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox"))?
		{
			return self.finish_sandbox_local(context, id).await;
		}

		let peers = self
			.peers(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the peers"))?;
		if self.finish_sandbox_peer(id, &peers).await? {
			return Ok(());
		}

		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if self.finish_sandbox_remote(id, &remotes).await? {
			return Ok(());
		}

		Err(tg::error!("failed to find the sandbox"))
	}

	async fn finish_sandbox_local(
		&self,
		context: &Context,
		id: &tg::sandbox::Id,
	) -> tg::Result<()> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let finished = self.try_finish_sandbox_local(id).await?;
		if !finished && !self.get_sandbox_exists_local(id).await? {
			return Err(tg::error!("failed to find the sandbox"));
		}
		Ok(())
	}

	async fn finish_sandbox_peer(
		&self,
		id: &tg::sandbox::Id,
		peers: &[String],
	) -> tg::Result<bool> {
		for peer in peers {
			let client = self.get_peer_client(peer.clone()).await.map_err(
				|source| tg::error!(!source, %id, peer = %peer, "failed to get the peer client"),
			)?;
			let output = client
				.try_get_sandbox(id, tg::sandbox::get::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %id, peer = %peer, "failed to get the sandbox"),
				)?;
			if output.is_none() {
				continue;
			}
			client
				.finish_sandbox(id, tg::sandbox::finish::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %id, peer = %peer, "failed to finish the sandbox"),
				)?;
			return Ok(true);
		}
		Ok(false)
	}

	async fn finish_sandbox_remote(
		&self,
		id: &tg::sandbox::Id,
		remotes: &[String],
	) -> tg::Result<bool> {
		for remote in remotes {
			let client = self.get_remote_client(remote.clone()).await.map_err(
				|source| tg::error!(!source, %id, remote = %remote, "failed to get the remote client"),
			)?;
			let output = client
				.try_get_sandbox(id, tg::sandbox::get::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %id, remote = %remote, "failed to get the sandbox"),
				)?;
			if output.is_none() {
				continue;
			}
			client
				.finish_sandbox(id, tg::sandbox::finish::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %id, remote = %remote, "failed to finish the sandbox"),
				)?;
			return Ok(true);
		}
		Ok(false)
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

		self.finish_sandbox_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to finish the sandbox"))?;

		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		Ok(http::Response::builder().body(BoxBody::empty()).unwrap())
	}
}
