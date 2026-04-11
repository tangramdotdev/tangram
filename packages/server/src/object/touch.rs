use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
};

impl Server {
	pub async fn touch_object_with_context(
		&self,
		_context: &Context,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<()> {
		if Self::local(arg.local, arg.remotes.as_ref())
			&& self
				.try_get_object_local(id, false)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
				.is_some()
		{
			return self.touch_object_local(id).await;
		}

		let peers = self
			.peers(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the peers"))?;
		if self.touch_object_peer(id, &peers).await? {
			return Ok(());
		}

		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if self.touch_object_remote(id, &remotes).await? {
			return Ok(());
		}

		Err(tg::error!("failed to find the object"))
	}

	async fn touch_object_local(&self, id: &tg::object::Id) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		self.index
			.touch_object(id, touched_at)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to touch the object"))?;
		Ok(())
	}

	async fn touch_object_peer(&self, id: &tg::object::Id, peers: &[String]) -> tg::Result<bool> {
		for peer in peers {
			let client = self.get_peer_client(peer.clone()).await.map_err(
				|source| tg::error!(!source, %id, peer = %peer, "failed to get the peer client"),
			)?;
			let output = client
				.try_get_object(id, tg::object::get::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %id, peer = %peer, "failed to get the object"),
				)?;
			if output.is_none() {
				continue;
			}
			client
				.touch_object(id, tg::object::touch::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %id, peer = %peer, "failed to touch the object"),
				)?;
			return Ok(true);
		}
		Ok(false)
	}

	async fn touch_object_remote(
		&self,
		id: &tg::object::Id,
		remotes: &[String],
	) -> tg::Result<bool> {
		for remote in remotes {
			let client = self.get_remote_client(remote.clone()).await.map_err(
				|source| tg::error!(!source, %id, remote = %remote, "failed to get the remote client"),
			)?;
			let output = client
				.try_get_object(id, tg::object::get::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %id, remote = %remote, "failed to get the object"),
				)?;
			if output.is_none() {
				continue;
			}
			client
				.touch_object(id, tg::object::touch::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %id, remote = %remote, "failed to touch the object"),
				)?;
			return Ok(true);
		}
		Ok(false)
	}

	pub(crate) async fn handle_touch_object_request(
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

		// Parse the object id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the object id"))?;

		// Get the arg.
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Touch the object.
		self.touch_object_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to touch the object"))?;

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
