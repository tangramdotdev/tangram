use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::Messenger,
};

impl Server {
	pub(crate) async fn try_set_process_tty_size_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::tty::size::put::Arg,
	) -> tg::Result<()> {
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(output) = self
				.try_get_process_local(id, false)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
		{
			return self
				.set_process_tty_size_local(id, arg.size, output.data.tty.is_some())
				.await;
		}

		let peers = self
			.peers(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the peers"))?;
		if self.set_process_tty_size_peer(id, &arg, &peers).await? {
			return Ok(());
		}

		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if self.set_process_tty_size_remote(id, &arg, &remotes).await? {
			return Ok(());
		}

		Err(tg::error!("failed to find the process"))
	}

	async fn set_process_tty_size_local(
		&self,
		id: &tg::process::Id,
		size: tg::process::tty::Size,
		has_tty: bool,
	) -> tg::Result<()> {
		// Check if the process has a tty.
		if !has_tty {
			return Err(tg::error!(%id, "the process does not have a tty associated with it"));
		}

		// Publish the message.
		let event = tg::process::tty::size::get::Event::Size(size);
		let payload = tangram_messenger::payload::Json(event);
		self.messenger
			.publish(format!("processes.{id}.tty"), payload)
			.await
			.map_err(|source| tg::error!(!source, "failed to update the tty size"))?;

		Ok(())
	}

	async fn set_process_tty_size_peer(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::tty::size::put::Arg,
		peers: &[String],
	) -> tg::Result<bool> {
		for peer in peers {
			let client = self.get_peer_client(peer.clone()).await.map_err(
				|source| tg::error!(!source, %id, peer = %peer, "failed to get the peer client"),
			)?;
			let output = client
				.try_get_process(id, tg::process::get::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %id, peer = %peer, "failed to get the process"),
				)?;
			if output.is_none() {
				continue;
			}
			let arg = tg::process::tty::size::put::Arg {
				local: None,
				remotes: None,
				size: arg.size,
			};
			client.set_process_tty_size(id, arg).await.map_err(
				|source| tg::error!(!source, %id, peer = %peer, "failed to set the process tty size"),
			)?;
			return Ok(true);
		}
		Ok(false)
	}

	async fn set_process_tty_size_remote(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::tty::size::put::Arg,
		remotes: &[String],
	) -> tg::Result<bool> {
		for remote in remotes {
			let client = self.get_remote_client(remote.clone()).await.map_err(
				|source| tg::error!(!source, %id, remote = %remote, "failed to get the remote client"),
			)?;
			let output = client
				.try_get_process(id, tg::process::get::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %id, remote = %remote, "failed to get the process"),
				)?;
			if output.is_none() {
				continue;
			}
			let arg = tg::process::tty::size::put::Arg {
				local: None,
				remotes: None,
				size: arg.size,
			};
			client.set_process_tty_size(id, arg).await.map_err(
				|source| tg::error!(!source, %id, remote = %remote, "failed to set the process tty size"),
			)?;
			return Ok(true);
		}
		Ok(false)
	}

	pub(crate) async fn handle_set_process_tty_size_request(
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
			.json()
			.await
			.map_err(|_| tg::error!("failed to deserialize the arg"))?;

		// Put the process tty.
		self.try_set_process_tty_size_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the process tty"))?;

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
