use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Server {
	pub async fn cancel_process_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<()> {
		if Self::local(arg.local, arg.remotes.as_ref())
			&& self
				.get_process_exists_local(id)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
		{
			return self.cancel_process_local(id, arg).await;
		}

		let peers = self
			.peers(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the peers"))?;
		if self.cancel_process_peer(id, &arg, &peers).await? {
			return Ok(());
		}

		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if self.cancel_process_remote(id, &arg, &remotes).await? {
			return Ok(());
		}

		Err(tg::error!("failed to find the process"))
	}

	async fn cancel_process_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<()> {
		// Get a sandbox store connection.
		let mut connection = self
			.sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		let status = self
			.try_lock_process_for_token_mutation_with_transaction(&transaction, id)
			.await
			.map_err(|source| tg::error!(!source, "failed to lock the process"))?;
		let Some(status) = status else {
			transaction
				.rollback()
				.await
				.map_err(|source| tg::error!(!source, "failed to roll back the transaction"))?;
			return Err(tg::error!("failed to find the process"));
		};

		// Delete the process token.
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				delete from process_tokens
				where process = {p}1 and token = {p}2;
			"
		);
		let params = db::params![id.to_string(), arg.token];
		let deleted = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// If no token was removed, then validate whether the token was stale or invalid.
		if deleted == 0 && !status.is_finished() {
			transaction
				.rollback()
				.await
				.map_err(|source| tg::error!(!source, "failed to roll back the transaction"))?;
			return Err(tg::error!("the process token was not found"));
		}

		// Update the token count.
		self.update_process_token_count_with_transaction(&transaction, id)
			.await
			.map_err(|source| tg::error!(!source, "failed to update the token count"))?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		drop(connection);

		// Publish the watchdog message if a token was removed.
		if deleted > 0 {
			self.spawn_publish_watchdog_message_task();
		}

		Ok(())
	}

	async fn cancel_process_peer(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::cancel::Arg,
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
			let arg = tg::process::cancel::Arg {
				local: None,
				remotes: None,
				token: arg.token.clone(),
			};
			client.cancel_process(id, arg).await.map_err(
				|source| tg::error!(!source, %id, peer = %peer, "failed to cancel the process"),
			)?;
			return Ok(true);
		}
		Ok(false)
	}

	async fn cancel_process_remote(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::cancel::Arg,
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
			let arg = tg::process::cancel::Arg {
				local: None,
				remotes: None,
				token: arg.token.clone(),
			};
			client.cancel_process(id, arg).await.map_err(
				|source| tg::error!(!source, %id, remote = %remote, "failed to cancel the process"),
			)?;
			return Ok(true);
		}
		Ok(false)
	}

	pub(crate) async fn handle_cancel_process_request(
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

		// Parse the ID.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Parse the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("query parameters required"))?;

		// Cancel the process.
		self.cancel_process_with_context(context, &id, arg).await?;

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
